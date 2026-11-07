# /// script
# requires-python = ">=3.12"
# dependencies = ["geopandas==1.2.0", "pyogrio==0.13.0", "pyarrow==26.0.1"]
# ///
"""
Converts the OSM water polygons (https://osmdata.openstreetmap.de/data/water-polygons.html) or lake centerlines
(https://github.com/acalcutt/osm-lakelines) shapefile zip to a GeoParquet file that planetiler can read with range
requests, fetching only the parts that overlap the area being generated.

usage: uv run scripts/shp2parquet.py water|lakes <input.zip> <output.parquet> [--min-features N]

Exits with a non-zero status if the output fails sanity checks, so a broken input never gets published.
"""
import argparse
import os
import signal
import sys
from dataclasses import dataclass

import geopandas as gpd
import pyarrow as pa
import pyarrow.parquet as pq


@dataclass
class Source:
  shapefile: str
  columns: list[str]


SOURCES = {
  "water": Source(shapefile="water-polygons-split-3857/water_polygons.shp", columns=[]),
  # OpenMapTiles matches lake centerlines to lakes by OSM_ID
  "lakes": Source(shapefile="lake_centerline.shp", columns=["OSM_ID"]),
}
# Small row groups let planetiler fetch only what's near the area it generates. They are split by geometry size, not
# row count, since some polygons are 1000x larger than others.
TARGET_ROW_GROUP_BYTES = 512 << 10
# Web mercator only covers latitudes up to this
MAX_LAT = 85.06


def convert(input_zip, source, output):
  print(f"reading {input_zip}")
  df = gpd.read_file(f"/vsizip/{input_zip}/{source.shapefile}", columns=source.columns, fid_as_index=True)
  # fid is the shapefile record index, the same feature id planetiler's shapefile reader uses
  df["fid"] = df.index
  print(f"reprojecting {len(df)} features")
  df = df.to_crs("OGC:CRS84")
  print("sorting")
  # Sort along a Hilbert curve so nearby features end up in the same row group. Stable so equal values keep fid order.
  df = df.iloc[df.geometry.hilbert_distance(total_bounds=(-180, -90, 180, 90)).argsort(kind="stable")]

  # geopandas writes the GeoArrow encoding and GeoParquet metadata, but can only split row groups by row count, so
  # write once in memory then copy into row groups split by size.
  print("encoding")
  buffer = pa.BufferOutputStream()
  df.to_parquet(buffer, index=False, geometry_encoding="geoarrow", write_covering_bbox=True, schema_version="1.1.0")
  table = pq.read_table(buffer.getvalue())
  doubles = [c.path for c in pq.ParquetFile(buffer.getvalue()).schema if c.path.startswith(("geometry.", "bbox."))]
  geometry_bytes = df.geometry.to_wkb().map(len)
  print(f"writing {output}")
  with pq.ParquetWriter(
    output, table.schema,
    compression="zstd", compression_level=9,
    use_dictionary=False,
    # BYTE_STREAM_SPLIT makes coordinates compress much better than plain doubles
    use_byte_stream_split=doubles,
    # The bbox statistics are all planetiler needs to skip row groups, and leaving out the rest keeps the footer small
    write_statistics=[c for c in doubles if c.startswith("bbox.")],
  ) as writer:
    start = group_bytes = 0
    for i, size in enumerate(geometry_bytes):
      group_bytes += size
      if group_bytes >= TARGET_ROW_GROUP_BYTES or i == len(df) - 1:
        writer.write_table(table.slice(start, i + 1 - start), row_group_size=len(df))
        start, group_bytes = i + 1, 0
  print(f"wrote {pq.ParquetFile(output).metadata.num_row_groups} row groups, {os.path.getsize(output) / 1e6:.1f} MB")
  return df


def check(df, min_features):
  print("checking output is valid")
  errors = []
  if len(df) < min_features:
    errors.append(f"{len(df)} features, expected at least {min_features}")
  invalid = (~df.geometry.is_valid).sum()
  if invalid > 0:
    errors.append(f"{invalid} invalid geometries")
  xmin, ymin, xmax, ymax = df.total_bounds
  if xmin < -180 or xmax > 180 or ymin < -MAX_LAT or ymax > MAX_LAT:
    errors.append(f"bounds {df.total_bounds} outside of web mercator")
  return errors


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("source", choices=SOURCES.keys())
  parser.add_argument("input")
  parser.add_argument("output")
  parser.add_argument("--min-features", type=int, default=1, help="fewer features than this means a broken input")
  args = parser.parse_args()
  # Python only handles Ctrl-C between bytecodes, never during the long GDAL / shapely / arrow calls, so exit at once
  signal.signal(signal.SIGINT, signal.SIG_DFL)
  # Show progress as it happens when the output is a pipe (GitHub Actions) too
  sys.stdout.reconfigure(line_buffering=True)
  source = SOURCES[args.source]

  df = convert(args.input, source, args.output)
  errors = check(df, args.min_features)
  if errors:
    sys.exit("FAILED: " + "; ".join(errors))
  print(f"done: {len(df)} features, bounds {df.total_bounds}")


if __name__ == "__main__":
  main()
