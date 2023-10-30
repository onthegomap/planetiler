Layer Stats
===========

This page describes how to generate and analyze layer stats data to find ways to optimize tile size.

### Generating Layer Stats

Run planetiler with `--output-layerstats` to generate an extra `<output>.layerstats.tsv.gz` file with a row for each
layer in each tile that can be used to analyze tile sizes. You can also get stats for an existing archive by running:

```bash
java -jar planetiler.jar stats --input=<path to mbtiles or pmtiles file> --output=layerstats.tsv.gz
```

The output is a gzipped tsv with a row per layer on each tile and the following columns:

|       column        |                                                                   description                                                                   |
|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| z                   | tile zoom                                                                                                                                       |
| x                   | tile x                                                                                                                                          |
| y                   | tile y                                                                                                                                          |
| hilbert             | tile hilbert ID (defines [pmtiles](https://protomaps.com/docs/pmtiles) order)                                                                   |
| archived_tile_bytes | stored tile size (usually gzipped)                                                                                                              |
| layer               | layer name                                                                                                                                      |
| layer_bytes         | encoded size of this layer on this tile                                                                                                         |
| layer_features      | number of features in this layer                                                                                                                |
| layer_geometries    | number of geometries in features in this layer, including inside multipoint/multipolygons/multilinestring features                              |
| layer_attr_bytes    | encoded size of the [attribute key/value pairs](https://github.com/mapbox/vector-tile-spec/tree/master/2.1#44-feature-attributes) in this layer |
| layer_attr_keys     | number of distinct attribute keys in this layer on this tile                                                                                    |
| layer_attr_values   | number of distinct attribute values in this layer on this tile                                                                                  |

### Analyzing Layer Stats

Load a layer stats file in [duckdb](https://duckdb.org/):

```sql
CREATE TABLE layerstats AS SELECT * FROM 'output.pmtiles.layerstats.tsv.gz';
```

Then get the biggest layers:

```sql
SELECT * FROM layerstats ORDER BY layer_bytes DESC LIMIT 2;
```

| z  |   x   |  y   |  hilbert  | archived_tile_bytes |    layer    | layer_bytes | layer_features | layer_geometries | layer_attr_bytes | layer_attr_keys | layer_attr_values |
|----|-------|------|-----------|---------------------|-------------|-------------|----------------|------------------|------------------|-----------------|-------------------|
| 14 | 13722 | 7013 | 305278258 | 1260526             | housenumber | 2412589     | 108390         | 108390           | 30764            | 1               | 3021              |
| 14 | 13723 | 7014 | 305278256 | 1059752             | housenumber | 1850041     | 83038          | 83038            | 26022            | 1               | 2542              |

To get a table of biggest layers by zoom:

```sql
PIVOT (
  SELECT z, layer, (max(layer_bytes)/1000)::int size FROM layerstats GROUP BY z, layer ORDER BY z ASC
) ON printf('%2d', z) USING sum(size);
-- duckdb sorts columns lexicographically, so left-pad the zoom so 2 comes before 10
```

|        layer        |  0  |  1  |  2  |  3  |  4  |  5  |  6  |  7  |  8  |  9  | 10  | 11  | 12  | 13  |  14  |
|---------------------|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|------|
| boundary            | 10  | 75  | 85  | 53  | 44  | 25  | 18  | 15  | 15  | 29  | 24  | 18  | 32  | 18  | 10   |
| landcover           | 2   | 1   | 8   | 5   | 3   | 31  | 18  | 584 | 599 | 435 | 294 | 175 | 166 | 111 | 334  |
| place               | 116 | 314 | 833 | 830 | 525 | 270 | 165 | 80  | 51  | 54  | 63  | 70  | 50  | 122 | 221  |
| water               | 8   | 4   | 11  | 9   | 15  | 13  | 89  | 114 | 126 | 109 | 133 | 94  | 167 | 116 | 91   |
| water_name          | 7   | 19  | 25  | 15  | 11  | 6   | 6   | 4   | 3   | 6   | 5   | 4   | 4   | 4   | 29   |
| waterway            |     |     |     | 1   | 4   | 2   | 18  | 13  | 10  | 28  | 20  | 16  | 60  | 66  | 73   |
| park                |     |     |     |     | 54  | 135 | 89  | 76  | 72  | 82  | 90  | 56  | 48  | 19  | 50   |
| landuse             |     |     |     |     | 3   | 2   | 33  | 67  | 95  | 107 | 177 | 132 | 66  | 313 | 109  |
| transportation      |     |     |     |     | 384 | 425 | 259 | 240 | 287 | 284 | 165 | 95  | 313 | 187 | 133  |
| transportation_name |     |     |     |     |     |     | 32  | 20  | 18  | 13  | 30  | 18  | 65  | 59  | 169  |
| mountain_peak       |     |     |     |     |     |     |     | 13  | 13  | 12  | 15  | 12  | 12  | 317 | 235  |
| aerodrome_label     |     |     |     |     |     |     |     |     | 5   | 4   | 5   | 4   | 4   | 4   | 4    |
| aeroway             |     |     |     |     |     |     |     |     |     |     | 16  | 26  | 35  | 31  | 18   |
| poi                 |     |     |     |     |     |     |     |     |     |     |     |     | 35  | 18  | 811  |
| building            |     |     |     |     |     |     |     |     |     |     |     |     |     | 94  | 1761 |
| housenumber         |     |     |     |     |     |     |     |     |     |     |     |     |     |     | 2412 |

To get biggest tiles:

```sql
CREATE TABLE tilestats AS SELECT
z, x, y,
any_value(archived_tile_bytes) gzipped,
sum(layer_bytes) raw
FROM layerstats GROUP BY z, x, y;

SELECT
z, x, y,
format_bytes(gzipped::int) gzipped,
format_bytes(raw::int) raw,
FROM tilestats ORDER BY gzipped DESC LIMIT 2;
```

NOTE: this group by uses a lot of memory so you need to be running in file-backed
mode `duckdb analysis.duckdb` (not in-memory mode)

| z  |  x   |  y   | gzipped | raw  |
|----|------|------|---------|------|
| 13 | 2286 | 3211 | 9KB     | 12KB |
| 13 | 2340 | 2961 | 9KB     | 12KB |

To make it easier to look at these tiles on a map, you can define following macros that convert z/x/y coordinates to
lat/lons:

```sql
CREATE MACRO lon(z, x) AS (x/2**z) * 360 - 180;
CREATE MACRO lat_n(z, y) AS pi() - 2 * pi() * y/2**z;
CREATE MACRO lat(z, y) AS degrees(atan(0.5*(exp(lat_n(z, y)) - exp(-lat_n(z, y)))));
CREATE MACRO debug_url(z, x, y) as concat(
  'https://protomaps.github.io/PMTiles/#map=',
  z + 0.5, '/',
  round(lat(z, y + 0.5), 5), '/',
  round(lon(z, x + 0.5), 5)
);

SELECT z, x, y, debug_url(z, x, y), layer, format_bytes(layer_bytes) size
FROM layerstats ORDER BY layer_bytes DESC LIMIT 2;
```

| z  |   x   |  y   |                        debug_url(z, x, y)                        |    layer    | size  |
|----|-------|------|------------------------------------------------------------------|-------------|-------|
| 14 | 13722 | 7013 | https://protomaps.github.io/PMTiles/#map=14.5/25.05575/121.51978 | housenumber | 2.4MB |
| 14 | 13723 | 7014 | https://protomaps.github.io/PMTiles/#map=14.5/25.03584/121.54175 | housenumber | 1.8MB |

Drag and drop your pmtiles archive to the pmtiles debugger to see the large tiles on a map. You can also switch to the
"inspect" tab to inspect an individual tile.

#### Computing Weighted Average Tile Sizes

If you compute a straight average tile size, it will be dominated by ocean tiles that no one looks at. You can compute a
weighted average based on actual usage by joining with a `z, x, y, loads` tile source. For
convenience, [top_osm_tiles.tsv.gz](top_osm_tiles.tsv.gz) has the top 1 million tiles from 90 days
of [OSM tile logs](https://planet.openstreetmap.org/tile_logs/) from summer 2023.

You can load these sample weights using duckdb's [httpfs module](https://duckdb.org/docs/extensions/httpfs.html):

```sql
INSTALL httpfs;
CREATE TABLE weights AS SELECT z, x, y, loads FROM 'https://raw.githubusercontent.com/onthegomap/planetiler/main/layerstats/top_osm_tiles.tsv.gz';
```

Then compute the weighted average tile size:

```sql
SELECT
format_bytes((sum(gzipped * loads) / sum(loads))::int) gzipped_avg,
format_bytes((sum(raw * loads) / sum(loads))::int) raw_avg,
FROM tilestats JOIN weights USING (z, x, y);
```

| gzipped_avg | raw_avg |
|-------------|---------|
| 81KB        | 132KB   |

If you are working with an extract, then the low-zoom tiles will dominate, so you can make the weighted average respect
the per-zoom weights that appear globally:

```sql
WITH zoom_weights AS (
  SELECT z, sum(loads) loads FROM weights GROUP BY z
),
zoom_avgs AS (
  SELECT
  z,
  sum(gzipped * loads) / sum(loads) gzipped,
  sum(raw * loads) / sum(loads) raw,
  FROM tilestats JOIN weights USING (z, x, y)
  GROUP BY z
)
SELECT
format_bytes((sum(gzipped * loads) / sum(loads))::int) gzipped_avg,
format_bytes((sum(raw * loads) / sum(loads))::int) raw_avg,
FROM zoom_avgs JOIN zoom_weights USING (z);
```

