Layer Stats
===========

This page describes how to generate and analyze layer stats data to find ways to optimize tile size.

### Generating Layer Stats

Run planetiler with `--output-layerstats` to generate an extra `<output>.layerstats.tsv.gz` file with a row per
tile layer that can be used to analyze tile sizes. You can also
get stats for an existing archive by
running:

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
| layer_attr_bytes    | encoded size of the [attribute key/value pairs](https://github.com/mapbox/vector-tile-spec/tree/master/2.1#44-feature-attributes) in this layer |
| layer_attr_keys     | number of distinct attribute keys in this layer on this tile                                                                                    |
| layer_attr_values   | number of distinct attribute values in this layer on this tile                                                                                  |

### Analyzing Layer Stats

In [duckdb](https://duckdb.org/) you can load a layer stats file:

```sql
create table layerstats as select * from 'output.pmtiles.layerstats.tsv.gz';
```

Then get the biggest layers:

```sql
select * from layerstats order by layer_bytes desc limit 2;
```

| z  |  x   |  y   |  hilbert  | archived_tile_bytes |  layer   | layer_bytes | layer_features | layer_attr_bytes | layer_attr_keys | layer_attr_values |
|----|------|------|-----------|---------------------|----------|-------------|----------------|------------------|-----------------|-------------------|
| 14 | 6435 | 8361 | 219723809 | 679498              | building | 799971      | 18             | 68               | 2               | 19                |
| 14 | 6435 | 8364 | 219723850 | 603677              | building | 693563      | 18             | 75               | 3               | 19                |

Or the biggest tiles, NOTE: this group by uses a lot of memory so you need to be running in file-backed
mode `duckdb analysis.duckdb`:

```sql
create table tilestats as select
  z, x, y,
  any_value(archived_tile_bytes) gzipped,
  sum(layer_bytes) raw
from layerstats group by z, x, y;
select * from tilestats order by gzipped desc limit 2;
```

| z  |  x   |  y   | gzipped |  raw   |
|----|------|------|---------|--------|
| 14 | 6435 | 8361 | 679498  | 974602 |
| 14 | 6437 | 8362 | 613512  | 883559 |

To make it easier to look at these tiles on a map, you can define following macros that convert z/x/y coordinates to
lat/lons:

```sql
create macro lon(z, x) as (x/2**z) * 360 - 180;
create macro lat_n(z, y) as pi() - 2 * pi() * y/2**z;
create macro lat(z, y) as degrees(atan(0.5*(exp(lat_n(z, y)) - exp(-lat_n(z, y)))));
create or replace macro debug_url(z, x, y) as concat('https://protomaps.github.io/PMTiles/#map=', z + 0.5, '/', round(lat(z, x + 0.5), 5), '/', round(lon(z, y + 0.5), 5));

select z, x, y, debug_url(z, x, y), layer, layer_bytes from layerstats order by layer_bytes desc limit 2;
```

| z  |  x   |  y   |                       debug_url(z, x, y)                       |  layer   | layer_bytes |
|----|------|------|----------------------------------------------------------------|----------|-------------|
| 14 | 6435 | 8361 | https://protomaps.github.io/PMTiles/#map=14.5/35.96912/3.72437 | building | 799971      |
| 14 | 6435 | 8364 | https://protomaps.github.io/PMTiles/#map=14.5/35.96912/3.79028 | building | 693563      |

Drag and drop your pmtiles archive to the pmtiles debugger to inspect the large tiles.

#### Computing Weighted Averages

If you compute a straight average tile size, it will be dominated by ocean tiles that no one looks at. You can compute a
weighted average based on actual usage by joining with a `z, x, y, loads` tile source. For
convenience, `top_osm_tiles.tsv.gz` has the top 1 million tiles from 90 days
of [OSM tile logs](https://planet.openstreetmap.org/tile_logs/) from summer 2023.

You can load these sample weights using duckdb's [httpfs module](https://duckdb.org/docs/extensions/httpfs.html):

```sql
install httpfs;
create table weights as select z, x, y, loads from 'https://raw.githubusercontent.com/onthegomap/planetiler/main/layerstats/top_osm_tiles.tsv.gz';
```

Then compute the weighted average tile size:

```sql
select
sum(gzipped * loads) / sum(loads) / 1000 gzipped_avg_kb,
sum(raw * loads) / sum(loads) / 1000 raw_avg_kb,
from tilestats join weights using (z, x, y);
```

|   gzipped_avg_kb   |    raw_avg_kb     |
|--------------------|-------------------|
| 47.430680122547145 | 68.06047582043456 |

If you are working with an extract, then the low-zoom tiles will dominate, so you can make the weighted average respect
the per-zoom weights that appear globally:

```sql
with zoom_weights as (
  select z, sum(loads) loads from weights group by z
),
zoom_avgs as (
  select
  z,
  sum(gzipped * loads) / sum(loads) gzipped,
  sum(raw * loads) / sum(loads) raw,
  from tilestats join weights using (z, x, y)
  group by z
)
select
  sum(gzipped * loads) / sum(loads) / 1000 gzipped_avg_kb,
  sum(raw * loads) / sum(loads) / 1000 raw_avg_kb,
from zoom_avgs join zoom_weights using (z);
```

|  gzipped_avg_kb   |    raw_avg_kb     |
|-------------------|-------------------|
| 47.42996479265248 | 68.05934476347593 |

