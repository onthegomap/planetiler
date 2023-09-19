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

Load a layer stats file in [duckdb](https://duckdb.org/):

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

To get a table of biggest layers by zoom:

```sql
pivot (
  select z, layer, (max(layer_bytes)/1000)::int size from layerstats group by z, layer order by z asc
) on z using sum(size);
```

|        layer        |  0  |  1  | 10  | 11  | 12  | 13  | 14  |  2  |  3  |  4  |  5  | 6  |  7  |  8  |  9  |
|---------------------|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|----|-----|-----|-----|
| boundary            | 10  | 75  | 24  | 18  | 32  | 18  | 10  | 85  | 53  | 44  | 25  | 18 | 15  | 15  | 29  |
| landcover           | 2   | 1   | 153 | 175 | 166 | 111 | 334 | 8   | 5   | 3   | 31  | 18 | 273 | 333 | 235 |
| place               | 116 | 191 | 16  | 14  | 10  | 25  | 57  | 236 | 154 | 123 | 58  | 30 | 21  | 15  | 14  |
| water               | 8   | 4   | 133 | 94  | 167 | 116 | 90  | 11  | 9   | 15  | 13  | 89 | 114 | 126 | 109 |
| water_name          | 7   | 7   | 4   | 4   | 4   | 4   | 9   | 7   | 6   | 4   | 3   | 3  | 3   | 3   | 4   |
| waterway            |     |     | 20  | 16  | 60  | 66  | 73  |     | 1   | 4   | 2   | 18 | 13  | 10  | 28  |
| park                |     |     | 90  | 56  | 48  | 19  | 50  |     |     | 53  | 135 | 89 | 75  | 68  | 82  |
| landuse             |     |     | 176 | 132 | 66  | 140 | 52  |     |     | 3   | 2   | 33 | 67  | 95  | 107 |
| transportation      |     |     | 165 | 95  | 312 | 187 | 133 |     |     | 60  | 103 | 61 | 126 | 287 | 284 |
| transportation_name |     |     | 30  | 18  | 65  | 59  | 169 |     |     |     |     | 32 | 20  | 18  | 13  |
| mountain_peak       |     |     | 7   | 8   | 6   | 295 | 232 |     |     |     |     |    | 8   | 7   | 9   |
| aerodrome_label     |     |     | 4   | 4   | 4   | 4   | 4   |     |     |     |     |    |     | 4   | 4   |
| aeroway             |     |     | 16  | 25  | 34  | 30  | 18  |     |     |     |     |    |     |     |     |
| poi                 |     |     |     |     | 22  | 10  | 542 |     |     |     |     |    |     |     |     |
| building            |     |     |     |     |     | 69  | 800 |     |     |     |     |    |     |     |     |
| housenumber         |     |     |     |     |     |     | 413 |     |     |     |     |    |     |     |     |

To get biggest tiles:

```sql
create table tilestats as select
z, x, y,
any_value(archived_tile_bytes) gzipped,
sum(layer_bytes) raw
from layerstats group by z, x, y;
select * from tilestats order by gzipped desc limit 2;
```

NOTE: this group by uses a lot of memory so you need to be running in file-backed
mode `duckdb analysis.duckdb` (not in-memory mode)

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
create or replace macro debug_url(z, x, y) as concat(
  'https://protomaps.github.io/PMTiles/#map=',
  z + 0.5, '/',
  round(lat(z, x + 0.5), 5), '/',
  round(lon(z, y + 0.5), 5)
);

select z, x, y, debug_url(z, x, y), layer, layer_bytes
from layerstats order by layer_bytes desc limit 2;
```

| z  |  x   |  y   |                       debug_url(z, x, y)                       |  layer   | layer_bytes |
|----|------|------|----------------------------------------------------------------|----------|-------------|
| 14 | 6435 | 8361 | https://protomaps.github.io/PMTiles/#map=14.5/35.96912/3.72437 | building | 799971      |
| 14 | 6435 | 8364 | https://protomaps.github.io/PMTiles/#map=14.5/35.96912/3.79028 | building | 693563      |

Drag and drop your pmtiles archive to the pmtiles debugger to see the large tiles on a map. You can also switch to the
"inspect" tab to inspect an individual tile.

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

