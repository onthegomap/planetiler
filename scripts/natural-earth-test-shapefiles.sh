#!/usr/bin/env bash
# Builds planetiler-core/src/test/resources/natural_earth_shapefiles.zip: the Natural Earth tables the OpenMapTiles
# profile reads, clipped to Monaco with a 1 degree margin, laid out like Natural Earth's natural_earth_vector.zip
# package. Tables with nothing in the box stay as empty shapefiles so the zip has every table the profile needs.
# Requires duckdb with the spatial extension.
set -euo pipefail
cd "$(dirname "$0")/.."
rm -rf target/natural_earth_shapefiles

# scale, theme, table, dimension to keep after clipping (1 points, 2 lines, 3 polygons), shapefile geometry type
while read -r scale theme table dimension type; do
  mkdir -p "target/natural_earth_shapefiles/${scale}_${theme}"
  # duckdb reads stdin otherwise, which would consume the table list
  duckdb < /dev/null -c "
    LOAD spatial;
    SET geometry_always_xy = true;
    COPY (
      SELECT * EXCLUDE geom,
        ST_CollectionExtract(ST_Intersection(geom, ST_MakeEnvelope(6.4, 42.7, 8.45, 44.75)), $dimension) AS geom
      FROM ST_Read('/vsizip//vsicurl/https://naciscdn.org/naturalearth/$scale/$theme/$table.zip/$table.shp')
      WHERE ST_Intersects(geom, ST_MakeEnvelope(6.4, 42.7, 8.45, 44.75))
    ) TO 'target/natural_earth_shapefiles/${scale}_${theme}/$table.shp' WITH (
      FORMAT GDAL, DRIVER 'ESRI Shapefile', SRS 'EPSG:4326', GEOMETRY_TYPE '$type',
      LAYER_CREATION_OPTIONS 'ENCODING=UTF-8'
    );"
done <<EOF
10m cultural ne_10m_admin_0_boundary_lines_land 2 LINESTRING
10m cultural ne_10m_admin_0_countries 3 POLYGON
10m cultural ne_10m_admin_1_states_provinces 3 POLYGON
10m cultural ne_10m_admin_1_states_provinces_lines 2 LINESTRING
10m cultural ne_10m_populated_places 1 POINT
10m physical ne_10m_antarctic_ice_shelves_polys 3 POLYGON
10m physical ne_10m_geography_marine_polys 3 POLYGON
10m physical ne_10m_glaciated_areas 3 POLYGON
10m physical ne_10m_lakes 3 POLYGON
10m physical ne_10m_ocean 3 POLYGON
50m cultural ne_50m_admin_0_boundary_lines_land 2 LINESTRING
50m cultural ne_50m_urban_areas 3 POLYGON
50m physical ne_50m_antarctic_ice_shelves_polys 3 POLYGON
50m physical ne_50m_glaciated_areas 3 POLYGON
50m physical ne_50m_lakes 3 POLYGON
50m physical ne_50m_ocean 3 POLYGON
50m physical ne_50m_rivers_lake_centerlines 2 LINESTRING
110m cultural ne_110m_admin_0_boundary_lines_land 2 LINESTRING
110m physical ne_110m_glaciated_areas 3 POLYGON
110m physical ne_110m_lakes 3 POLYGON
110m physical ne_110m_ocean 3 POLYGON
110m physical ne_110m_rivers_lake_centerlines 2 LINESTRING
EOF

rm -f planetiler-core/src/test/resources/natural_earth_shapefiles.zip
cd target/natural_earth_shapefiles && zip -q -X -r ../../planetiler-core/src/test/resources/natural_earth_shapefiles.zip .
