## Licenses

FlatMap licensed under MIT license.

Copyright 2021 Michael Barry and FlatMap Contributors.

The `flatmap-core` module includes the following software:

- Maven Dependencies:
  - Jackson for JSON/XML handling (Apache license)
  - Prometheus JVM Client (Apache license)
  - SLF4j (MIT license)
  - log4j (Apache license)
  - org.locationtech.jts:jts-core (Eclipse Distribution License)
  - org.geotools:gt-shapefile (LGPL)
  - org.geotools:gt-epsg-hsql
    (LGPL, [BSD for HSQL](https://github.com/geotools/geotools/blob/main/licenses/HSQL.md)
    , [EPSG](https://github.com/geotools/geotools/blob/main/licenses/EPSG.md))
  - org.mapdb:mapdb (Apache license)
  - org.msgpack:msgpack-core (Apache license)
  - org.xerial:sqlite-jdbc (Apache license)
  - com.ibm.icu:icu4j ([ICU license](https://github.com/unicode-org/icu/blob/main/icu4c/LICENSE))
- Adapted code:
  - `DouglasPeuckerSimplifier` from [JTS](https://github.com/locationtech/jts) (EDL)
  - `OsmMultipolygon` from [imposm3](https://github.com/omniscale/imposm3) (Apache license)
  - `TiledGeometry` from [geojson-vt](https://github.com/mapbox/geojson-vt) (ISC license)
  - `VectorTileEncoder`
    from [java-vector-tile](https://github.com/ElectronicChartCentre/java-vector-tile) (Apache
    license)
  - `Imposm3Parsers` from [imposm3](https://github.com/omniscale/imposm3) (Apache license)

Additionally, the `flatmap-openmaptiles` module is a Java port
of [OpenMapTiles](https://github.com/openmaptiles/openmaptiles):

- Maven Dependencies:
  - org.yaml:snakeyaml (Apache license)
  - org.commonmark:commonmark (BSD 2-clause license)
- Code adapted from OpenMapTiles (BSD 3-Clause License):
  - `generated` package generated from OpenMapTiles
  - All code in `layers` package ported from OpenMapTiles
  - `LanguageUtils` ported from OpenMapTiles
- Schema
  - The cartography and visual design features of the map tile schema are licensed
    under [CC-BY 4.0](http://creativecommons.org/licenses/by/4.0/). Products or services using maps
    derived from OpenMapTiles schema need to visibly credit "OpenMapTiles.org" or reference
    "OpenMapTiles" with a link to http://openmaptiles.org/. More
    details [here](https://github.com/openmaptiles/openmaptiles/blob/master/LICENSE.md#design-license-cc-by-40)

## Data

|source | license | used as default | included in repo |
|-------|---------|-----------------|------------------|
| OpenStreetMap (OSM) data | [ODBL](https://www.openstreetmap.org/copyright) | yes | yes
| Natural Earth | [public domain](https://www.naturalearthdata.com/about/terms-of-use/) | yes | yes
| OSM Lakelines | [MIT](https://github.com/lukasmartinelli/osm-lakelines), data from OSM [ODBL](https://www.openstreetmap.org/copyright) | yes | no
| OSM Water Polygons | [acknowledgement](https://osmdata.openstreetmap.de/info/license.html), data from OSM [ODBL](https://www.openstreetmap.org/copyright) | yes | yes
| Wikidata name translations | [CCO](https://www.wikidata.org/wiki/Wikidata:Licensing) | no | no
