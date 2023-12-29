## Licenses

Planetiler licensed under the Apache license, Version 2.0

Copyright 2021 Michael Barry and Planetiler Contributors.

Planetiler includes the following software:

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
  - org.msgpack:msgpack-core (Apache license)
  - org.xerial:sqlite-jdbc (Apache license)
  - com.ibm.icu:icu4j ([ICU license](https://github.com/unicode-org/icu/blob/main/icu4c/LICENSE))
  - com.google.guava:guava (Apache license)
  - com.google.protobuf:protobuf-java (BSD 3-Clause License)
  - com.carrotsearch:hppc (Apache license)
  - com.github.jnr:jnr-ffi (Apache license)
  - org.roaringbitmap:RoaringBitmap (Apache license)
  - org.projectnessie.cel:cel-tools (Apache license)
  - mil.nga.geopackage:geopackage (MIT license)
  - org.snakeyaml:snakeyaml-engine (Apache license)
  - org.commonmark:commonmark (BSD 2-clause license)
  - org.tukaani:xz (public domain)
  - org.luaj:luaj-jse (MIT license)
  - org.apache.bcel:bcel (Apache license)
- Adapted code:
  - `DouglasPeuckerSimplifier` from [JTS](https://github.com/locationtech/jts) (EDL)
  - `OsmMultipolygon` from [imposm3](https://github.com/omniscale/imposm3) (Apache license)
  - `TiledGeometry` from [geojson-vt](https://github.com/mapbox/geojson-vt) (ISC license)
  - `VectorTileEncoder`
    from [java-vector-tile](https://github.com/ElectronicChartCentre/java-vector-tile) (Apache license)
  - `Imposm3Parsers` from [imposm3](https://github.com/omniscale/imposm3) (Apache license)
  - `PbfDecoder` from [osmosis](https://github.com/openstreetmap/osmosis) (Public Domain)
  - `PbfFieldDecoder` from [osmosis](https://github.com/openstreetmap/osmosis) (Public Domain)
  - `Madvise` from [uppend](https://github.com/upserve/uppend/) (MIT License)
  - `ArrayLongMinHeap` implementations from [graphhopper](https://github.com/graphhopper/graphhopper) (Apache license)
  - `Hilbert` implementation
    from [github.com/rawrunprotected/hilbert_curves](https://github.com/rawrunprotected/hilbert_curves) (Public Domain)
  - `osmformat.proto` and `fileformat.proto` (generates `Osmformat.java` and `Fileformat.java`)
    from [openstreetmap/OSM-binary](https://github.com/openstreetmap/OSM-binary/tree/master/osmpbf) (MIT License)
  - `VarInt` from [Bazel](https://github.com/bazelbuild/bazel) (Apache license)
  - `SeekableInMemoryByteChannel`
    from [Apache Commons compress](https://commons.apache.org/proper/commons-compress/apidocs/org/apache/commons/compress/utils/SeekableInMemoryByteChannel.html) (
    Apache License)
  - Several classes in `org.luaj.vm2.*` from [luaj](https://github.com/luaj/luaj) (MIT License)
- [`planetiler-openmaptiles`](https://github.com/openmaptiles/planetiler-openmaptiles) submodule (BSD 3-Clause License)
- Schema
  - The cartography and visual design features of the map tile schema are licensed
    under [CC-BY 4.0](http://creativecommons.org/licenses/by/4.0/). Products or services using maps derived from
    OpenMapTiles schema need to visibly credit "OpenMapTiles.org" or reference
    "OpenMapTiles" with a link to http://openmaptiles.org/. More
    details [here](https://github.com/openmaptiles/openmaptiles/blob/master/LICENSE.md#design-license-cc-by-40)

## Data

|           source           |                                                               license                                                                | used as default | included in repo |
|----------------------------|--------------------------------------------------------------------------------------------------------------------------------------|-----------------|------------------|
| OpenStreetMap (OSM) data   | [ODBL](https://www.openstreetmap.org/copyright)                                                                                      | yes             | yes              |
| Natural Earth              | [public domain](https://www.naturalearthdata.com/about/terms-of-use/)                                                                | yes             | yes              |
| OSM Lakelines              | [MIT](https://github.com/lukasmartinelli/osm-lakelines), data from OSM [ODBL](https://www.openstreetmap.org/copyright)               | yes             | no               |
| OSM Water Polygons         | [acknowledgement](https://osmdata.openstreetmap.de/info/license.html), data from OSM [ODBL](https://www.openstreetmap.org/copyright) | yes             | yes              |
| Wikidata name translations | [CCO](https://www.wikidata.org/wiki/Wikidata:Licensing)                                                                              | no              | no               |

