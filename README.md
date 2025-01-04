# Planetiler

Planetiler (_**pla**&middot;nuh&middot;tai&middot;lr_, formerly named "Flatmap") is a tool that generates
[Vector Tiles](https://github.com/mapbox/vector-tile-spec/tree/master/2.1)
from geographic data sources like [OpenStreetMap](https://www.openstreetmap.org/). Planetiler aims to be fast and
memory-efficient so that you can build a map of the world in a few hours on a single machine without any external tools
or database.

Vector tiles contain raw point, line, and polygon geometries that clients like [MapLibre](https://github.com/maplibre)
can use to render custom maps in the browser, native apps, or on a server. Planetiler packages tiles into
an [MBTiles](https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md) (sqlite)
or [PMTiles](https://github.com/protomaps/PMTiles) file that can be served using tools
like [TileServer GL](https://github.com/maptiler/tileserver-gl) or [Martin](https://github.com/maplibre/martin) or
even [queried directly from the browser](https://github.com/protomaps/PMTiles/tree/main/js).
See [awesome-vector-tiles](https://github.com/mapbox/awesome-vector-tiles) for more projects that work with data in this
format.

Planetiler works by mapping input elements to vector tile features, flattening them into a big list, then sorting by
tile ID to group into tiles. See [ARCHITECTURE.md](ARCHITECTURE.md) for more details or
this [blog post](https://medium.com/@onthegomap/dc419f3af75d?source=friends_link&sk=fb71eaa0e2b26775a9d98c81750ec10b)
for more of the backstory.

## Demo

See the [live demo](https://onthegomap.github.io/planetiler-demo/) of vector tiles created by Planetiler
and [hosted by OpenStreetMap US](https://github.com/osmus/tileservice).

[![Planetiler Demo Screenshot](./diagrams/demo.png)](https://onthegomap.github.io/planetiler-demo/)
[© OpenMapTiles](https://www.openmaptiles.org/) [© OpenStreetMap contributors](https://www.openstreetmap.org/copyright)

## Usage

To generate a map of an area using the [OpenMapTiles profile](https://github.com/openmaptiles/planetiler-openmaptiles),
you will need:

- Java 21+ (see [CONTRIBUTING.md](CONTRIBUTING.md)) or [Docker](https://docs.docker.com/get-docker/)
- at least 1GB of free SSD disk space plus 5-10x the size of the `.osm.pbf` file
- at least 0.5x as much free RAM as the input `.osm.pbf` file size

#### To build the map:

Using Java, download `planetiler.jar` from
the [latest release](https://github.com/onthegomap/planetiler/releases/latest)
and run it:

```bash
wget https://github.com/onthegomap/planetiler/releases/latest/download/planetiler.jar
java -Xmx1g -jar planetiler.jar --download --area=monaco
```

Or using Docker:

```bash
docker run -e JAVA_TOOL_OPTIONS="-Xmx1g" -v "$(pwd)/data":/data ghcr.io/onthegomap/planetiler:latest --download --area=monaco
```

:warning: This starts off by downloading about 1GB of [data sources](NOTICE.md#data) required by the OpenMapTiles
profile
including ~750MB for [ocean polygons](https://osmdata.openstreetmap.de/data/water-polygons.html) and ~240MB
for [Natural Earth Data](https://www.naturalearthdata.com/).

<details>
<summary>To download smaller extracts just for Monaco:</summary>

Java:

```bash
java -Xmx1g -jar planetiler.jar --download --area=monaco \
  --water-polygons-url=https://github.com/onthegomap/planetiler/raw/main/planetiler-core/src/test/resources/water-polygons-split-3857.zip \
  --natural-earth-url=https://github.com/onthegomap/planetiler/raw/main/planetiler-core/src/test/resources/natural_earth_vector.sqlite.zip
```

Docker:

```bash
docker run -e JAVA_TOOL_OPTIONS="-Xmx1g" -v "$(pwd)/data":/data ghcr.io/onthegomap/planetiler:latest --download --area=monaco \
  --water-polygons-url=https://github.com/onthegomap/planetiler/raw/main/planetiler-core/src/test/resources/water-polygons-split-3857.zip \
  --natural-earth-url=https://github.com/onthegomap/planetiler/raw/main/planetiler-core/src/test/resources/natural_earth_vector.sqlite.zip
```

You will need the full data sources to run anywhere besides Monaco.

</details>

#### To view tiles locally:

Using [Node.js](https://nodejs.org/en/download/package-manager):

```bash
npm install -g tileserver-gl-light
tileserver-gl-light data/output.mbtiles
```

Or using [Docker](https://docs.docker.com/get-docker/):

```bash
docker run --rm -it -v "$(pwd)/data":/data -p 8080:8080 maptiler/tileserver-gl -p 8080
```

Then open http://localhost:8080 to view tiles.

Some common arguments:

- `--output` tells planetiler where to write output to, and what format to write it in. For
  example `--output=australia.pmtiles` creates a pmtiles archive named `australia.pmtiles`.
  It is best to specify the full path to the file. In docker image you should be using `/data/australia.pmtiles` to let the docker know where to write the file.
- `--download` downloads input sources automatically and `--only-download` exits after downloading
- `--area=monaco` downloads a `.osm.pbf` extract from [Geofabrik](https://download.geofabrik.de/)
- `--osm-path=path/to/file.osm.pbf` points Planetiler at an existing OSM extract on disk
- `-Xmx1g` controls how much RAM to give the JVM (recommended: 0.5x the input .osm.pbf file size to leave room for
  memory-mapped files)
- `--force` overwrites the output file
- `--help` shows all of the options and exits

### Git submodules

Planetiler has a submodule dependency
on [planetiler-openmaptiles](https://github.com/openmaptiles/planetiler-openmaptiles). Add `--recurse-submodules`
to `git clone`, `git pull`, or `git checkout` commands to also update submodule dependencies.

To clone the repo with submodules:

```bash
git clone --recurse-submodules https://github.com/onthegomap/planetiler.git
```

If you already pulled the repo, you can initialize submodules with:

```bash
git submodule update --init
```

To force git to always update submodules (recommended), run this command in your local repo:

```bash
git config --local submodule.recurse true
```

Learn more about working with submodules [here](https://git-scm.com/book/en/v2/Git-Tools-Submodules).

## Generating a Map of the World

See [PLANET.md](PLANET.md).

## Generating Custom Vector Tiles

If you want to customize the OpenMapTiles schema or generate an mbtiles file with OpenMapTiles + extra layers, then
fork https://github.com/openmaptiles/planetiler-openmaptiles make changes there, and run directly from that repo. It
is a standalone Java project with a dependency on Planetiler.

If you want to generate a separate mbtiles file with overlay layers or a full custom basemap, then:

- For simple schemas, run a recent planetiler jar or docker image with a custom schema defined in a yaml
  configuration file. See [planetiler-custommap](planetiler-custommap) for details.
- For complex schemas (or if you prefer working in Java), create a new Java project
  that [depends on Planetiler](#use-as-a-library). See the [planetiler-examples](planetiler-examples) project for a
  working example.

If you want to customize how planetiler works internally, then fork this project, build from source, and
consider [contributing](#contributing) your change back for others to use!

## Benchmarks

Some example runtimes for the OpenMapTiles profile (excluding downloading resources):

|                     Input                      | Version |             Machine             |           Time            | output size  |                                                      Logs                                                      |
|------------------------------------------------|---------|---------------------------------|---------------------------|--------------|----------------------------------------------------------------------------------------------------------------|
| s3://osm-pds/2024/planet-240115.osm.pbf (69GB) | 0.7.0   | c3d-standard-180 (180cpu/720GB) | 22m cpu:44h34m  avg:120   | 69GB pmtiles | [logs](planet-logs/v0.7.0-planet-c3d-standard-180.txt)                                                         |
| s3://osm-pds/2024/planet-240108.osm.pbf (73GB) | 0.7.0   | c7gd.16xlarge (64cpu/128GB)     | 42m cpu:42m28s avg:52     | 69GB pmtiles | [logs](planet-logs/v0.7.0-planet-c7gd-128gb.txt)                                                               |
| s3://osm-pds/2022/planet-220530.osm.pbf (69GB) | 0.5.0   | c6gd.16xlarge (64cpu/128GB)     | 53m cpu:41h58m avg:47.1   | 79GB mbtiles | [logs](planet-logs/v0.5.0-planet-c6gd-128gb.txt), [VisualVM Profile](planet-logs/v0.5.0-planet-c6gd-128gb.nps) |
| s3://osm-pds/2022/planet-220530.osm.pbf (69GB) | 0.5.0   | c6gd.8xlarge (32cpu/64GB)       | 1h27m cpu:37h55m avg:26.1 | 79GB mbtiles | [logs](planet-logs/v0.5.0-planet-c6gd-64gb.txt)                                                                |
| s3://osm-pds/2022/planet-220530.osm.pbf (69GB) | 0.5.0   | c6gd.4xlarge (16cpu/32GB)       | 2h38m cpu:34h3m avg:12.9  | 79GB mbtiles | [logs](planet-logs/v0.5.0-planet-c6gd-32gb.txt)                                                                |

Merging nearby buildings at z13 is very expensive, when run with `--building-merge-z13=false`:

|                     Input                      | Version |             Machine             |           Time           | output size  |                                                                            Logs                                                                            |
|------------------------------------------------|---------|---------------------------------|--------------------------|--------------|------------------------------------------------------------------------------------------------------------------------------------------------------------|
| s3://osm-pds/2024/planet-240115.osm.pbf (69GB) | 0.7.0   | c3d-standard-180 (180cpu/720GB) | 16m cpu:27h45m avg:104   | 69GB pmtiles | [logs](planet-logs/v0.7.0-planet-c3d-standard-180-no-z13-building-merge.txt)                                                                               |
| s3://osm-pds/2024/planet-240108.osm.pbf (73GB) | 0.7.0   | c7gd.16xlarge (64cpu/128GB)     | 29m cpu:23h57 avg:50     | 69GB pmtiles | [logs](planet-logs/v0.7.0-planet-c7gd-128gb-no-z13-building-merge.txt)                                                                                     |
| s3://osm-pds/2024/planet-240108.osm.pbf (73GB) | 0.7.0   | c7gd.2xlarge (8cpu/16GB)        | 3h35m cpu:19h45 avg:5.5  | 69GB pmtiles | [logs](planet-logs/v0.7.0-planet-c7gd-16gb-no-z13-building-merge.txt)                                                                                      |
| s3://osm-pds/2024/planet-240108.osm.pbf (73GB) | 0.7.0   | im4gn.large (2cpu/8GB)          | 18h18m cpu:28h6m avg:1.5 | 69GB pmtiles | [logs](planet-logs/v0.7.0-planet-im4gn-8gb-no-z13-building-merge.txt)                                                                                      |
| s3://osm-pds/2022/planet-220530.osm.pbf (69GB) | 0.5.0   | c6gd.16xlarge (64cpu/128GB)     | 39m cpu:27h4m avg:42.1   | 79GB mbtiles | [logs](planet-logs/v0.5.0-planet-c6gd-128gb-no-z13-building-merge.txt), [VisualVM Profile](planet-logs/v0.5.0-planet-c6gd-128gb-no-z13-building-merge.nps) |

## Alternatives

Some other tools that generate vector tiles from OpenStreetMap data:

- [OpenMapTiles](https://github.com/openmaptiles/openmaptiles) is the reference implementation of
  the [OpenMapTiles schema](https://openmaptiles.org/schema/) that
  the [OpenMapTiles profile](https://github.com/openmaptiles/planetiler-openmaptiles)
  is based on. It uses an intermediate postgres database and operates in two modes:
  1. Import data into database (~1 day) then serve vector tiles directly from the database. Tile serving is slower and
     requires bigger machines, but lets you easily incorporate realtime updates
  2. Import data into database (~1 day) then pregenerate every tile for the planet into an mbtiles file which
     takes [over 100 days](https://github.com/openmaptiles/openmaptiles/issues/654#issuecomment-724606293)
     or a cluster of machines, but then tiles can be served faster on smaller machines
- [Tilemaker](https://github.com/systemed/tilemaker) uses a similar approach to Planetiler (no intermediate database),
  is more mature, and has a convenient lua API for building custom profiles without recompiling the tool, but takes
  [about a day](https://github.com/systemed/tilemaker/issues/315#issue-994322040) to generate a map of the world

Some companies that generate and host tiles for you:

- [Mapbox](https://www.mapbox.com/) - data from the pioneer of vector tile technologies
- [Maptiler](https://www.maptiler.com/) - data from the creator of OpenMapTiles schema
- [Stadia Maps](https://stadiamaps.com/) - what [onthegomap.com](https://onthegomap.com/) uses in production

If you want to host tiles yourself but have someone else generate them for you, those companies also offer plans to
download regularly-updated tilesets.

## Features

- Supports [Natural Earth](https://www.naturalearthdata.com/),
  OpenStreetMap [.osm.pbf](https://wiki.openstreetmap.org/wiki/PBF_Format),
  [`geopackage`](https://www.geopackage.org/),
  and [Esri Shapefiles](https://en.wikipedia.org/wiki/Shapefile) data sources
- Writes to [MBTiles](https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md) or
  or [PMTiles](https://github.com/protomaps/PMTiles) output.
- Java-based [Profile API](planetiler-core/src/main/java/com/onthegomap/planetiler/Profile.java) to customize how source
  elements map to vector tile features, and post-process generated tiles
  using [JTS geometry utilities](https://github.com/locationtech/jts)
- [YAML config file format](planetiler-custommap) that lets you create custom schemas without writing Java code
- Merge nearby lines or polygons with the same tags before emitting vector tiles
- Automatically fixes self-intersecting polygons
- Built-in OpenMapTiles profile based on [OpenMapTiles](https://openmaptiles.org/) v3.13.1
- Optionally download additional name translations for elements from Wikidata
- Export real-time stats to a [prometheus push gateway](https://github.com/prometheus/pushgateway) using
  `--pushgateway=http://user:password@ip` argument (and a [grafana dashboard](grafana.json) for viewing)
- Automatically downloads region extracts from [Geofabrik](https://download.geofabrik.de/)
  using `geofabrik:australia` shortcut as a source URL
- Unit-test profiles to verify mapping logic, or integration-test to verify the actual contents of a generated mbtiles
  file ([example](planetiler-examples/src/test/java/com/onthegomap/planetiler/examples/BikeRouteOverlayTest.java))

## Limitations

- It is harder to join and group data than when using database. Planetiler automatically groups features into tiles, so
  you can easily post-process nearby features in the same tile before emitting, but if you want to group or join across
  features in different tiles, then you must explicitly store data when processing a feature to use with later features
  or store features and defer processing until an input source is
  finished  ([boundary layer example](https://github.com/onthegomap/planetiler/blob/9e9cf7c413027ffb3ab5c7436d11418935ae3f6a/planetiler-basemap/src/main/java/com/onthegomap/planetiler/basemap/layers/Boundary.java#L294))
- Planetiler only does full imports from `.osm.pbf` snapshots, there is no way to incorporate real-time updates.

## Use as a library

Since Java 22, you can use Planetile as a library with a custom profile by running:

`java -cp planetiler.jar Profile.java`.

See [the examples](https://github.com/onthegomap/planetiler-examples) for more details.

Planetiler can be used as a maven-style dependency in a Java project using the settings below:

### Maven

Add this repository block to your `pom.xml`:

```xml
<repositories>
  <repository>
    <id>osgeo</id>
    <name>OSGeo Release Repository</name>
    <url>https://repo.osgeo.org/repository/release/</url>
    <snapshots>
      <enabled>false</enabled>
    </snapshots>
    <releases>
      <enabled>true</enabled>
    </releases>
  </repository>
</repositories>
```

Then add the following dependency:

```xml
<dependency>
  <groupId>com.onthegomap.planetiler</groupId>
  <artifactId>planetiler-core</artifactId>
  <version>${planetiler.version}</version>
</dependency>
```

### Gradle

Set up your repositories block::

```groovy
mavenCentral()
maven {
    url "https://repo.osgeo.org/repository/release/"
}
```

Set up your dependencies block:

```groovy
implementation 'com.onthegomap.planetiler:planetiler-core:<version>'
```

## Contributing

Pull requests are welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## Support

For general questions, check out the #planetiler channel on [OSM-US Slack](https://osmus.slack.com/) (get an
invite [here](https://slack.openstreetmap.us/)), or start
a [GitHub discussion](https://github.com/onthegomap/planetiler/discussions).

Found a bug or have a feature request? Open a [GitHub issue](https://github.com/onthegomap/planetiler/issues) to report.

This is a side project, so support is limited. If you have the time and ability, feel free to open a pull request to fix
issues or implement new features.

## Acknowledgement

Planetiler is made possible by these awesome open source projects:

- [OpenMapTiles](https://openmaptiles.org/) for the [schema](https://openmaptiles.org/schema/)
  and [reference implementation](https://github.com/openmaptiles/openmaptiles)
  that
  the [openmaptiles profile](https://github.com/openmaptiles/planetiler-openmaptiles/tree/main/src/main/java/org/openmaptiles/layers)
  is based on
- [Graphhopper](https://www.graphhopper.com/) for basis of utilities to process OpenStreetMap data in Java
- [JTS Topology Suite](https://github.com/locationtech/jts) for working with vector geometries
- [Geotools](https://github.com/geotools/geotools) for shapefile processing
- [SQLite JDBC Driver](https://github.com/xerial/sqlite-jdbc) for reading Natural Earth data and writing MBTiles files
- [MessagePack](https://msgpack.org/) for compact binary encoding of intermediate map features
- [geojson-vt](https://github.com/mapbox/geojson-vt) for the basis of
  the [stripe clipping algorithm](planetiler-core/src/main/java/com/onthegomap/planetiler/render/TiledGeometry.java)
  that planetiler uses to slice geometries into tiles
- [java-vector-tile](https://github.com/ElectronicChartCentre/java-vector-tile) for the basis of
  the [vector tile encoder](planetiler-core/src/main/java/com/onthegomap/planetiler/VectorTile.java)
- [imposm3](https://github.com/omniscale/imposm3) for the basis
  of [OSM multipolygon processing](planetiler-core/src/main/java/com/onthegomap/planetiler/reader/osm/OsmMultipolygon.java)
  and [tag parsing utilities](planetiler-core/src/main/java/com/onthegomap/planetiler/util/Imposm3Parsers.java)
- [HPPC](http://labs.carrotsearch.com/) for high-performance primitive Java collections
- [Osmosis](https://wiki.openstreetmap.org/wiki/Osmosis) for Java utilities to parse OpenStreetMap data
- [JNR-FFI](https://github.com/jnr/jnr-ffi) for utilities to access low-level system utilities to improve memory-mapped
  file performance.
- [cel-java](https://github.com/projectnessie/cel-java) for the Java implementation of
  Google's [Common Expression Language](https://github.com/google/cel-spec) that powers dynamic expressions embedded in
  schema config files.
- [PMTiles](https://github.com/protomaps/PMTiles) optimized tile storage format
- [Apache Parquet](https://github.com/apache/parquet-mr) to support reading geoparquet files in java (with dependencies
  minimized by [parquet-floor](https://github.com/strategicblue/parquet-floor))

See [NOTICE.md](NOTICE.md) for a full list and license details.

## Author

Planetiler was created by [Michael Barry](https://github.com/msbarry) for future use generating custom basemaps or
overlays for [On The Go Map](https://onthegomap.com).

## License and Attribution

Planetiler source code is licensed under the [Apache 2.0 License](LICENSE), so it can be used and modified in commercial
or other open source projects according to the license guidelines.

Maps built using planetiler do not require any special attribution, but the data or schema used might. Any maps
generated from OpenStreetMap data
must [visibly credit OpenStreetMap contributors](https://www.openstreetmap.org/copyright). Any map generated with the
profile based on OpenMapTiles or a derivative
must [visibly credit OpenMapTiles](https://github.com/openmaptiles/openmaptiles/blob/master/LICENSE.md#design-license-cc-by-40)
as well.

