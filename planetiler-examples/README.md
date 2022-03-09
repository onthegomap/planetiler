# Planetiler Example Project

This is a minimal example project that shows how to create custom maps with Planetiler.

Requirements:

- Java 16+ (see [CONTIRBUTING.md](../CONTRIBUTING.md))
  - on mac: `brew install --cask temurin`
- [Maven](https://maven.apache.org/install.html)
  - on mac: `brew install maven`
- [Node.js](https://nodejs.org/en/download/)
  - on mac: `brew install node`
- [TileServer GL](https://github.com/maptiler/tileserver-gl)
  - `npm install -g tileserver-gl-light`
- Also recommended: [IntelliJ IDEA](https://www.jetbrains.com/help/idea/installation-guide.html)
- Disk: 5-10x as much free space as the input data
- RAM: 1.5x the size of the `.osm.pbf` file

First, make a copy of this example project. It contains:

- [standalone.pom.xml](./standalone.pom.xml) - build instructions for Maven:
  - `com.onthegomap:planetiler-core` main Planetiler dependency
  - `com.onthegomap:planetiler-core` test dependency for test utilities
  - `maven-assembly-plugin` build plugin configuration to create a single executable jar file from Maven's `package`
    goal command
- `pom.xml` exists for the parent pom.xml to treat this as a child project, you can replace with `standalone.pom.xml`
  or append `--file standalone.pom.xml` to every maven command to run as a standalone project.
- [src/main/java/com/onthegomap/planetiler/examples](src/main/java/com/onthegomap/planetiler/examples) - some minimal
  example map profiles:
  - [ToiletsOverlay](src/main/java/com/onthegomap/planetiler/examples/ToiletsOverlay.java) - demonstrates how to build a
    simple overlay with toilets locations from OpenStreetMap
  - [BikeRouteOverlay](src/main/java/com/onthegomap/planetiler/examples/BikeRouteOverlay.java) - demonstrates how to use
    OSM relations to build an overlay map of [bicycle routes](https://wiki.openstreetmap.org/wiki/Tag:route=bicycle)
  - [ToiletsOverlayLowLevelApi](src/main/java/com/onthegomap/planetiler/examples/ToiletsOverlayLowLevelApi.java)
    &nbsp;- alternate driver for the ToiletsOverlay using lower-level Planetiler APIs
- [src/test/java/com/onthegomap/planetiler/examples](src/main/java/com/onthegomap/planetiler/examples)
  unit and integration tests for each of the map generators

Then, create a new class that implements `com.onthegomap.planetiler.Profile`:

```java
package com.onthegomap.planetiler.examples;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.reader.SourceFeature;
import java.nio.file.Path;

public class MyProfile implements Profile {
  @Override
  public String name() {
    // name that shows up in the MBTiles metadata table
    return "My Profile";
  }
}
```

Then, implement the `processFeature()` method that determines what vector tile features to emit for each source feature.
For example, to include a map of [toilets from OpenStreetMap](https://wiki.openstreetmap.org/wiki/Tag:amenity=toilets)
at zoom level 12 and above:

```java
@Override
public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
  if (sourceFeature.isPoint() && sourceFeature.hasTag("amenity", "toilets")) {
    features.point("toilets") // create a point in layer named "toilets"
      .setMinZoom(12)
      .setAttr("customers_only", sourceFeature.hasTag("access", "customers"))
      .setAttr("indoor", sourceFeature.getBoolean("indoor"))
      .setAttr("name", sourceFeature.getTag("name"))
      .setAttr("operator", sourceFeature.getTag("operator"));
  }
}
```

Next, add a `main` entrypoint that
uses [Planetiler](../planetiler-core/src/main/java/com/onthegomap/planetiler/Planetiler.java) to define input sources
and default input/output paths:

```java
public static void main(String... args) throws Exception {
  Planetiler.create(args)
    .setProfile(new MyProfile())
    // if input.pbf not found, download Monaco from Geofabrik
    .addOsmSource("osm", Path.of("data", "sources", "input.pbf"), "geofabrik:monaco")
    .overwriteOutput("mbtiles", Path.of("data", "toilets.mbtiles"))
    .run();
}
```

Then build the application into a single jar file with all dependencies included:

```bash
mvn clean package --file standalone.pom.xml
```

And run the application:

```bash
java -cp target/*-with-deps.jar com.onthegomap.planetilerler.examples.MyProfile
```

Then, to inspect the tiles:

```bash
tileserver-gl-light --mbtiles data/toilets.mbtiles
```

Finally, open http://localhost:8080 to see your tiles.

## Testing your profile

Unit tests verify the logic for mapping source features to vector tile features, and integration tests run the entire
profile end-to-end and ensure the output vector tiles contain features you
expect.  [TestUtils](../planetiler-core/src/test/java/com/onthegomap/planetiler/TestUtils.java) contains utilities for
unit and integration testing.

A basic unit test:

```java
@Test
public void unitTest() {
  var profile = new MyProfile();
  var node = SimpleFeature.create(
    TestUtils.newPoint(1, 2),
    Map.of("amenity", "toilets")
  );
  List<FeatureCollector.Feature> mapFeatures = TestUtils.processSourceFeature(node, profile);
  // Then inspect attributes of each of vector tile fetures emitted...
  assertEquals(1, mapFeatures.length);
  assertEquals(12, mapFeatures.get(0).getMinZoom());
}
```

A basic integration test:

```java
@Test
public void integrationTest(@TempDir Path tmpDir) throws Exception {
  Path mbtilesPath = tmpDir.resolve("output.mbtiles");
  MyProfile.main(
    "--osm_path=" + TestUtils.pathToResource("monaco-latest.osm.pbf"),
    "--tmp=" + tmpDir,
    "--mbtiles=" + mbtilesPath,
  ));
  try (Mbtiles mbtiles = Mbtiles.newReadOnlyDatabase(mbtilesPath)) {
    Map<String, String> metadata = mbtiles.metadata().getAll();
    assertEquals("My Profile", metadata.get("name"));
    // then inspect features in the emitted vector tiles
    TestUtils.assertNumFeatures(mbtiles, "toilets", 14, Map.of(), GeoUtils.WORLD_LAT_LON_BOUNDS,
      34, Point.class);
  }
}
```

See [ToiletsProfileTest](./src/test/java/com/onthegomap/planetiler/examples/ToiletsProfileTest.java)
for a complete unit and integration test.

## Next Steps

Check out:

- The other [minimal examples](./src/main/java/com/onthegomap/planetiler/examples)
- The [basemap profile](../planetiler-basemap) for a full-featured example of a complex profile with processing broken
  out into a handler per layer
- [Planetiler](../planetiler-core/src/main/java/com/onthegomap/planetiler/Planetiler.java) for more options when
  invoking the program
- [FeatureCollector](../planetiler-core/src/main/java/com/onthegomap/planetiler/FeatureCollector.java)
  for the full API to construct vector tile features
- [SourceFeature](../planetiler-core/src/main/java/com/onthegomap/planetiler/reader/SourceFeature.java)
  and [WithTags](../planetiler-core/src/main/java/com/onthegomap/planetiler/reader/WithTags.java)
  for the full API to extract data from source features
- [Profile](../planetiler-core/src/main/java/com/onthegomap/planetiler/Profile.java) for the rest of methods you can
  implement to:
  - customize OSM relation preprocessing
  - set MBTiles metadata attributes
  - get notified when a source finishes processing
  - and post-process vector-tile features (i.e. merge touching linestrings or polygons)

