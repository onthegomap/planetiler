package com.onthegomap.planetiler.examples;

import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.archive.TileArchiveWriter;
import com.onthegomap.planetiler.archive.TileArchives;
import com.onthegomap.planetiler.archive.WriteableTileArchive;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.collection.LongLongMap;
import com.onthegomap.planetiler.collection.LongLongMultimap;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.TileOrder;
import com.onthegomap.planetiler.reader.osm.OsmInputFile;
import com.onthegomap.planetiler.reader.osm.OsmReader;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.FileUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Alternative driver program for {@link ToiletsOverlay} that uses the low-level planetiler APIs instead of the
 * {@link Planetiler} convenience wrapper.
 * <p>
 * To run this example:
 * <ol>
 * <li>Download a .osm.pbf extract (see <a href="https://download.geofabrik.de/">Geofabrik download site</a></li>
 * <li>then build the examples: {@code mvn clean package}</li>
 * <li>then run this example:
 * {@code java -cp target/*-fatjar.jar com.onthegomap.planetiler.examples.ToiletsOverlayLowLevelApi}</li>
 * <li>then run the demo tileserver: {@code tileserver-gl-light --mbtiles=data/toilets.mbtiles}</li>
 * <li>and view the output at <a href="http://localhost:8080">localhost:8080</a></li>
 * </ol>
 */
public class ToiletsOverlayLowLevelApi {

  private static final Logger LOGGER = LoggerFactory.getLogger(ToiletsOverlayLowLevelApi.class);

  public static void main(String[] args) throws Exception {
    run(
      Path.of("data", "sources", "input.pbf"),
      Path.of("data", "tmp"),
      Path.of("data", "toilets.mbtiles")
    );
  }

  static void run(Path input, Path tmpDir, Path output) throws IOException {
    // Collect runtime statistics in memory. Alternatively you can push them to
    // prometheus using a push gateway (see https://github.com/prometheus/pushgateway)
    Stats stats = Stats.inMemory();
    Profile profile = new ToiletsOverlay();

    // use default settings, but only allow overrides from -Dkey=value jvm arguments
    PlanetilerConfig config = PlanetilerConfig.from(Arguments.fromJvmProperties());

    // extract mbtiles metadata from profile
    TileArchiveMetadata tileArchiveMetadata = new TileArchiveMetadata(profile, config);

    // overwrite output each time
    FileUtils.deleteFile(output);
    // make sure temp directories exist
    Files.createDirectories(tmpDir);

    /*
     * Set up the FeatureGroup utility that groups features by tile. To group features by tile,
     * FeatureSort first sorts them by a key that includes the tile ID in it, then FeatureGroup
     * reads features in order, batching all consecutive features in the same tile together.
     *
     * Most applications should use external merge sort which writes features to disk, sorts
     * chunks from disk in parallel, and iterates through the sorted chunks to efficiently
     * sort more data than fits in memory - but if you have a lot of RAM there is FeatureSort.inMemory
     * option too.
     */
    FeatureGroup featureGroup = FeatureGroup.newDiskBackedFeatureGroup(
      TileOrder.TMS,
      tmpDir.resolve("feature.db"),
      profile, config, stats
    );

    try (
      /*
       * OSM nodes each have a latitude/longitude, and ways are a collection of node IDs so to
       * get the shape of ways we need to store the latitude and longitude of each node and look
       * them up when processing each way. There are several in-memory and disk-based options,
       * but even for the disk-based ones you'll likely need enough RAM to fit the whole thing for
       * random-access lookups to be fast.
       *
       * BUT, since this profile only needs nodes and not ways we can use the noop version to avoid storing
       * any node locations.
       */
      var nodeLocations = LongLongMap.noop();
      var multipolygons = LongLongMultimap.noop();
      var osmReader = new OsmReader("osm", new OsmInputFile(input), nodeLocations, multipolygons, profile, stats)
    ) {
      // Normally you need to run OsmReader.pass1(config) first which stores node locations and preprocesses relations for
      // way processing, and counts elements. But since this profile only processes nodes we can skip pass 1.
      LOGGER.info("Skipping OsmReader.pass1(config) since we don't care about ways or relations");

      // make the second pass through OSM data which emits output map features for each input source feature
      osmReader.pass2(featureGroup, config);
    } finally {
      // normally you'll want to release any resources/memory held by the profile
      // although in this example it's not really necessary
      profile.release();
    }

    // sort the features that were written to disk to prepare for grouping them into tiles
    featureGroup.prepare();

    // then process rendered features, grouped by tile, encoding them into binary vector tile format
    // and writing to the output mbtiles file.
    try (WriteableTileArchive db = TileArchives.newWriter(output, config)) {
      TileArchiveWriter.writeOutput(featureGroup, db, () -> FileUtils.fileSize(output), tileArchiveMetadata, config,
        stats);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to write to " + output, e);
    }

    // dump recorded timings at the end
    stats.printSummary();
  }
}
