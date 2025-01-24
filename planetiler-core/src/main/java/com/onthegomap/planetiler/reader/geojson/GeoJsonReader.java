package com.onthegomap.planetiler.reader.geojson;

import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.reader.SimpleFeature;
import com.onthegomap.planetiler.reader.SimpleReader;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.SourceFeatureProcessor;
import com.onthegomap.planetiler.stats.Stats;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;

/**
 * Utility that reads {@link SourceFeature SourceFeatures} from the vector geometries contained in a GeoJSON file.
 * <p>
 * The file can contain newline-delimited features, or features in a feature collection.
 *
 * @see <a href="https://stevage.github.io/ndgeojson/">Newline-delimted geojson</a>
 * @see <a href="https://datatracker.ietf.org/doc/html/rfc7946">GeoJSON specification (RFC 7946)</a>
 */
public class GeoJsonReader extends SimpleReader<SimpleFeature> {

  private volatile long count = -1;
  private final String layer;
  private final GeoJson file;

  GeoJsonReader(String sourceName, Path input) {
    super(sourceName);
    this.file = GeoJson.from(input);
    layer = input.getFileName().toString().replaceFirst("\\.[^.]+$", ""); // remove file extention.
  }

  /**
   * Renders map features for all elements in a GeoJSON file based on the mapping logic defined in {@code
   * profile}.
   *
   * @param sourceName  string ID for this reader to use in logs and stats
   * @param sourcePaths paths to the {@code .geojson} files on disk
   * @param writer      consumer for rendered features
   * @param config      user-defined parameters controlling number of threads and log interval
   * @param profile     logic that defines what map features to emit for each source feature
   * @param stats       to keep track of counters and timings
   * @throws IllegalArgumentException if a problem occurs reading the input file
   */
  public static void process(String sourceName, List<Path> sourcePaths, FeatureGroup writer, PlanetilerConfig config,
    Profile profile, Stats stats) {
    SourceFeatureProcessor.processFiles(
      sourceName,
      sourcePaths,
      path -> new GeoJsonReader(sourceName, path),
      writer, config, profile, stats
    );
  }

  @Override
  public void close() throws IOException {}

  @Override
  public synchronized long getFeatureCount() {
    if (count < 0) {
      count = file.count();
    }
    return count;
  }

  @Override
  public void readFeatures(Consumer<SimpleFeature> next) throws Exception {
    long id = 0;
    for (var feature : file) {
      next.accept(SimpleFeature.create(feature.geometry(), feature.tags(), sourceName, layer, id++));
    }
  }
}
