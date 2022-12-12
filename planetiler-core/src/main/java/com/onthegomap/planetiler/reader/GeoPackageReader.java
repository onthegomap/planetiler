package com.onthegomap.planetiler.reader;

import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.FileUtils;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

/**
 * Utility that reads {@link SourceFeature SourceFeatures} from the vector geometries contained in a GeoPackage file.
 */
public class GeoPackageReader extends SimpleReader implements Closeable {

  private final DataStore dataStore;

  GeoPackageReader(String sourceName, Path input, Profile profile, Stats stats) {
    super(profile, stats, sourceName);

    try {
      dataStore = openDataStore(input);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static File findGpkgFile(Path path) throws IOException {
    if (Files.isDirectory(path)) {
      try (var walkStream = Files.walk(path)) {
        return walkStream
          .filter(z -> FileUtils.hasExtension(z, "gpkg"))
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException("No .gpkg file found inside " + path))
          .toFile();
      }
    } else if (FileUtils.hasExtension(path, "gpkg")) {
      return path.toFile();
    } else {
      throw new IllegalArgumentException("Invalid geopackage input: " + path + " must be .gpkg");
    }
  }

  private static DataStore openDataStore(Path basePath) throws IOException {
    File path = findGpkgFile(basePath);
    return DataStoreFinder.getDataStore(Map.ofEntries(
      Map.entry("dbtype", "geopkg"),
      Map.entry("database", path.getAbsolutePath()),
      Map.entry("read-only", true)
    ));
  }

  /**
   * Renders map features for all elements from an OGC GeoPackage based on the mapping logic defined in {@code profile}.
   *
   * @param sourceName string ID for this reader to use in logs and stats
   * @param input      path to the {@code .gpkg} file on disk
   * @param writer     consumer for rendered features
   * @param config     user-defined parameters controlling number of threads and log interval
   * @param profile    logic that defines what map features to emit for each source feature
   * @param stats      to keep track of counters and timings
   * @throws IllegalArgumentException if a problem occurs reading the input file
   */
  public static void process(String sourceName, Path input, FeatureGroup writer,
    PlanetilerConfig config, Profile profile, Stats stats) {
    try (var reader = new GeoPackageReader(sourceName, input, profile, stats)) {
      reader.process(writer, config);
    }
  }

  @Override
  public long getCount() {
    try {
      long numFeatures = 0;

      for (String name : dataStore.getTypeNames()) {
        SimpleFeatureSource source = dataStore.getFeatureSource(name);
        numFeatures += source.getCount(Query.ALL);
      }
      return numFeatures;
    } catch (IOException e) {
      return 0;
    }
  }

  @Override
  public WorkerPipeline.SourceStep<SimpleFeature> read() {
    return next -> {
      long id = 0;
      CoordinateReferenceSystem lonLatCRS = CRS.decode("EPSG:4326", true);

      for (var featureName : dataStore.getTypeNames()) {
        SimpleFeatureSource source = dataStore.getFeatureSource(featureName);
        SimpleFeatureType schema = source.getSchema();
        List<AttributeDescriptor> attrDescriptors = schema.getAttributeDescriptors();
        MathTransform transform = CRS.findMathTransform(schema.getCoordinateReferenceSystem(), lonLatCRS);

        try (var featureIterator = source.getFeatures().features()) {
          while (featureIterator.hasNext()) {
            var feature = featureIterator.next();

            Geometry featureGeom = (Geometry) feature.getDefaultGeometry();
            Geometry lonLatGeom = (transform.isIdentity()) ? featureGeom : JTS.transform(featureGeom, transform);

            if (lonLatGeom == null) {
              continue;
            }

            SimpleFeature geom = SimpleFeature.create(lonLatGeom, new HashMap<>(feature.getAttributeCount()),
              sourceName, featureName, ++id);

            for (int i = 1; i < feature.getAttributeCount(); ++i) {
              geom.setTag(attrDescriptors.get(i).getLocalName(), feature.getAttribute(i));
            }

            next.accept(geom);
          }
        }
      }
    };
  }

  @Override
  public void close() {
    dataStore.dispose();
  }
}
