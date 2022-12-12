package com.onthegomap.planetiler.reader;

import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.FileUtils;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.feature.FeatureCollection;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/**
 * Utility that reads {@link SourceFeature SourceFeatures} from the geometries contained in an ESRI shapefile.
 * <p>
 * Shapefile processing handled by geotools {@link ShapefileDataStore}.
 *
 * @see <a href=
 *      "https://www.esri.com/content/dam/esrisites/sitecore-archive/Files/Pdfs/library/whitepapers/pdfs/shapefile.pdf">ESRI
 *      Shapefile Specification</a>
 */
public class ShapefileReader extends SimpleReader<SimpleFeature> {

  private final CoordinateReferenceSystem sourceProjection;

  ShapefileReader(String sourceProjection, String sourceName, List<Path> sourcePaths, Profile profile, Stats stats) {
    super(profile, stats, sourceName, sourcePaths);
    try {
      this.sourceProjection = sourceProjection == null ? null : CRS.decode(sourceProjection);
    } catch (FactoryException e) {
      throw new IllegalArgumentException("Bad reference system", e);
    }
  }

  ShapefileReader(String name, List<Path> input, Profile profile, Stats stats) {
    this(null, name, input, profile, stats);
  }

  /**
   * Renders map features for all elements from an ESRI Shapefile based on the mapping logic defined in {@code profile}.
   * Overrides the coordinate reference system defined in the shapefile.
   *
   * @param sourceProjection code for the coordinate reference system of the input data, to be parsed by
   *                         {@link CRS#decode(String)}
   * @param sourceName       string ID for this reader to use in logs and stats
   * @param sourcePaths      list of paths to the {@code .shp} file on disk, or a {@code .zip} file containing the
   *                         shapefile components
   * @param writer           consumer for rendered features
   * @param config           user-defined parameters controlling number of threads and log interval
   * @param profile          logic that defines what map features to emit for each source feature
   * @param stats            to keep track of counters and timings
   * @throws IllegalArgumentException if a problem occurs reading the input file
   */
  public static void processWithProjection(String sourceProjection, String sourceName, List<Path> sourcePaths,
    FeatureGroup writer, PlanetilerConfig config, Profile profile, Stats stats) {

    var reader = new ShapefileReader(sourceProjection, sourceName, sourcePaths, profile, stats);
    reader.process(writer, config);
  }

  /**
   * Renders map features for all elements from an ESRI Shapefile based on the mapping logic defined in {@code profile}.
   * Infers the coordinate reference system from the shapefile.
   *
   * @param sourceName  string ID for this reader to use in logs and stats
   * @param sourcePaths list of paths to the {@code .shp} file on disk, or a {@code .zip} file containing the shapefile
   *                    components
   * @param writer      consumer for rendered features
   * @param config      user-defined parameters controlling number of threads and log interval
   * @param profile     logic that defines what map features to emit for each source feature
   * @param stats       to keep track of counters and timings
   * @throws IllegalArgumentException if a problem occurs reading the input file
   */
  public static void process(String sourceName, List<Path> sourcePaths, FeatureGroup writer, PlanetilerConfig config,
    Profile profile, Stats stats) {
    processWithProjection(null, sourceName, sourcePaths, writer, config, profile, stats);
  }

  @Override
  public long getCountForPath(Path path) {
    try (var meta = ShapefileData.fromPath(path)) {
      return meta.inputSource.size();
    }
  }

  @Override
  public void readPath(Path path, Consumer<SimpleFeature> next) throws Exception {
    try (
      var meta = ShapefileData.fromPath(path);
      var iter = meta.inputSource.features()
    ) {
      var schema = meta.inputSource.getSchema();
      var attributeNames = new String[schema.getAttributeCount()];
      for (int i = 0; i < attributeNames.length; i++) {
        attributeNames[i] = schema.getDescriptor(i).getLocalName();
      }

      CoordinateReferenceSystem src =
        sourceProjection == null ? schema.getCoordinateReferenceSystem() : sourceProjection;
      CoordinateReferenceSystem dest = CRS.decode("EPSG:4326", true);

      var transformToLatLon = CRS.findMathTransform(src, dest);
      if (transformToLatLon.isIdentity()) {
        transformToLatLon = null;
      }

      while (iter.hasNext()) {
        org.opengis.feature.simple.SimpleFeature feature = iter.next();
        Geometry source = (Geometry) feature.getDefaultGeometry();
        Geometry latLonGeometry = source;
        if (transformToLatLon != null) {
          latLonGeometry = JTS.transform(source, transformToLatLon);
        }
        if (latLonGeometry != null) {
          SimpleFeature geom = SimpleFeature.create(latLonGeometry, new HashMap<>(attributeNames.length),
            sourceName, null, id.incrementAndGet());
          for (int i = 1; i < attributeNames.length; i++) {
            geom.setTag(attributeNames[i], feature.getAttribute(i));
          }
          next.accept(geom);
        }
      }
    }
  }

  private record ShapefileData(
    ShapefileDataStore dataStore,
    FeatureCollection<SimpleFeatureType, org.opengis.feature.simple.SimpleFeature> inputSource
  ) implements Closeable {

    static ShapefileData fromPath(Path path) {
      var dataStore = openDataStore(path);

      try {
        String typeName = dataStore.getTypeNames()[0];

        var featureSource = dataStore.getFeatureSource(typeName);
        var inputSource = featureSource.getFeatures(Filter.INCLUDE);
        return new ShapefileData(dataStore, inputSource);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public void close() {
      this.dataStore.dispose();
    }

    private static URI findShpFile(Path path, Stream<Path> walkStream) {
      return walkStream
        .filter(z -> FileUtils.hasExtension(z, "shp"))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("No .shp file found inside " + path))
        .toUri();
    }

    private static ShapefileDataStore openDataStore(Path path) {
      try {
        URI uri;
        if (Files.isDirectory(path)) {
          try (var walkStream = Files.walk(path)) {
            uri = findShpFile(path, walkStream);
          }
        } else if (FileUtils.hasExtension(path, "zip")) {
          try (
            var zipFs = FileSystems.newFileSystem(path);
            var walkStream = FileUtils.walkFileSystem(zipFs)
          ) {
            uri = findShpFile(path, walkStream);
          }
        } else if (FileUtils.hasExtension(path, "shp")) {
          uri = path.toUri();
        } else {
          throw new IllegalArgumentException("Invalid shapefile input: " + path + " must be zip or shp");
        }
        var store = new ShapefileDataStore(uri.toURL());
        store.setTryCPGFile(true);
        return store;
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }
}
