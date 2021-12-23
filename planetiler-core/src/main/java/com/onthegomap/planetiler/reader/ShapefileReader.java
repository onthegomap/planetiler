package com.onthegomap.planetiler.reader;

import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.FileUtils;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.HashMap;
import org.geotools.data.FeatureSource;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.feature.FeatureCollection;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

/**
 * Utility that reads {@link SourceFeature SourceFeatures} from the geometries contained in an ESRI shapefile.
 * <p>
 * Shapefile processing handled by geotools {@link ShapefileDataStore}.
 *
 * @see <a href="https://www.esri.com/content/dam/esrisites/sitecore-archive/Files/Pdfs/library/whitepapers/pdfs/shapefile.pdf">ESRI
 * Shapefile Specification</a>
 */
public class ShapefileReader extends SimpleReader implements Closeable {

  private final FeatureCollection<SimpleFeatureType, org.opengis.feature.simple.SimpleFeature> inputSource;
  private final String[] attributeNames;
  private final ShapefileDataStore dataStore;
  private MathTransform transformToLatLon;

  ShapefileReader(String sourceProjection, String sourceName, Path input, Profile profile, Stats stats) {
    super(profile, stats, sourceName);
    dataStore = open(input);
    try {
      String typeName = dataStore.getTypeNames()[0];
      FeatureSource<SimpleFeatureType, org.opengis.feature.simple.SimpleFeature> source = dataStore
        .getFeatureSource(typeName);

      inputSource = source.getFeatures(Filter.INCLUDE);
      CoordinateReferenceSystem src =
        sourceProjection == null ? source.getSchema().getCoordinateReferenceSystem() : CRS.decode(sourceProjection);
      CoordinateReferenceSystem dest = CRS.decode("EPSG:4326", true);
      transformToLatLon = CRS.findMathTransform(src, dest);
      if (transformToLatLon.isIdentity()) {
        transformToLatLon = null;
      }
      attributeNames = new String[inputSource.getSchema().getAttributeCount()];
      for (int i = 0; i < attributeNames.length; i++) {
        attributeNames[i] = inputSource.getSchema().getDescriptor(i).getLocalName();
      }
    } catch (IOException | FactoryException e) {
      throw new RuntimeException(e);
    }
  }

  ShapefileReader(String name, Path input, Profile profile, Stats stats) {
    this(null, name, input, profile, stats);
  }

  /**
   * Renders map features for all elements from an ESRI Shapefile based on the mapping logic defined in {@code profile}.
   * Overrides the coordinate reference system defined in the shapefile.
   *
   * @param sourceProjection code for the coordinate reference system of the input data, to be parsed by {@link
   *                         CRS#decode(String)}
   * @param sourceName       string ID for this reader to use in logs and stats
   * @param input            path to the {@code .shp} file on disk, or a {@code .zip} file containing the shapefile
   *                         components
   * @param writer           consumer for rendered features
   * @param config           user-defined parameters controlling number of threads and log interval
   * @param profile          logic that defines what map features to emit for each source feature
   * @param stats            to keep track of counters and timings
   * @throws IllegalArgumentException if a problem occurs reading the input file
   */
  public static void processWithProjection(String sourceProjection, String sourceName, Path input, FeatureGroup writer,
    PlanetilerConfig config, Profile profile, Stats stats) {
    try (var reader = new ShapefileReader(sourceProjection, sourceName, input, profile, stats)) {
      reader.process(writer, config);
    }
  }

  /**
   * Renders map features for all elements from an ESRI Shapefile based on the mapping logic defined in {@code profile}.
   * Infers the coordinate reference system from the shapefile.
   *
   * @param sourceName string ID for this reader to use in logs and stats
   * @param input      path to the {@code .shp} file on disk, or a {@code .zip} file containing the shapefile
   *                   components
   * @param writer     consumer for rendered features
   * @param config     user-defined parameters controlling number of threads and log interval
   * @param profile    logic that defines what map features to emit for each source feature
   * @param stats      to keep track of counters and timings
   * @throws IllegalArgumentException if a problem occurs reading the input file
   */
  public static void process(String sourceName, Path input, FeatureGroup writer, PlanetilerConfig config,
    Profile profile,
    Stats stats) {
    processWithProjection(null, sourceName, input, writer, config, profile, stats);
  }

  private ShapefileDataStore open(Path path) {
    try {
      URI uri;
      if (FileUtils.hasExtension(path, "zip")) {
        try (var zipFs = FileSystems.newFileSystem(path)) {
          Path shapeFileInZip = FileUtils.walkFileSystem(zipFs)
            .filter(z -> FileUtils.hasExtension(z, "shp"))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("No .shp file found inside " + path));
          uri = shapeFileInZip.toUri();
        }
      } else if (FileUtils.hasExtension(path, "shp")) {
        uri = path.toUri();
      } else {
        throw new IllegalArgumentException("Invalid shapefile input: " + path + " must be zip or shp");
      }
      return new ShapefileDataStore(uri.toURL());
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public long getCount() {
    return inputSource.size();
  }

  @Override
  public WorkerPipeline.SourceStep<SimpleFeature> read() {
    return next -> {
      try (var iter = inputSource.features()) {
        long id = 0;
        while (iter.hasNext()) {
          id++;
          org.opengis.feature.simple.SimpleFeature feature = iter.next();
          Geometry source = (Geometry) feature.getDefaultGeometry();
          Geometry latLonGeometry = source;
          if (transformToLatLon != null) {
            latLonGeometry = JTS.transform(source, transformToLatLon);
          }
          if (latLonGeometry != null) {
            SimpleFeature geom = SimpleFeature.create(latLonGeometry, new HashMap<>(attributeNames.length), sourceName,
              null, id);
            for (int i = 1; i < attributeNames.length; i++) {
              geom.setTag(attributeNames[i], feature.getAttribute(i));
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
