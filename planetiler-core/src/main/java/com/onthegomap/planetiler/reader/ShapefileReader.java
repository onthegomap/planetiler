package com.onthegomap.planetiler.reader;

import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.stats.Stats;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;
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
import org.opengis.referencing.operation.TransformException;

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

  private final FeatureCollection<SimpleFeatureType, org.opengis.feature.simple.SimpleFeature> inputSource;
  private final String[] attributeNames;
  private final ShapefileDataStore dataStore;
  private MathTransform transformToLatLon;

  public ShapefileReader(String sourceProjection, String sourceName, Path input) {
    super(sourceName);
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
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (FactoryException e) {
      throw new FileFormatException("Bad reference system", e);
    }
  }

  /**
   * Renders map features for all elements from an ESRI Shapefile based on the mapping logic defined in {@code profile}.
   * Overrides the coordinate reference system defined in the shapefile.
   *
   * @param sourceProjection code for the coordinate reference system of the input data, to be parsed by
   *                         {@link CRS#decode(String)}
   * @param sourceName       string ID for this reader to use in logs and stats
   * @param sourcePaths      paths to the {@code .shp} files on disk, or {@code .zip} files containing the shapefile
   *                         components
   * @param writer           consumer for rendered features
   * @param config           user-defined parameters controlling number of threads and log interval
   * @param profile          logic that defines what map features to emit for each source feature
   * @param stats            to keep track of counters and timings
   * @throws IllegalArgumentException if a problem occurs reading the input file
   */
  public static void processWithProjection(String sourceProjection, String sourceName, List<Path> sourcePaths,
    FeatureGroup writer, PlanetilerConfig config, Profile profile, Stats stats) {
    SourceFeatureProcessor.processFiles(
      sourceName,
      sourcePaths,
      path -> new ShapefileReader(sourceProjection, sourceName, path),
      writer, config, profile, stats
    );
  }

  private ShapefileDataStore open(Path path) {
    try {
      var store = new ShapefileDataStore(path.toUri().toURL());
      store.setTryCPGFile(true);
      return store;
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public long getFeatureCount() {
    return inputSource.size();
  }

  @Override
  public void readFeatures(Consumer<SimpleFeature> next) throws TransformException {
    long id = 0;
    try (var iter = inputSource.features()) {
      while (iter.hasNext()) {
        org.opengis.feature.simple.SimpleFeature feature = iter.next();
        Geometry source = (Geometry) feature.getDefaultGeometry();
        Geometry latLonGeometry = source;
        if (transformToLatLon != null) {
          latLonGeometry = JTS.transform(source, transformToLatLon);
        }
        if (latLonGeometry != null) {
          SimpleFeature geom = SimpleFeature.create(latLonGeometry, new HashMap<>(attributeNames.length),
            sourceName, null, ++id);
          for (int i = 1; i < attributeNames.length; i++) {
            geom.setTag(attributeNames[i], feature.getAttribute(i));
          }
          next.accept(geom);
        }
      }
    }
  }

  @Override
  public void close() {
    dataStore.dispose();
  }
}
