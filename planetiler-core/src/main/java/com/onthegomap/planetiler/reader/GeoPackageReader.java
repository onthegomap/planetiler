package com.onthegomap.planetiler.reader;

import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.stats.Stats;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;
import mil.nga.geopackage.GeoPackage;
import mil.nga.geopackage.GeoPackageManager;
import mil.nga.geopackage.features.user.FeatureDao;
import mil.nga.geopackage.geom.GeoPackageGeometryData;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.WKBReader;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

/**
 * Utility that reads {@link SourceFeature SourceFeatures} from the vector geometries contained in a GeoPackage file.
 */
public class GeoPackageReader extends SimpleReader<SimpleFeature> {

  private final GeoPackage geoPackage;

  GeoPackageReader(String sourceName, Path input) {
    super(sourceName);

    geoPackage = GeoPackageManager.open(false, input.toFile());
  }

  /**
   * Renders map features for all elements from an OGC GeoPackage based on the mapping logic defined in {@code
   * profile}.
   *
   * @param sourceName  string ID for this reader to use in logs and stats
   * @param sourcePaths paths to the {@code .gpkg} files on disk
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
      path -> new GeoPackageReader(sourceName, path),
      writer, config, profile, stats
    );
  }

  @Override
  public long getFeatureCount() {
    long numFeatures = 0;

    for (String name : geoPackage.getFeatureTables()) {
      FeatureDao features = geoPackage.getFeatureDao(name);
      numFeatures += features.count();
    }
    return numFeatures;
  }

  @Override
  public void readFeatures(Consumer<SimpleFeature> next) throws Exception {
    CoordinateReferenceSystem lonLatCRS = CRS.decode("EPSG:4326");
    long id = 0;

    for (var featureName : geoPackage.getFeatureTables()) {
      FeatureDao features = geoPackage.getFeatureDao(featureName);

      MathTransform transform = CRS.findMathTransform(
        CRS.decode("EPSG:" + features.getSrsId()),
        lonLatCRS);

      for (var feature : features.queryForAll()) {
        GeoPackageGeometryData geometryData = feature.getGeometry();
        if (geometryData == null) {
          continue;
        }

        Geometry featureGeom = (new WKBReader()).read(geometryData.getWkb());
        Geometry lonLatGeom = (transform.isIdentity()) ? featureGeom : JTS.transform(featureGeom, transform);

        SimpleFeature geom = SimpleFeature.create(lonLatGeom, new HashMap<>(),
          sourceName, featureName, ++id);

        var columns = feature.getColumns();
        for (int i = 0; i < columns.columnCount(); ++i) {
          if (i != columns.getGeometryIndex()) {
            geom.setTag(columns.getColumnName(i), feature.getValue(i));
          }
        }

        next.accept(geom);
      }
    }
  }

  @Override
  public void close() {
    geoPackage.close();
  }
}
