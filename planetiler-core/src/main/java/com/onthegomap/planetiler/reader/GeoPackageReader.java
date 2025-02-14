package com.onthegomap.planetiler.reader;

import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.config.Bounds;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.FileUtils;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;
import mil.nga.geopackage.BoundingBox;
import mil.nga.geopackage.GeoPackage;
import mil.nga.geopackage.GeoPackageManager;
import mil.nga.geopackage.features.index.FeatureIndexManager;
import mil.nga.geopackage.features.index.FeatureIndexType;
import mil.nga.geopackage.features.user.FeatureColumns;
import mil.nga.geopackage.features.user.FeatureDao;
import mil.nga.geopackage.features.user.FeatureRow;
import mil.nga.geopackage.geom.GeoPackageGeometryData;
import org.geotools.api.referencing.FactoryException;
import org.geotools.api.referencing.operation.MathTransform;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.WKBReader;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility that reads {@link SourceFeature SourceFeatures} from the vector geometries contained in a GeoPackage file.
 */
public class GeoPackageReader extends SimpleReader<SimpleFeature> {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoPackageReader.class);

  private final boolean keepUnzipped;
  private Path extractedPath = null;
  private final GeoPackage geoPackage;
  private final MathTransform coordinateTransform;

  private final Bounds bounds;

  GeoPackageReader(String sourceProjection, String sourceName, Path input, Path tmpDir, boolean keepUnzipped,
    Bounds bounds) {

    super(sourceName);
    this.keepUnzipped = keepUnzipped;
    this.bounds = bounds;

    if (sourceProjection != null) {
      try {
        var sourceCRS = CRS.decode(sourceProjection);
        var latLonCRS = CRS.decode("EPSG:4326");
        coordinateTransform = CRS.findMathTransform(sourceCRS, latLonCRS);
      } catch (FactoryException e) {
        throw new FileFormatException("Bad reference system", e);
      }
    } else {
      coordinateTransform = null;
    }

    try {
      geoPackage = openGeopackage(input, tmpDir);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Create a {@link GeoPackageManager} for the given path. If {@code input} refers to a file within a ZIP archive,
   * first extract it.
   */
  private GeoPackage openGeopackage(Path input, Path unzippedDir) throws IOException {
    var inputUri = input.toUri();
    if ("jar".equals(inputUri.getScheme())) {
      extractedPath = keepUnzipped ? unzippedDir.resolve(URLEncoder.encode(input.toString(), StandardCharsets.UTF_8)) :
        Files.createTempFile(unzippedDir, "", ".gpkg");
      FileUtils.createParentDirectories(extractedPath);
      if (!keepUnzipped || FileUtils.isNewer(input, extractedPath)) {
        try (var inputStream = inputUri.toURL().openStream()) {
          FileUtils.safeCopy(inputStream, extractedPath);
        }
      }
      return GeoPackageManager.open(false, extractedPath.toFile());
    }

    return GeoPackageManager.open(false, input.toFile());
  }


  /**
   * Renders map features for all elements from an OGC GeoPackage based on the mapping logic defined in {@code
   * profile}.
   *
   * @param sourceProjection code for the coordinate reference system of the input data, to be parsed by
   *                         {@link CRS#decode(String)}
   * @param sourceName       string ID for this reader to use in logs and stats
   * @param sourcePaths      paths to the {@code .gpkg} files on disk
   * @param tmpDir           path to temporary directory for extracting data from zip files
   * @param writer           consumer for rendered features
   * @param config           user-defined parameters controlling number of threads and log interval
   * @param profile          logic that defines what map features to emit for each source feature
   * @param stats            to keep track of counters and timings
   * @param keepUnzipped     to keep unzipped files around after running (speeds up subsequent runs, but uses more disk)
   * @throws IllegalArgumentException if a problem occurs reading the input file
   */
  public static void process(String sourceProjection, String sourceName, List<Path> sourcePaths, Path tmpDir,
    FeatureGroup writer, PlanetilerConfig config, Profile profile, Stats stats, boolean keepUnzipped) {
    SourceFeatureProcessor.processFiles(
      sourceName,
      sourcePaths,
      path -> new GeoPackageReader(sourceProjection, sourceName, path, tmpDir, keepUnzipped, config.bounds()),
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
    var latLonCRS = CRS.decode("EPSG:4326");
    long id = 0;
    boolean loggedMissingGeometry = false;

    for (var featureName : geoPackage.getFeatureTables()) {
      FeatureDao features = geoPackage.getFeatureDao(featureName);

      // GeoPackage spec allows this to be 0 (undefined geographic CRS) or
      // -1 (undefined cartesian CRS). Both cases will throw when trying to
      // call CRS.decode
      long srsId = features.getSrsId();

      MathTransform transform = (coordinateTransform != null) ? coordinateTransform :
        CRS.findMathTransform(CRS.decode("EPSG:" + srsId), latLonCRS);

      FeatureIndexManager indexer = new FeatureIndexManager(geoPackage,
        features);

      Iterable<FeatureRow> results;

      if (this.bounds != null && indexer.isIndexed() && srsId == 4326) {
        var l = this.bounds.latLon();
        indexer.setIndexLocation(FeatureIndexType.RTREE);
        var boundingBox = new BoundingBox(l.getMinX(), l.getMinY(), l.getMaxX(), l.getMaxY());
        results = indexer.query(boundingBox);
      } else {
        results = features.queryForAll();
      }

      for (FeatureRow feature : results) {
        GeoPackageGeometryData geometryData = feature.getGeometry();
        byte[] wkb;
        if (geometryData == null || (wkb = geometryData.getWkb()).length == 0) {
          if (!loggedMissingGeometry) {
            loggedMissingGeometry = true;
            LOGGER.warn("Geopackage file contains empty geometry: {}", geoPackage.getPath());
          }
          continue;
        }

        Geometry featureGeom = (new WKBReader()).read(wkb);
        Geometry latLonGeom = (transform.isIdentity()) ? featureGeom : JTS.transform(featureGeom, transform);

        FeatureColumns columns = feature.getColumns();
        SimpleFeature geom = SimpleFeature.create(latLonGeom, HashMap.newHashMap(columns.columnCount()),
          sourceName, featureName, ++id);

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
  public void close() throws IOException {
    geoPackage.close();

    if (!keepUnzipped && extractedPath != null) {
      FileUtils.delete(extractedPath);
    }
  }
}
