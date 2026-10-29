package com.onthegomap.planetiler.reader;

import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.config.Bounds;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.stats.Stats;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;
import org.geotools.api.data.FeatureSource;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.referencing.FactoryException;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.api.referencing.operation.MathTransform;
import org.geotools.api.referencing.operation.OperationNotFoundException;
import org.geotools.api.referencing.operation.TransformException;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.dbf.DbaseFileReader;
import org.geotools.data.shapefile.files.ShpFileType;
import org.geotools.data.shapefile.files.ShpFiles;
import org.geotools.data.shapefile.shp.IndexFile;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOGGER = LoggerFactory.getLogger(ShapefileReader.class);

  private final String[] attributeNames;
  private final ShapefileDataStore dataStore;
  private final ShpFiles shpFiles;
  private final String layer;
  private final Path input;
  private MathTransform transformToLatLon;
  // bounds in the shapefile's coordinate reference system, or null to read every record
  private Envelope sourceBounds = null;

  public ShapefileReader(String sourceProjection, String sourceName, Path input) {
    this(sourceProjection, sourceName, input, Bounds.WORLD);
  }

  public ShapefileReader(String sourceProjection, String sourceName, Path input, Bounds bounds) {
    super(sourceName);
    this.layer = input.getFileName().toString().replaceAll("\\.shp$", "");
    this.input = input;
    dataStore = open(input);
    try {
      shpFiles = new ShpFiles(input.toUri().toURL());
      String typeName = dataStore.getTypeNames()[0];
      FeatureSource<SimpleFeatureType, org.geotools.api.feature.simple.SimpleFeature> source = dataStore
        .getFeatureSource(typeName);
      CoordinateReferenceSystem sourceCrs = source.getSchema().getCoordinateReferenceSystem();
      CoordinateReferenceSystem src =
        sourceProjection == null ? sourceCrs : GeoUtils.decodeCRS(sourceProjection, sourceCrs);
      CoordinateReferenceSystem dest = CRS.decode("EPSG:4326", true);
      transformToLatLon = findMathTransform(input, src, dest);
      if (transformToLatLon.isIdentity()) {
        transformToLatLon = null;
      }

      var schema = source.getSchema();
      Envelope env = bounds.latLon();
      if (!bounds.isWorld()) {
        var bbox = new ReferencedEnvelope(env.getMinX(), env.getMaxX(), env.getMinY(), env.getMaxY(), dest);
        try {
          sourceBounds =
            new Envelope(bbox.transform(schema.getGeometryDescriptor().getCoordinateReferenceSystem(), true));
        } catch (TransformException _) {
          // just read every record
        }
      }

      attributeNames = new String[schema.getAttributeCount()];
      for (int i = 0; i < attributeNames.length; i++) {
        attributeNames[i] = schema.getDescriptor(i).getLocalName();
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (FactoryException e) {
      throw new FileFormatException("Bad reference system", e);
    }
  }

  private static MathTransform findMathTransform(Path input, CoordinateReferenceSystem src,
    CoordinateReferenceSystem dest) throws FactoryException {
    try {
      return CRS.findMathTransform(src, dest);
    } catch (OperationNotFoundException e) {
      var result = CRS.findMathTransform(src, dest, true);
      LOGGER.warn(
        "Failed to parse projection from {} (\"{}\") using lenient mode instead which may result in data inconsistencies",
        input.getFileName(), e.getMessage());
      return result;
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
      path -> new ShapefileReader(sourceProjection, sourceName, path, config.bounds()),
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
    // only reads record headers: counting through ShapefileDataStore with a bounds filter decodes every geometry,
    // which is slow and fails on bad records
    try {
      if (sourceBounds == null && (!shpFiles.isLocal() || shpFiles.exists(ShpFileType.SHX))) {
        try (var shx = new IndexFile(shpFiles, false)) {
          return shx.getRecordCount();
        } catch (IOException _) {
          // no usable .shx, count the .shp records instead
        }
      }
      long count = 0;
      try (var shp = openShp()) {
        while (shp.hasNext()) {
          var nextRecord = shp.nextRecord();
          if (sourceBounds == null || sourceBounds.intersects(nextRecord.envelope())) {
            count++;
          }
        }
      }
      return count;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void readFeatures(Consumer<SimpleFeature> next) throws IOException, TransformException {
    boolean hasAttributes = attributeNames.length > 1;
    long id = 0;
    try (
      var shp = openShp();
      var dbf = hasAttributes ?
        new DbaseFileReader(shpFiles, false, dataStore.getCharset(), dataStore.getTimeZone()) : null
    ) {
      while (shp.hasNext()) {
        // read the .shp and .dbf files in lockstep so if a single feature fails, it advances both to avoid reading
        // mismatched geometries and attributes
        var nextRecord = shp.nextRecord();
        var row = dbf == null ? null : dbf.readRow();
        boolean deleted = row != null && row.isDeleted();
        if (!deleted && (sourceBounds == null || sourceBounds.intersects(nextRecord.envelope()))) {
          Geometry source = readGeometry(nextRecord);
          if (source != null) {
            next.accept(toFeature(source, row, ++id));
          }
        }
      }
    }
  }

  private Geometry readGeometry(org.geotools.data.shapefile.shp.ShapefileReader.Record shpRecord) {
    try {
      return (Geometry) shpRecord.shape();
    } catch (RuntimeException e) {
      LOGGER.warn("Skipping record {} of {} with a geometry that can't be read: {}", shpRecord.number,
        input.getFileName(), e.toString());
      return null;
    }
  }

  private SimpleFeature toFeature(Geometry source, DbaseFileReader.Row row, long id)
    throws IOException, TransformException {
    Geometry latLonGeometry = transformToLatLon == null ? source : JTS.transform(source, transformToLatLon);
    SimpleFeature feature = SimpleFeature.create(latLonGeometry, HashMap.newHashMap(attributeNames.length),
      sourceName, layer, id);
    if (row != null) {
      for (int i = 1; i < attributeNames.length; i++) {
        // the schema lists the geometry first, then the .dbf columns in order
        feature.setTag(attributeNames[i], row.read(i - 1));
      }
    }
    return feature;
  }

  private org.geotools.data.shapefile.shp.ShapefileReader openShp() throws IOException {
    return new org.geotools.data.shapefile.shp.ShapefileReader(shpFiles, true, false, new GeometryFactory());
  }

  @Override
  public void close() {
    dataStore.dispose();
  }
}
