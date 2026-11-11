package com.onthegomap.planetiler.reader;

import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.config.Bounds;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.FileUtils;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
import org.geotools.data.shapefile.shp.ShapeType;
import org.geotools.data.shapefile.shp.ShapefileHeader;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility that reads {@link SourceFeature SourceFeatures} from the geometries contained in an ESRI shapefile.
 * <p>
 * Reads the {@code .shp} records itself and hands each one off undecoded as a {@link ShapefileFeature}, so geometries
 * get decoded and reprojected on the processing threads. Attributes, the coordinate reference system and the character
 * set come from geotools {@link ShapefileDataStore}.
 *
 * @see <a href=
 *      "https://www.esri.com/content/dam/esrisites/sitecore-archive/Files/Pdfs/library/whitepapers/pdfs/shapefile.pdf">ESRI
 *      Shapefile Specification</a>
 */
public class ShapefileReader extends SimpleReader<ShapefileFeature> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ShapefileReader.class);

  private final String[] attributeNames;
  private final boolean emptyStringsAsNull;
  private final ShapefileDataStore dataStore;
  private final ShpFiles shpFiles;
  private final String layer;
  private final Path input;
  private long idOffset = 0;
  private MathTransform transformToLatLon;
  // bounds in the shapefile's coordinate reference system, or null to read every record
  private Envelope sourceBounds = null;
  private final boolean filtersToBounds;

  public ShapefileReader(String sourceProjection, String sourceName, Path input) {
    this(sourceProjection, sourceName, input, Bounds.WORLD);
  }

  public ShapefileReader(String sourceProjection, String sourceName, Path input, Bounds bounds) {
    this(sourceName, input, bounds, ShapefileOptions.builder().projection(sourceProjection).build());
  }

  public ShapefileReader(String sourceName, Path input, Bounds bounds, ShapefileOptions options) {
    super(sourceName);
    this.emptyStringsAsNull = options.emptyStringsAsNull();
    this.layer = input.getFileName().toString().replaceAll("\\.shp$", "");
    this.input = input;
    dataStore = open(input);
    try {
      // input can be a path inside a zip file, whose jar: URL the JVM never closes, so convert it to a URL that can be
      // closed
      shpFiles = new ShpFiles(FileUtils.toUncachedUrl(input));
      String typeName = dataStore.getTypeNames()[0];
      FeatureSource<SimpleFeatureType, org.geotools.api.feature.simple.SimpleFeature> source = dataStore
        .getFeatureSource(typeName);
      CoordinateReferenceSystem sourceCrs = source.getSchema().getCoordinateReferenceSystem();
      CoordinateReferenceSystem src =
        options.projection() == null ? sourceCrs : GeoUtils.decodeCRS(options.projection(), sourceCrs);
      // same as CRS.decode("EPSG:4326", true) without initializing EPSG database
      CoordinateReferenceSystem dest = DefaultGeographicCRS.WGS84;
      transformToLatLon = findMathTransform(input, src, dest);
      if (transformToLatLon.isIdentity()) {
        transformToLatLon = null;
      }

      var schema = source.getSchema();
      Envelope env = bounds.latLon();
      if (!bounds.isWorld() && transformToLatLon == null) {
        // reprojecting the bounds would initialize the EPSG database even though the data is already in WGS84
        sourceBounds = new Envelope(env);
      } else if (!bounds.isWorld()) {
        var bbox = new ReferencedEnvelope(env.getMinX(), env.getMaxX(), env.getMinY(), env.getMaxY(), dest);
        try {
          sourceBounds =
            new Envelope(bbox.transform(schema.getGeometryDescriptor().getCoordinateReferenceSystem(), true));
        } catch (TransformException _) {
          // just read every record
        }
      }
      filtersToBounds = bounds.isWorld() || sourceBounds != null;

      attributeNames = new String[schema.getAttributeCount()];
      for (int i = 0; i < attributeNames.length; i++) {
        String name = schema.getDescriptor(i).getLocalName();
        attributeNames[i] = options.lowercaseAttributeNames() ? name.toLowerCase(Locale.ROOT) : name;
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
    process(sourceName, sourcePaths, ShapefileOptions.builder().projection(sourceProjection).build(), writer, config,
      profile, stats);
  }

  /**
   * Renders map features for all elements from a list of Shapefiles based on the mapping logic defined in
   * {@code profile}.
   *
   * @param sourceName  string ID for this reader to use in logs and stats
   * @param sourcePaths paths to the {@code .shp} files on disk, or inside {@code .zip} files
   * @param options     reader options
   * @param writer      consumer for rendered features
   * @param config      user-defined parameters controlling number of threads and log interval
   * @param profile     logic that defines what map features to emit for each source feature
   * @param stats       to keep track of counters and timings
   * @throws IllegalArgumentException if a problem occurs reading the input file
   */
  public static void process(String sourceName, List<Path> sourcePaths, ShapefileOptions options, FeatureGroup writer,
    PlanetilerConfig config, Profile profile, Stats stats) {
    SourceFeatureProcessor.processFiles(
      sourceName,
      sourcePaths,
      path -> new ShapefileReader(sourceName, path, config.bounds(), options),
      writer, config, profile, stats
    );
  }

  private ShapefileDataStore open(Path path) {
    try {
      // input can be a path inside a zip file, whose jar: URL the JVM never closes, so convert it to a URL that can be
      // closed
      var store = new ShapefileDataStore(FileUtils.toUncachedUrl(path));
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
          // compares the bounds without deserializing the entire geometry
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
  public void setFeatureIdOffset(long idOffset) {
    this.idOffset = idOffset;
  }

  @Override
  public boolean filtersToBounds() {
    return filtersToBounds;
  }

  @Override
  public void readFeatures(Consumer<ShapefileFeature> next) throws IOException {
    long id = 0;
    try (
      var shp = new BufferedInputStream(FileUtils.toUncachedUrl(input).openStream(), 1 << 16);
      var dbf = openDbf()
    ) {
      // From spec "Organization of the Main File": a 100 byte header, then each record is the record number and
      // content length (big endian ints), then the content (little endian, starting with the shape type). Lengths in
      // the file are in 16-bit words.
      var header = new ShapefileHeader();
      header.read(ByteBuffer.wrap(shp.readNBytes(100)), false);
      long fileBytes = 2L * Integer.toUnsignedLong(header.getFileLength());
      long positionInFile = 100;
      while (positionInFile < fileBytes) {
        var recordHeader = ByteBuffer.wrap(shp.readNBytes(8));
        int contentBytes = recordHeader.limit() < 8 ? 0 : 2 * recordHeader.getInt(4);
        byte[] content = shp.readNBytes(Math.max(contentBytes, 0));
        positionInFile += recordHeader.limit() + content.length;
        if (recordHeader.limit() < 8 || content.length < contentBytes) {
          LOGGER.warn("Stopping at byte {} of {} since the file ends in the middle of a record", positionInFile,
            input.getFileName());
          break;
        }
        // read the .shp and .dbf files in lockstep so that a record that can't be read doesn't shift the attributes
        // of the ones after it
        var row = dbf == null ? null : dbf.readRow();
        boolean deleted = row != null && row.isDeleted();
        if (content.length < 4) {
          LOGGER.warn("Skipping record {} of {} without a shape type", recordHeader.getInt(0), input.getFileName());
        } else if (!deleted && intersectsSourceBounds(content)) {
          next.accept(
            new ShapefileFeature(content, header.getShapeType(), transformToLatLon, readTags(row), sourceName, layer,
              idOffset + (++id)));
        }
      }
    }
  }

  private DbaseFileReader openDbf() throws IOException {
    if (attributeNames.length <= 1) {
      return null;
    }
    // Use a bigger buffer than default when reading to reduce syscalls while reading the file
    var channel = shpFiles.getReadChannel(ShpFileType.DBF, () -> sourceName);
    var buffered = Channels.newChannel(new BufferedInputStream(Channels.newInputStream(channel), 1 << 20)); // NOSONAR
    return new DbaseFileReader(buffered, false, dataStore.getCharset(), dataStore.getTimeZone());
  }

  private boolean intersectsSourceBounds(byte[] content) {
    // check if a record intersects the bounding box without decoding the entire geometry
    var buffer = ByteBuffer.wrap(content).order(ByteOrder.LITTLE_ENDIAN);
    var type = ShapeType.forID(buffer.getInt(0));
    if (type == ShapeType.NULL) {
      return false;
    } else if (sourceBounds == null) {
      return true;
    }
    boolean point = type.isPointType();
    if (content.length < (point ? 20 : 36)) {
      // the geometry can't be read either, so let decoding report it
      return true;
    }
    // sourceBounds is reprojected to the file's projection, so is safe to compare against bbox in the file
    double minX = buffer.getDouble(4);
    double minY = buffer.getDouble(12);
    double maxX = point ? minX : buffer.getDouble(20);
    double maxY = point ? minY : buffer.getDouble(28);
    return sourceBounds.intersects(new Envelope(minX, maxX, minY, maxY));
  }

  private Map<String, Object> readTags(DbaseFileReader.Row row) throws IOException {
    Map<String, Object> tags = HashMap.newHashMap(attributeNames.length);
    if (row != null) {
      for (int i = 1; i < attributeNames.length; i++) {
        // the schema lists the geometry first, then the .dbf columns in order
        Object value = row.read(i - 1);
        if (!emptyStringsAsNull || !"".equals(value)) {
          tags.put(attributeNames[i], value);
        }
      }
    }
    return tags;
  }

  private org.geotools.data.shapefile.shp.ShapefileReader openShp() throws IOException {
    var reader = new org.geotools.data.shapefile.shp.ShapefileReader(shpFiles, true, false, GeoUtils.JTS_FACTORY);
    if (reader.getHeader().getShapeType() == ShapeType.POLYGON) {
      reader.setHandler(new ShapefilePolygonHandler());
    }
    return reader;
  }

  @Override
  public void close() {
    dataStore.dispose();
  }
}
