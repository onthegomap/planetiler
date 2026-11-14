package com.onthegomap.planetiler.reader.shapefile;

import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.config.Bounds;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.reader.FileFormatException;
import com.onthegomap.planetiler.reader.ShapefileOptions;
import com.onthegomap.planetiler.reader.SimpleReader;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.SourceFeatureProcessor;
import com.onthegomap.planetiler.stats.Stats;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.geotools.api.referencing.FactoryException;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.api.referencing.operation.MathTransform;
import org.geotools.api.referencing.operation.OperationNotFoundException;
import org.geotools.api.referencing.operation.TransformException;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility that reads {@link SourceFeature SourceFeatures} from the geometries contained in an ESRI shapefile.
 * <p>
 * Reads the {@code .shp} records and hands each one off undecoded as a {@link ShapefileFeature}, so geometries get
 * decoded and reprojected on the processing threads. Attributes come from the {@code .dbf} file (in the character set
 * the {@code .cpg} file names), and the coordinate reference system from the {@code .prj} file.
 *
 * @see <a href=
 *      "https://www.esri.com/content/dam/esrisites/sitecore-archive/Files/Pdfs/library/whitepapers/pdfs/shapefile.pdf">ESRI
 *      Shapefile Specification</a>
 */
public class ShapefileReader extends SimpleReader<ShapefileFeature> {
  // A shapefile is a set of files with the same base name:
  // - .shp: 100 byte header (ShpHeader), then records that are each an 8 byte header (ShpRecordHeader) followed by
  //   the contents (ShpRecord)
  // - .shx: 100 byte header, then an 8 byte offset and length per record (only used to count records)
  // - .dbf: one row of attributes per record (see DbfReader)
  // - .prj: optional WKT of the coordinate reference system
  // - .cpg: optional charset of the .dbf text

  private static final Logger LOGGER = LoggerFactory.getLogger(ShapefileReader.class);

  private final boolean emptyStringsAsNull;
  private final boolean lowercaseAttributeNames;
  private final String layer;
  private final Path input;
  private final FileSystem zipFileSystem;
  private final Charset charset;
  private long idOffset = 0;
  // transform from the shapefile's coordinate reference system to lat/lon, or null if it is already lat/lon
  private final MathTransform transformToLatLon;
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
    this.lowercaseAttributeNames = options.lowercaseAttributeNames();
    this.layer = input.getFileName().toString().replaceAll("(?i)\\.shp$", "");
    FileSystem reopened = null;
    try {
      // paths inside a zip file can come from a zip file system that was closed after listing its files
      if ("jar".equals(input.toUri().getScheme())) {
        var fs = input.getFileSystem();
        if (!fs.isOpen()) {
          reopened = FileSystems.newFileSystem(Path.of(fs.toString())); // NOSONAR closed when ShapefileReader is closed
          input = reopened.getPath(input.toString());
        }
      }
      this.input = input;
      this.charset = Optional.ofNullable(options.charset())
        .or(() -> Optional.ofNullable(this.readCpgCharset()))
        // ISO-8859-1 when nothing names a charset, to match legacy GeoTools behavior
        .orElse(StandardCharsets.ISO_8859_1);
      CoordinateReferenceSystem src = sourceCrs(options.projection());
      // same as CRS.decode("EPSG:4326", true) without initializing EPSG database
      CoordinateReferenceSystem dest = DefaultGeographicCRS.WGS84;
      MathTransform transform = findMathTransform(input, src, dest);
      transformToLatLon = transform.isIdentity() ? null : transform;
      Envelope env = bounds.latLon();
      if (!bounds.isWorld() && transformToLatLon == null) {
        // reprojecting the bounds would initialize the EPSG database even though the data is already in WGS84
        sourceBounds = new Envelope(env);
      } else if (!bounds.isWorld()) {
        var bbox = new ReferencedEnvelope(env.getMinX(), env.getMaxX(), env.getMinY(), env.getMaxY(), dest);
        sourceBounds = tryTransform(bbox, src);
      }
      filtersToBounds = bounds.isWorld() || sourceBounds != null;
    } catch (IOException e) {
      throw new UncheckedIOException(closeAfterFailure(reopened, e));
    } catch (FactoryException e) {
      throw new FileFormatException("Bad reference system", closeAfterFailure(reopened, e));
    } catch (RuntimeException e) {
      throw closeAfterFailure(reopened, e);
    }
    zipFileSystem = reopened;
  }

  private Envelope tryTransform(ReferencedEnvelope bbox, CoordinateReferenceSystem src) throws FactoryException {
    try {
      return new Envelope(bbox.transform(src, true));
    } catch (TransformException _) {
      // just read every record
      return null;
    }
  }

  private CoordinateReferenceSystem sourceCrs(String projection) throws IOException, FactoryException {
    String prj = readSibling("prj");
    CoordinateReferenceSystem fileCrs = null;
    if (prj != null) {
      try {
        fileCrs = CRS.parseWKT(prj);
      } catch (FactoryException e) {
        if (projection == null) {
          throw e;
        }
        // the projection argument overrides it anyway
      }
    }
    if (projection != null) {
      return GeoUtils.decodeCRS(projection, fileCrs);
    } else if (fileCrs == null) {
      LOGGER.warn("No .prj file next to {}, assuming it is in lon/lat (EPSG:4326)", input.getFileName());
      return DefaultGeographicCRS.WGS84;
    }
    return fileCrs;
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

  // Closes the zip file system the constructor reopened, so a failed reader doesn't keep the zip open (or locked, on
  // Windows).
  private static <E extends Exception> E closeAfterFailure(FileSystem reopened, E failure) {
    if (reopened != null) {
      try {
        reopened.close();
      } catch (IOException e) {
        failure.addSuppressed(e);
      }
    }
    return failure;
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

  /** Returns the component of this shapefile with {@code extension} (any case), or null if there is none. */
  private Path sibling(String extension) {
    String base = input.getFileName().toString().replaceAll("(?i)\\.shp$", "");
    for (String ext : List.of(extension, extension.toUpperCase(Locale.ROOT))) {
      Path path = input.resolveSibling(base + "." + ext);
      if (Files.exists(path)) {
        return path;
      }
    }
    return null;
  }

  private String readSibling(String extension) throws IOException {
    Path path = sibling(extension);
    // .prj and .cpg files hold ASCII text (WKT, a charset name). ISO-8859-1 maps each byte to one character, so a
    // stray non-ASCII byte can't make reading fail the way it would with UTF-8.
    return path == null ? null : Files.readString(path, StandardCharsets.ISO_8859_1).trim();
  }

  private Charset readCpgCharset() {
    String cpg = null;
    try {
      cpg = readSibling("cpg");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    if (cpg == null || cpg.isBlank()) {
      return null;
    }
    try {
      // .cpg files aren't in the shapefile spec, they're an ESRI convention. ESRI tools write either a charset name or
      // a Windows code page number, where 65001 means UTF-8, so map the numbers to Java charset names. See
      // https://support.esri.com/en-us/knowledge-base/read-and-write-shapefile-and-dbase-files-encoded-in-var-000013192
      return cpg.matches("\\d+") ? codePageCharset(cpg) : Charset.forName(cpg);
    } catch (IllegalArgumentException _) {
      LOGGER.warn("Ignoring unknown charset \"{}\" in .cpg file of {}", cpg, input.getFileName());
      return null;
    }
  }

  private static Charset codePageCharset(String number) {
    if (number.equals("65001")) {
      return StandardCharsets.UTF_8;
    }
    // Java names Windows code pages (1252, 874, 932, ...) windows-N, and only knows DOS code pages (850, 852, 866, ...)
    // as cpN. cpN doesn't work for all Windows code pages either (cp932 is an IBM variant of Shift-JIS).
    String windows = "windows-" + number;
    return Charset.isSupported(windows) ? Charset.forName(windows) : Charset.forName("cp" + number);
  }

  @Override
  public long getFeatureCount() {
    try {
      Path shx = sibling("shx");
      if (sourceBounds == null && shx != null) {
        return ShpHeader.recordsInIndexFileFromSize(Files.size(shx));
      }
      var count = new AtomicLong();
      forEachRecord(content -> {
        if (intersectsSourceBounds(content)) {
          count.incrementAndGet();
        }
      });
      return count.get();
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
    Path dbfPath = sibling("dbf");
    try (
      var dbfStream = dbfPath == null ? null : buffered(dbfPath, 1 << 20);
      var dbf = dbfStream == null ? null : new DbfReader(dbfStream, charset)
    ) {
      String[] attributeNames = dbf == null ? new String[0] : dbf.fieldNames().stream()
        .map(name -> lowercaseAttributeNames ? name.toLowerCase(Locale.ROOT) : name)
        .toArray(String[]::new);
      var id = new AtomicLong(idOffset);
      forEachRecord(content -> {
        // read the .shp and .dbf files in lockstep so that a record that can't be read doesn't shift the attributes
        // of the ones after it
        DbfReader.Row row = null;
        if (dbf != null && attributeNames.length > 0) {
          row = dbf.next();
          if (row == null) {
            throw new FileFormatException(
              "The .dbf file of " + input.getFileName() + " has fewer records than the .shp file");
          }
        }
        boolean deleted = row != null && row.deleted();
        if (!ShpRecord.hasShapeType(content)) {
          LOGGER.warn("Skipping a record of {} without a shape type", input.getFileName());
        } else if (!deleted && intersectsSourceBounds(content)) {
          next.accept(new ShapefileFeature(content, transformToLatLon, readTags(attributeNames, row), sourceName, layer,
            id.incrementAndGet()));
        }
      });
    }
  }

  private interface RecordConsumer {
    void accept(byte[] content) throws IOException;
  }

  private void forEachRecord(RecordConsumer consumer) throws IOException {
    try (var shp = buffered(input, 1 << 16)) {
      byte[] header = shp.readNBytes(ShpHeader.BYTES);
      if (!ShpHeader.isHeader(header)) {
        throw new FileFormatException("Missing .shp header in " + input.getFileName());
      }
      long fileBytes = ShpHeader.parse(header).fileBytes();
      long positionInFile = ShpHeader.BYTES;
      byte[] content;
      while (positionInFile < fileBytes && (content = readNextRecord(shp, positionInFile)) != null) {
        positionInFile += ShpRecordHeader.BYTES + content.length;
        consumer.accept(content);
      }
    }
  }

  /** Returns the contents of the next record, or {@code null} if the file ends in the middle of it. */
  @SuppressWarnings("java:S1168")
  private byte[] readNextRecord(InputStream shp, long positionInFile) throws IOException {
    byte[] recordHeader = shp.readNBytes(ShpRecordHeader.BYTES);
    if (recordHeader.length < ShpRecordHeader.BYTES) {
      warnTruncated(positionInFile + recordHeader.length);
      return null;
    }
    int contentBytes = ShpRecordHeader.parseContentLength(recordHeader);
    byte[] content = shp.readNBytes(Math.max(contentBytes, 0));
    if (content.length < contentBytes) {
      warnTruncated(positionInFile + recordHeader.length + content.length);
      return null;
    }
    return content;
  }

  private void warnTruncated(long positionInFile) {
    LOGGER.warn("Stopping at byte {} of {} since the file ends in the middle of a record", positionInFile,
      input.getFileName());
  }

  private static InputStream buffered(Path path, int size) throws IOException {
    return new BufferedInputStream(Files.newInputStream(path), size);
  }

  private boolean intersectsSourceBounds(byte[] content) {
    // check if a record intersects the bounding box without decoding the entire geometry
    Envelope recordBounds = ShpRecord.parseBounds(content);
    if (recordBounds == null) {
      // let decoding report it
      return true;
    }
    // sourceBounds is reprojected to the file's projection, so is safe to compare against bounds in the file
    return !recordBounds.isNull() && (sourceBounds == null || sourceBounds.intersects(recordBounds));
  }

  private Map<String, Object> readTags(String[] attributeNames, DbfReader.Row row) {
    Map<String, Object> tags = HashMap.newHashMap(attributeNames.length);
    if (row != null) {
      for (int i = 0; i < attributeNames.length; i++) {
        Object value = row.values()[i];
        if (!emptyStringsAsNull || !"".equals(value)) {
          tags.put(attributeNames[i], value);
        }
      }
    }
    return tags;
  }

  @Override
  public void close() {
    // the .shp and .dbf files are opened and closed by each read
    if (zipFileSystem != null) {
      try {
        zipFileSystem.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }
}
