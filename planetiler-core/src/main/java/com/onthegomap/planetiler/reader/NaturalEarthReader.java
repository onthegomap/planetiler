package com.onthegomap.planetiler.reader;

import static com.onthegomap.planetiler.util.Exceptions.throwFatalException;

import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.FileUtils;
import com.onthegomap.planetiler.util.LogUtil;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility that reads {@link SourceFeature SourceFeatures} from the geometries contained in a Natural Earth sqlite
 * distribution.
 *
 * @see <a href="https://www.naturalearthdata.com/">Natural Earth</a>
 */
public class NaturalEarthReader extends SimpleReader<SimpleFeature> {

  private static final Pattern VALID_TABLE_NAME = Pattern.compile("ne_[a-z0-9_]+", Pattern.CASE_INSENSITIVE);
  private static final Logger LOGGER = LoggerFactory.getLogger(NaturalEarthReader.class);

  private final Connection conn;
  private final boolean keepUnzipped;
  private Path extracted;

  static {
    // make sure sqlite driver loaded
    try {
      Class.forName("org.sqlite.JDBC");
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("sqlite JDBC driver not found");
    }
  }

  NaturalEarthReader(String sourceName, Path input, Path tmpDir, boolean keepUnzipped) {
    super(sourceName);
    this.keepUnzipped = keepUnzipped;

    LogUtil.setStage(sourceName);
    try {
      conn = open(input, tmpDir);
    } catch (IOException | SQLException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Renders map features for all elements from a Natural Earth sqlite file, or zip file containing a sqlite file, based
   * on the mapping logic defined in {@code profile}.
   *
   * @param sourceName   string ID for this reader to use in logs and stats
   * @param sourcePath   path to the sqlite or zip file
   * @param tmpDir       directory to extract the sqlite file into (if input is a zip file).
   * @param writer       consumer for rendered features
   * @param config       user-defined parameters controlling number of threads and log interval
   * @param profile      logic that defines what map features to emit for each source feature
   * @param stats        to keep track of counters and timings
   * @param keepUnzipped to keep unzipped files around after running (speeds up subsequent runs, but uses more disk)
   * @throws IllegalArgumentException if a problem occurs reading the input file
   */
  public static void process(String sourceName, Path sourcePath, Path tmpDir, FeatureGroup writer,
    PlanetilerConfig config, Profile profile, Stats stats, boolean keepUnzipped) {
    SourceFeatureProcessor.processFiles(
      sourceName,
      List.of(sourcePath),
      path -> new NaturalEarthReader(sourceName, path, tmpDir, keepUnzipped),
      writer, config, profile, stats
    );
  }

  /** Returns a JDBC connection to the sqlite file. Input can be the sqlite file itself or a zip file containing it. */
  private Connection open(Path path, Path unzippedDir) throws IOException, SQLException {
    String uri = "jdbc:sqlite:" + path.toAbsolutePath();
    if (FileUtils.hasExtension(path, "zip")) {
      try (var zipFs = FileSystems.newFileSystem(path)) {
        var zipEntry = FileUtils.walkFileSystem(zipFs)
          .filter(Files::isRegularFile)
          .filter(entry -> FileUtils.hasExtension(entry, "sqlite"))
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException("No .sqlite file found inside " + path));
        extracted = unzippedDir.resolve(URLEncoder.encode(zipEntry.toString(), StandardCharsets.UTF_8));
        FileUtils.createParentDirectories(extracted);
        if (!keepUnzipped || FileUtils.isNewer(path, extracted)) {
          LOGGER.info("unzipping {} to {}", path.toAbsolutePath(), extracted);
          Files.copy(Files.newInputStream(zipEntry), extracted, StandardCopyOption.REPLACE_EXISTING);
        }
        if (!keepUnzipped) {
          extracted.toFile().deleteOnExit();
        }
      }
      uri = "jdbc:sqlite:" + extracted.toAbsolutePath();
    }
    return DriverManager.getConnection(uri);
  }

  private List<String> tableNames() {
    List<String> result = new ArrayList<>();
    try (ResultSet rs = conn.getMetaData().getTables(null, null, null, null)) {
      while (rs.next()) {
        String table = rs.getString("TABLE_NAME");
        if (VALID_TABLE_NAME.matcher(table).matches()) {
          result.add(table);
        }
      }
    } catch (SQLException e) {
      throwFatalException(e);
    }
    return result;
  }

  @Override
  public long getFeatureCount() {
    long numFeatures = 0;
    for (String table : tableNames()) {
      try (
        var stmt = conn.createStatement();
        @SuppressWarnings("java:S2077") // table name checked against a regex
        var result = stmt.executeQuery("SELECT COUNT(*) FROM %S WHERE GEOMETRY IS NOT NULL;".formatted(table))
      ) {
        numFeatures += result.getLong(1);
      } catch (SQLException e) {
        // maybe no GEOMETRY column?
      }
    }
    return numFeatures;
  }

  @Override
  public void readFeatures(Consumer<SimpleFeature> next) throws Exception {
    long id = 0;
    // pass every element in every table through the profile
    var tables = tableNames();
    for (int i = 0; i < tables.size(); i++) {
      String table = tables.get(i);
      LOGGER.trace("Naturalearth loading {}/{}: {}", i, tables.size(), table);

      try (Statement statement = conn.createStatement()) {
        @SuppressWarnings("java:S2077") // table name checked against a regex
        ResultSet rs = statement.executeQuery("SELECT * FROM %s;".formatted(table));
        String[] column = new String[rs.getMetaData().getColumnCount()];
        int geometryColumn = -1;
        int neIdColumn = -1;
        for (int c = 0; c < column.length; c++) {
          String name = rs.getMetaData().getColumnName(c + 1);
          column[c] = name;
          if ("GEOMETRY".equals(name)) {
            geometryColumn = c;
          } else if ("ne_id".equals(name)) {
            neIdColumn = c;
          }
        }
        if (geometryColumn >= 0) {
          while (rs.next()) {
            byte[] geometry = rs.getBytes(geometryColumn + 1);
            if (geometry == null) {
              continue;
            }

            long neId;
            if (neIdColumn >= 0) {
              neId = rs.getLong(neIdColumn + 1);
            } else {
              neId = id++;
            }

            // create the feature and pass to next stage
            Geometry latLonGeometry = GeoUtils.WKB_READER.read(geometry);
            SimpleFeature readerGeometry = SimpleFeature.create(latLonGeometry, HashMap.newHashMap(column.length - 1),
              sourceName, table, neId);
            for (int c = 0; c < column.length; c++) {
              if (c != geometryColumn) {
                Object value = rs.getObject(c + 1);
                String key = column[c];
                readerGeometry.setTag(key, value);
              }
            }
            next.accept(readerGeometry);
          }
        }
      }
    }
  }

  @Override
  public void close() {
    try {
      conn.close();
    } catch (SQLException e) {
      LOGGER.error("Error closing sqlite file", e);
    }
    if (!keepUnzipped && extracted != null) {
      FileUtils.deleteFile(extracted);
    }
  }
}
