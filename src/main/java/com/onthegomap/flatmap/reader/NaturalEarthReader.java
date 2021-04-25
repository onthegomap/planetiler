package com.onthegomap.flatmap.reader;

import com.onthegomap.flatmap.SourceFeature;
import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.worker.Topology.SourceStep;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipFile;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NaturalEarthReader extends Reader {

  private static final Logger LOGGER = LoggerFactory.getLogger(NaturalEarthReader.class);

  private final Connection conn;
  private Path extracted;

  public NaturalEarthReader(File input, Stats stats) {
    this(input, null, stats);
  }

  public NaturalEarthReader(File input, File tmpDir, Stats stats) {
    super(stats);
    try {
      conn = open(input, tmpDir);
    } catch (IOException | SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private Connection open(File file, File tmpLocation) throws IOException, SQLException {
    String path = "jdbc:sqlite:" + file.getAbsolutePath();
    if (file.getName().endsWith(".zip")) {
      File toOpen = tmpLocation == null ? File.createTempFile("sqlite", "natearth") : tmpLocation;
      extracted = toOpen.toPath();
      toOpen.deleteOnExit();
      try (ZipFile zipFile = new ZipFile(file)) {
        var zipEntry = zipFile.stream()
          .filter(entry -> entry.getName().endsWith(".sqlite"))
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException("No .sqlite file found inside " + file.getName()));
        LOGGER.info("unzipping " + file.getAbsolutePath() + " to " + extracted);
        Files.copy(zipFile.getInputStream(zipEntry), extracted, StandardCopyOption.REPLACE_EXISTING);
      }
      path = "jdbc:sqlite:" + toOpen.getAbsolutePath();
    }
    return DriverManager.getConnection(path);
  }

  private List<String> tableNames() {
    List<String> result = new ArrayList<>();
    try (ResultSet rs = conn.getMetaData().getTables(null, null, null, null)) {
      while (rs.next()) {
        String table = rs.getString("TABLE_NAME");
        result.add(table);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  @Override
  public long getCount() {
    long count = 0;
    for (String table : tableNames()) {
      try (
        var stmt = conn.createStatement();
        var result = stmt.executeQuery("select count(*) from " + table + " where GEOMETRY is not null;")
      ) {
        count += result.getLong(1);
      } catch (SQLException e) {
        // maybe no GEOMETRY column?
      }
    }
    return count;
  }

  @Override
  public SourceStep<SourceFeature> read() {
    return next -> {
      var tables = tableNames();
      for (int i = 0; i < tables.size(); i++) {
        String table = tables.get(i);
        LOGGER.trace("Naturalearth loading " + i + "/" + tables.size() + ": " + table);

        try (Statement statement = conn.createStatement()) {
          ResultSet rs = statement.executeQuery("select * from " + table + ";");
          String[] column = new String[rs.getMetaData().getColumnCount()];
          int geometryColumn = -1;
          for (int c = 0; c < column.length; c++) {
            String name = rs.getMetaData().getColumnName(c + 1);
            column[c] = name;
            if ("GEOMETRY".equals(name)) {
              geometryColumn = c;
            }
          }
          if (geometryColumn >= 0) {
            while (rs.next()) {
              byte[] geometry = rs.getBytes(geometryColumn + 1);
              if (geometry == null) {
                continue;
              }
              Geometry geom = GeoUtils.wkbReader.read(geometry);
              SourceFeature readerGeometry = new ReaderFeature(geom, column.length - 1);
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
    };
  }

  @Override
  public void close() {
    try {
      conn.close();
    } catch (SQLException e) {
      LOGGER.error("Error closing sqlite file", e);
    }
    if (extracted != null) {
      try {
        Files.deleteIfExists(extracted);
      } catch (IOException e) {
        LOGGER.error("Error deleting temp file", e);
      }
    }
  }
}
