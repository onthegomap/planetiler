package com.onthegomap.planetiler.util;

import com.onthegomap.planetiler.archive.ReadableTileArchive;
import com.onthegomap.planetiler.archive.TileArchiveConfig;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.archive.WriteableTileArchive;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.geo.TileOrder;
import de.bytefish.pgbulkinsert.row.SimpleRowWriter;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.postgresql.PGConnection;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Pgtiles implements ReadableTileArchive, WriteableTileArchive {
  private static final Logger LOGGER = LoggerFactory.getLogger(Pgtiles.class);

  private final String url;
  private final Arguments options;
  private final Connection connection;

  public Pgtiles(String url, Arguments options) throws SQLException {
    this.url = url;
    this.options = options;
    var props = new Properties();
    props.putAll(options.toMap());
    this.connection = DriverManager.getConnection(url, props);
  }

  public static WriteableTileArchive writer(URI uri, Arguments options) {
    try {
      return new Pgtiles("jdbc:postgresql://" + uri.getAuthority() + uri.getPath(), options);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public static ReadableTileArchive reader(URI uri, Arguments options) {
    try {
      return new Pgtiles("jdbc:postgresql://" + uri.getAuthority() + uri.getPath(), options);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }


  public static long getSize(TileArchiveConfig archiveConfig) {
    Properties options = new Properties();
    options.putAll(archiveConfig.options());
    String url = "jdbc:postgresql://" + archiveConfig.uri().getAuthority() + archiveConfig.uri().getPath();
    return getSize(url, archiveConfig.options());
  }

  public static long getSize(String url, Map<String, String> args) {
    Properties options = new Properties();
    options.putAll(args);
    try (
      var connection = DriverManager.getConnection(url, options);
      var stmt = connection.createStatement()
    ) {
      var result = stmt.executeQuery("select pg_total_relation_size('public.tile_data')");
      result.next();
      return result.getLong(1);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public byte[] getTile(int x, int y, int z) {
    return null;
  }

  private void execute(String... queries) {
    for (String query : queries) {
      try (var statement = connection.createStatement()) {
        LOGGER.debug("Execute postgres: {}", query);
        statement.execute(query);
      } catch (SQLException throwables) {
        throw new IllegalStateException("Error executing queries " + String.join(",", queries), throwables);
      }
    }
  }

  public static void main(String[] args) throws SQLException {
    try (
      var connection = DriverManager.getConnection(args[0]);
    ) {
      LOGGER.info("Connected...");
      try (
        var result = connection.prepareStatement("select id from tile_data").executeQuery()
      ) {
        LOGGER.info("Starting...");
        RoaringBitmap bitmap = new RoaringBitmap();
        int i = 0;
        while (result.next()) {
          if (i++ % 1_000_000 == 0) {
            LOGGER.info("Finished {} bitmap size {}", i, Format.defaultInstance().storage(bitmap.getLongSizeInBytes()));
          }
          bitmap.add(result.getInt("id"));
        }
        LOGGER.info("Finished {} bitmap size {}", i, Format.defaultInstance().storage(bitmap.getLongSizeInBytes()));
        bitmap.runOptimize();
        LOGGER.info("Finished {} bitmap size {}", i, Format.defaultInstance().storage(bitmap.getLongSizeInBytes()));
      }
    }
  }

  @Override
  public void initialize(TileArchiveMetadata metadata) {
    execute("""
      DROP TABLE IF EXISTS tile_data
      """);
    execute("""
      CREATE TABLE tile_data (
        id INTEGER PRIMARY KEY,
        data BYTEA
      )""");
  }

  @Override
  public void finish(TileArchiveMetadata metadata) {
    LOGGER.info("Final postgres DB size: {}", Format.defaultInstance().storage(getSize(url, options.toMap())));
  }

  @Override
  public CloseableIterator<TileCoord> getAllTileCoords() {
    return CloseableIterator.wrap(Collections.emptyIterator());
  }

  @Override
  public TileArchiveMetadata metadata() {
    return null;
  }

  @Override
  public boolean deduplicates() {
    return false;
  }

  @Override
  public TileOrder tileOrder() {
    return TileOrder.TMS;
  }

  @Override
  public TileWriter newTileWriter() {
    return new BulkLoader();
  }

  @Override
  public void close() throws IOException {

  }

  private class BulkLoader implements TileWriter {

    private final SimpleRowWriter writer;

    private BulkLoader() {
      PGConnection pgConnection = PostgreSqlUtils.getPGConnection(connection);
      try {
        this.writer = new SimpleRowWriter(new SimpleRowWriter.Table("public", "tile_data", new String[]{"id", "data"}),
          pgConnection);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void write(TileEncodingResult encodingResult) {
      this.writer.startRow(row -> {
        row.setInteger(0, encodingResult.coord().encoded());
        row.setByteArray(1, encodingResult.tileData());
      });
    }

    @Override
    public void close() {
      this.writer.close();
    }
  }
}
