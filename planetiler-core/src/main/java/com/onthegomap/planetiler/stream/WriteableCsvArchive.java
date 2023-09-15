package com.onthegomap.planetiler.stream;

import com.onthegomap.planetiler.archive.TileArchiveConfig;
import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.geo.TileCoord;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Writes tile data into a CSV file (or pipe).
 * <p>
 * A simple (not very efficient) upload to S3 using minio client could look as follows:
 *
 * <pre>
 * mkfifo /tmp/data/output.csv
 * # now run planetiler with the options --append --output=/tmp/data/output.csv
 *
 * # ... and start a script to upload data
 * #! /bin/bash
 * while IFS="," read -r x y z encoded
 * do
 *   echo "pushing tile z=$z x=$x y=$y"
 *   # echo $encoded | base64 -d | gzip -d | aws s3 cp - s3://BUCKET/map/$z/$x/$y.pbf --content-type=application/x-protobuf
 *   echo $encoded | base64 -d | aws s3 cp - s3://BUCKET/map/$z/$x/$y.pbf --content-type=application/x-protobuf --content-encoding=gzip
 * done &lt; "${1:-/dev/stdin}"
 * </pre>
 *
 * Loading data into mysql could be done like this:
 *
 * <pre>
 * mkfifo /tmp/data/output.csv
 * # now run planetiler with the options --append --output=/tmp/data/output.csv
 *
 * mysql&gt; ...create tile(s) table
 * mysql&gt; LOAD DATA INFILE '/tmp/data/output.csv'
 *  -&gt; INTO TABLE tiles
 *  -&gt; FIELDS TERMINATED BY ','
 *  -&gt; LINES TERMINATED BY '\n'
 *  -&gt; (tile_column, tile_row, zoom_level, @var1)
 *  -&gt; SET tile_data = FROM_BASE64(@var1);
 * </pre>
 *
 * Loading data into postgres could be done like this:
 *
 * <pre>
 * mkfifo /tmp/data/output_raw.csv
 * mkfifo /tmp/data/output_transformed.csv
 * # prefix hex-data with '\x' for the postgres import
 * cat /tmp/data/output_raw.csv | sed -r 's/^([0-9]+,)([0-9]+,)([0-9]+,)(.*)$/\1\2\3\\x\4/' > /tmp/data/output_transformed.csv
 * # now run planetiler with the options --append --output=/tmp/data/output_raw.csv --csv_binary_encoding=hex
 * ...create tile(s) table
 * postgres=# \copy tiles(tile_column, tile_row, zoom_level, tile_data) from /tmp/data/output_transformed.csv DELIMITER ',' CSV;
 * </pre>
 *
 * Check {@link WriteableStreamArchive} to see how to write to multiple files. This can be used to parallelize uploads.
 */
public final class WriteableCsvArchive extends WriteableStreamArchive {

  static final String OPTION_COLUMN_SEPARATOR = "column_separator";
  static final String OPTION_LINE_SEPARTATOR = "line_separator";
  static final String OPTION_BINARY_ENCODING = "binary_encoding";

  private final String columnSeparator;
  private final String lineSeparator;
  private final Function<byte[], String> tileDataEncoder;

  private WriteableCsvArchive(TileArchiveConfig.Format format, Path p, StreamArchiveConfig config) {
    super(p, config);
    final String defaultColumnSeparator = switch (format) {
      case CSV -> "','";
      case TSV -> "'\\t'";
      default -> throw new IllegalArgumentException("supported formats are csv and tsv but got " + format.id());
    };
    this.columnSeparator = StreamArchiveUtils.getEscapedString(config.moreOptions(), format,
      OPTION_COLUMN_SEPARATOR, "column separator", defaultColumnSeparator, List.of(",", " "));
    this.lineSeparator = StreamArchiveUtils.getEscapedString(config.moreOptions(), format,
      OPTION_LINE_SEPARTATOR, "line separator", "'\\n'", List.of("\n", "\r\n"));
    final BinaryEncoding binaryEncoding = BinaryEncoding.fromId(config.moreOptions().getString(OPTION_BINARY_ENCODING,
      "binary (tile) data encoding - one of " + BinaryEncoding.ids(), "base64"));
    this.tileDataEncoder = switch (binaryEncoding) {
      case BASE64 -> Base64.getEncoder()::encodeToString;
      case HEX -> HexFormat.of()::formatHex;
    };
  }

  public static WriteableCsvArchive newWriteToFile(TileArchiveConfig.Format format, Path path,
    StreamArchiveConfig config) {
    return new WriteableCsvArchive(format, path, config);
  }

  @Override
  protected TileWriter newTileWriter(OutputStream outputStream) {
    return new CsvTileWriter(outputStream, columnSeparator, lineSeparator, tileDataEncoder);
  }

  private static class CsvTileWriter implements TileWriter {

    private final Function<byte[], String> tileDataEncoder;

    private final Writer writer;

    private final String columnSeparator;
    private final String lineSeparator;

    CsvTileWriter(Writer writer, String columnSeparator, String lineSeparator,
      Function<byte[], String> tileDataEncoder) {

      this.writer = writer;
      this.columnSeparator = columnSeparator;
      this.lineSeparator = lineSeparator;
      this.tileDataEncoder = tileDataEncoder;

    }

    CsvTileWriter(OutputStream outputStream, String columnSeparator, String lineSeparator,
      Function<byte[], String> tileDataEncoder) {
      this(new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8.newEncoder())),
        columnSeparator, lineSeparator, tileDataEncoder);
    }

    @Override
    public void write(TileEncodingResult encodingResult) {
      final TileCoord coord = encodingResult.coord();
      final String tileDataEncoded = tileDataEncoder.apply(encodingResult.tileData());
      try {
        // x | y | z | encoded data
        writer.write("%d%s%d%s%d%s%s%s".formatted(coord.x(), columnSeparator, coord.y(), columnSeparator, coord.z(),
          columnSeparator, tileDataEncoded, lineSeparator));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public void close() {
      try {
        writer.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  private enum BinaryEncoding {

    BASE64("base64"),
    HEX("hex");

    private final String id;

    private BinaryEncoding(String id) {
      this.id = id;
    }

    static List<String> ids() {
      return Stream.of(BinaryEncoding.values()).map(BinaryEncoding::id).toList();
    }

    static BinaryEncoding fromId(String id) {
      return Stream.of(BinaryEncoding.values())
        .filter(de -> de.id().equals(id))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException(
          "unexpected binary encoding - expected one of " + ids() + " but got " + id));
    }

    String id() {
      return id;
    }
  }
}
