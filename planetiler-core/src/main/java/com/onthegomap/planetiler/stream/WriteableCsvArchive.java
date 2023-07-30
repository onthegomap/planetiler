package com.onthegomap.planetiler.stream;

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
 * mkfifo /var/lib/mysql-files/output.csv
 * # now run planetiler with the options --append --output=/var/lib/mysql-files/output.csv
 *
 * mysql&gt; ...create tile(s) table
 * mysql&gt; LOAD DATA INFILE '/var/lib/mysql-files/output.csv'
 *  -&gt; INTO TABLE tiles
 *  -&gt; FIELDS TERMINATED BY ','
 *  -&gt; LINES TERMINATED BY '\n'
 *  -&gt; (tile_column, tile_row, zoom_level, @var1)
 *  -&gt; SET tile_data = FROM_BASE64(@var1);
 * </pre>
 *
 * Check {@link WritableStreamArchive} to see how to write to multiple files. This can be used to parallelize uploads.
 */
public final class WriteableCsvArchive extends WritableStreamArchive {

  static final String OPTION_DELIMITER = "delimiter";

  private final String delimiter;

  private WriteableCsvArchive(Path p, StreamArchiveConfig config) {
    super(p, config);
    this.delimiter = config.moreOptions()
      .getString(OPTION_DELIMITER,
        "field delimiter - pass from command line as follows delimiter=',' delimiter=' '",
        "','")
      // allow values to be wrapped by single quotes => allows to pass a space which otherwise gets trimmed
      .replaceAll("^'(.+?)'$", "$1")
      .translateEscapes();
  }

  public static WriteableCsvArchive newWriteToFile(Path path, StreamArchiveConfig config) {
    return new WriteableCsvArchive(path, config);
  }

  @Override
  protected TileWriter newTileWriter(OutputStream outputStream) {
    return new CsvTileWriter(outputStream, delimiter);
  }

  private static class CsvTileWriter implements TileWriter {

    private static final Base64.Encoder tileDataEncoder = Base64.getEncoder();

    private final Writer writer;

    private final String delimiter;

    CsvTileWriter(Writer writer, String delimiter) {
      this.writer = writer;
      this.delimiter = delimiter;
    }

    CsvTileWriter(OutputStream outputStream, String delimiter) {
      this(new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8.newEncoder())), delimiter);
    }

    @Override
    public void write(TileEncodingResult encodingResult) {
      final TileCoord coord = encodingResult.coord();
      final String tileDataEncoded = tileDataEncoder.encodeToString(encodingResult.tileData());
      try {
        // x | y | z | encoded data
        writer.write("%d%s%d%s%d%s%s\n"
          .formatted(coord.x(), delimiter, coord.y(), delimiter, coord.z(), delimiter, tileDataEncoded));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public void close() {
      try {
        writer.close();
        writer.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

  }
}
