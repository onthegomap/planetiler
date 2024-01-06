package com.onthegomap.planetiler.stream;

import com.onthegomap.planetiler.archive.Tile;
import com.onthegomap.planetiler.archive.TileArchiveConfig;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.util.CloseableIterator;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Scanner;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * Reads tiles from a CSV file. Counterpart to {@link WriteableCsvArchive}.
 * <p>
 * Supported arguments:
 * <dl>
 * <dt>column_separator</dt>
 * <dd>The column separator e.g. ",", ";", "\t"</dd>
 * <dt>line_separator</dt>
 * <dd>The line separator e.g. "\n", "\r", "\r\n"</dd>
 * </dl>
 *
 * @see WriteableCsvArchive
 */
public class ReadableCsvArchive extends ReadableStreamArchive<String> {

  private final Pattern columnSeparatorPattern;
  private final Pattern lineSeparatorPattern;
  private final Function<String, byte[]> tileDataDecoder;

  private ReadableCsvArchive(TileArchiveConfig.Format format, Path basePath, StreamArchiveConfig config) {
    super(basePath, config);
    this.columnSeparatorPattern =
      Pattern.compile(Pattern.quote(StreamArchiveUtils.csvOptionColumnSeparator(config.formatOptions(), format)));
    this.lineSeparatorPattern =
      Pattern.compile(Pattern.quote(StreamArchiveUtils.csvOptionLineSeparator(config.formatOptions(), format)));
    final CsvBinaryEncoding binaryEncoding = StreamArchiveUtils.csvOptionBinaryEncoding(config.formatOptions());
    this.tileDataDecoder = binaryEncoding::decode;
  }

  public static ReadableCsvArchive newReader(TileArchiveConfig.Format format, Path basePath,
    StreamArchiveConfig config) {
    return new ReadableCsvArchive(format, basePath, config);
  }

  @Override
  CloseableIterator<String> createIterator() {
    try {
      @SuppressWarnings("java:S2095") final Scanner s =
        new Scanner(basePath.toFile()).useDelimiter(lineSeparatorPattern);
      return new CloseableIterator<>() {
        @Override
        public void close() {
          s.close();
        }

        @Override
        public boolean hasNext() {
          return s.hasNext();
        }

        @Override
        public String next() {
          return s.next();
        }
      };
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

  }

  @Override
  Optional<Tile> mapEntryToTile(String entry) {
    final String[] splits = columnSeparatorPattern.split(entry);
    final byte[] bytes;
    if (splits.length == 4) {
      bytes = tileDataDecoder.apply(splits[3].strip());
    } else if (splits.length == 3) {
      bytes = null;
    } else {
      throw new InvalidCsvFormat(entry.length() > 20 ? entry.substring(0, 20) + "..." : entry);
    }
    return Optional.of(new Tile(
      TileCoord.ofXYZ(
        Integer.parseInt(splits[0].strip()),
        Integer.parseInt(splits[1].strip()),
        Integer.parseInt(splits[2].strip())
      ),
      bytes
    ));
  }

  @Override
  Optional<TileArchiveMetadata> mapEntryToMetadata(String entry) {
    return Optional.empty();
  }

  static class InvalidCsvFormat extends RuntimeException {
    InvalidCsvFormat(String message) {
      super(message);
    }
  }
}
