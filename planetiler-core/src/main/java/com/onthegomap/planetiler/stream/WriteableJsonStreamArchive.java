package com.onthegomap.planetiler.stream;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.geo.TileCoord;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes JSON-serialized tile data as well as meta data into file(s). The entries are of type
 * {@link JsonStreamArchiveEntry} are separated by newline (by default).
 */
public final class WriteableJsonStreamArchive extends WriteableStreamArchive {

  private static final Logger LOGGER = LoggerFactory.getLogger(WriteableJsonStreamArchive.class);

  private static final JsonMapper jsonMapper = StreamArchiveUtils.jsonMapperJsonStreamArchive;

  private final boolean writeTilesOnly;
  private final String rootValueSeparator;

  private WriteableJsonStreamArchive(Path p, StreamArchiveConfig config) {
    super(p, config);
    this.writeTilesOnly = StreamArchiveUtils.jsonOptionWriteTilesOnly(config.formatOptions());
    this.rootValueSeparator = StreamArchiveUtils.jsonOptionRootValueSeparator(config.formatOptions());
  }

  public static WriteableJsonStreamArchive newWriteToFile(Path path, StreamArchiveConfig config) {
    return new WriteableJsonStreamArchive(path, config);
  }

  @Override
  protected TileWriter newTileWriter(OutputStream outputStream) {
    return new JsonTileWriter(outputStream, rootValueSeparator);
  }

  @Override
  public void initialize() {
    if (writeTilesOnly) {
      return;
    }
    writeEntryFlush(new JsonStreamArchiveEntry.InitializationEntry());
  }

  @Override
  public void finish(TileArchiveMetadata metadata) {
    if (writeTilesOnly) {
      return;
    }
    writeEntryFlush(new JsonStreamArchiveEntry.FinishEntry(metadata));
  }

  private void writeEntryFlush(JsonStreamArchiveEntry entry) {
    try (var out = new OutputStreamWriter(getPrimaryOutputStream(), StandardCharsets.UTF_8.newEncoder())) {
      jsonMapper
        .writerFor(JsonStreamArchiveEntry.class)
        .withoutFeatures(JsonGenerator.Feature.AUTO_CLOSE_TARGET)
        .writeValue(out, entry);
      out.write(rootValueSeparator);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static class JsonTileWriter implements TileWriter {

    private final OutputStream outputStream;
    private final SequenceWriter jsonWriter;
    private final String rootValueSeparator;

    JsonTileWriter(OutputStream out, String rootValueSeparator) {
      this.outputStream = new BufferedOutputStream(out);
      this.rootValueSeparator = rootValueSeparator;
      try {
        this.jsonWriter =
          jsonMapper.writerFor(JsonStreamArchiveEntry.class).withRootValueSeparator(rootValueSeparator)
            .writeValues(outputStream);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public void write(TileEncodingResult encodingResult) {
      final TileCoord coord = encodingResult.coord();
      try {
        jsonWriter
          .write(new JsonStreamArchiveEntry.TileEntry(coord.x(), coord.y(), coord.z(), encodingResult.tileData()));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public void close() {
      UncheckedIOException flushOrWriteError = null;
      try {
        jsonWriter.flush();
        // jackson only handles newlines between entries but does not append one to the last one
        for (byte b : rootValueSeparator.getBytes(StandardCharsets.UTF_8)) {
          outputStream.write(b);
        }
      } catch (IOException e) {
        LOGGER.warn("failed to finish writing", e);
        flushOrWriteError = new UncheckedIOException(e);
      }

      try {
        jsonWriter.close();
        outputStream.close();
      } catch (IOException e) {
        if (flushOrWriteError != null) {
          e.addSuppressed(flushOrWriteError);
        }
        throw new UncheckedIOException(e);
      }

      if (flushOrWriteError != null) {
        throw flushOrWriteError;
      }
    }
  }

}
