package com.onthegomap.planetiler.stream;

import com.onthegomap.planetiler.archive.WriteableTileArchive;
import com.onthegomap.planetiler.geo.TileOrder;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.core.util.CloseShieldOutputStream;
import org.checkerframework.dataflow.qual.Pure;

/**
 * Base archive for all kinds of simple file streams. This is primarily useful when the file is a named pipe. In that
 * case data can directly be transformed and consumed by other programs.
 * <p>
 * Writing can be parallelized across multiple files (tile_write_threads). For the first file the base path is used. For
 * consecutive files 1, 2, ... is appended to the base bath.
 *
 * <pre>
 * # create the pipes
 * mkfifo /tmp/data/output.csv
 * mkfifo /tmp/data/output.csv1
 * mkfifo /tmp/data/output.csv2
 * # start the consumers
 * consumer_program < /tmp/data/output.csv
 * consumer_program < /tmp/data/output.csv1
 * consumer_program < /tmp/data/output.csv2
 *
 * # now run planetiler with the options --append --output=/tmp/data/output.csv --tile_write_threads=3
 * </pre>
 */
abstract class WritableStreamArchive implements WriteableTileArchive {

  private final OutputStream primaryOutputStream;
  private final OutputStreamSupplier outputStreamFactory;
  @SuppressWarnings("unused")
  private final StreamArchiveConfig config;

  private final AtomicInteger tileWriterCounter = new AtomicInteger(0);

  private WritableStreamArchive(OutputStreamSupplier outputStreamFactory, StreamArchiveConfig config) {
    this.outputStreamFactory = outputStreamFactory;
    this.config = config;

    this.primaryOutputStream = outputStreamFactory.newOutputStream(0);
  }

  protected WritableStreamArchive(Path p, StreamArchiveConfig config) {
    this(new FileOutputStreamSupplier(p, config.appendToFile()), config);
  }

  @Override
  public final void close() throws IOException {
    primaryOutputStream.close();
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
  public final TileWriter newTileWriter() {
    final int tileWriterIndex = tileWriterCounter.getAndIncrement();
    if (tileWriterIndex == 0) {
      return newTileWriter(getPrimaryOutputStream());
    } else {
      return newTileWriter(outputStreamFactory.newOutputStream(tileWriterIndex));
    }

  }

  @Pure
  protected abstract TileWriter newTileWriter(OutputStream outputStream);

  protected final OutputStream getPrimaryOutputStream() {
    /*
     * the outputstream of the first writer must be closed by the archive and not the tile writer
     * since the primary stream can be used to send meta data, as well
     */
    return new CloseShieldOutputStream(primaryOutputStream);
  }

  @FunctionalInterface
  private interface OutputStreamSupplier {
    OutputStream newOutputStream(int index);
  }

  private static class FileOutputStreamSupplier implements OutputStreamSupplier {

    private final Path basePath;
    private final OpenOption[] openOptions;

    FileOutputStreamSupplier(Path basePath, boolean append) {
      this.basePath = basePath;
      this.openOptions =
        new OpenOption[]{StandardOpenOption.WRITE, append ? StandardOpenOption.APPEND : StandardOpenOption.CREATE_NEW};
    }

    @Override
    public OutputStream newOutputStream(int index) {
      final Path p = StreamArchiveUtils.constructIndexedPath(basePath, index);
      try {
        return Files.newOutputStream(p, openOptions);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }
}
