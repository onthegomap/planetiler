package com.onthegomap.planetiler.files;

import com.google.common.base.Preconditions;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.archive.TileArchiveMetadataDeSer;
import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.archive.WriteableTileArchive;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.geo.TileOrder;
import com.onthegomap.planetiler.util.CountingOutputStream;
import com.onthegomap.planetiler.util.FileUtils;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteableFilesArchive implements WriteableTileArchive {

  private static final Logger LOGGER = LoggerFactory.getLogger(WriteableFilesArchive.class);

  private final LongAdder bytesWritten = new LongAdder();

  private final Path basePath;
  private final Path metadataPath;

  private final Function<TileCoord, Path> tileSchemeEncoder;

  private final TileOrder tileOrder;

  private final WriteMode writeMode;

  private WriteableFilesArchive(Path basePath, Arguments options, boolean overwriteMetadata) {
    this.basePath = createValidateDirectory(basePath);
    this.metadataPath = FilesArchiveUtils.metadataPath(basePath, options)
      .flatMap(p -> FilesArchiveUtils.metadataPath(p.getParent(), options))
      .orElse(null);
    if (this.metadataPath != null && Files.exists(this.metadataPath)) {
      if (!overwriteMetadata) {
        throw new IllegalArgumentException(this.metadataPath + " already exists");
      } else if (!Files.isRegularFile(this.metadataPath)) {
        throw new IllegalArgumentException("require " + this.metadataPath + " to be a regular file");
      }
    }
    final TileSchemeEncoding tileSchemeEncoding = FilesArchiveUtils.tilesSchemeEncoding(options, basePath);
    this.tileSchemeEncoder = tileSchemeEncoding.encoder();
    this.tileOrder = tileSchemeEncoding.preferredTileOrder();
    this.writeMode = FilesArchiveUtils.writeMode(options);
  }

  public static WriteableFilesArchive newWriter(Path basePath, Arguments options, boolean overwriteMetadata) {
    return new WriteableFilesArchive(basePath, options, overwriteMetadata);
  }

  @Override
  public boolean deduplicates() {
    return false;
  }

  @Override
  public TileOrder tileOrder() {
    return tileOrder;
  }

  @Override
  public TileWriter newTileWriter() {
    return switch (writeMode) {
      case SYNC -> new SyncTileFilesWriter(basePath, tileSchemeEncoder, bytesWritten);
      case ASYNC -> new AsyncTileFilesWriter(basePath, tileSchemeEncoder, bytesWritten);
    };
  }

  @Override
  public void finish(TileArchiveMetadata tileArchiveMetadata) {
    if (metadataPath == null) {
      return;
    }
    try (OutputStream s = new CountingOutputStream(Files.newOutputStream(metadataPath), bytesWritten::add)) {
      TileArchiveMetadataDeSer.mbtilesMapper().writeValue(s, tileArchiveMetadata);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public long bytesWritten() {
    return bytesWritten.sum();
  }

  @Override
  public void close() throws IOException {
    // nothing to do here
  }

  private static Path createValidateDirectory(Path p) {
    if (!Files.exists(p)) {
      FileUtils.createDirectory(p);
    }
    Preconditions.checkArgument(
      Files.isDirectory(p),
      "require \"" + p + "\" to be a directory"
    );
    return p;
  }

  private abstract static class BaseTileFilesWriter implements TileWriter {

    private final Function<TileCoord, Path> tileSchemeEncoder;
    private final LongAdder bytesWritten;
    private Path lastCheckedFolder;

    BaseTileFilesWriter(Path basePath, Function<TileCoord, Path> tileSchemeEncoder, LongAdder bytesWritten) {
      this.tileSchemeEncoder = tileSchemeEncoder;
      this.lastCheckedFolder = basePath;
      this.bytesWritten = bytesWritten;
    }

    @Override
    public final void write(TileEncodingResult encodingResult) {

      final byte[] data = encodingResult.tileData();

      final Path file = tileSchemeEncoder.apply(encodingResult.coord());
      final Path folder = file.getParent();

      // tiny optimization in order to avoid too many unnecessary "folder-exists-checks" (I/O)
      // only effective when the tileScheme is z/x/y but doesn't really harm otherwise
      if (!lastCheckedFolder.equals(folder) && !Files.exists(folder)) {
        FileUtils.createDirectory(folder);
      }
      lastCheckedFolder = folder;
      try {
        writeData(file, data);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      bytesWritten.add(data.length);
    }

    protected abstract void writeData(Path p, byte[] b) throws IOException;
  }

  private static class SyncTileFilesWriter extends BaseTileFilesWriter {

    SyncTileFilesWriter(Path basePath, Function<TileCoord, Path> tileSchemeEncoder, LongAdder bytesWritten) {
      super(basePath, tileSchemeEncoder, bytesWritten);
    }

    @Override
    protected void writeData(Path p, byte[] b) throws IOException {
      try {
        Files.write(p, b);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public void close() {
      // nothing to do here
    }
  }

  private static class AsyncTileFilesWriter extends BaseTileFilesWriter {

    private final Phaser phaser = new Phaser(1); // register self

    private Throwable failure;

    AsyncTileFilesWriter(Path basePath, Function<TileCoord, Path> tileSchemeEncoder, LongAdder bytesWritten) {
      super(basePath, tileSchemeEncoder, bytesWritten);
    }

    @Override
    protected void writeData(Path p, byte[] b) throws IOException {
      // propagate failure from previous run if possible
      throwOnFailure();

      try {
        // do not use try-with-resource and close since otherwise the file can't be written async anymore
        @SuppressWarnings("java:S2095") final AsynchronousFileChannel asyncFile =
          AsynchronousFileChannel.open(p, StandardOpenOption.WRITE,
            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        phaser.register();

        asyncFile.write(ByteBuffer.wrap(b), 0, phaser,
          new CompletionHandler<>() {
            @Override
            public void completed(Integer integer, Phaser phaser) {
              phaser.arriveAndDeregister();
              closeFileSilently();
            }

            @Override
            public void failed(Throwable t, Phaser phaser) {
              LOGGER.atError().setCause(t).setMessage(() -> "failed to write file" + p).log();
              if (failure == null) {
                failure = t;
              }
              phaser.arriveAndDeregister();
              closeFileSilently();
            }

            private void closeFileSilently() {
              try {
                asyncFile.close();
              } catch (IOException e) {
                LOGGER.atError().setCause(e).setMessage(() -> "failed to close file" + p).log();
              }
            }
          });
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public void close() {
      phaser.arriveAndAwaitAdvance();
      throwOnFailure();
    }

    @SuppressWarnings("java:S112")
    private void throwOnFailure() {
      // _try_ to fail early... value may not be visible to others
      if (failure instanceof RuntimeException re) {
        throw re;
      } else if (failure != null) {
        throw new RuntimeException(failure);
      }
    }
  }

  enum WriteMode {
    SYNC,
    ASYNC
  }
}
