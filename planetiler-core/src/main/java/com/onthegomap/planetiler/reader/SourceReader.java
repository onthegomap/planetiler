package com.onthegomap.planetiler.reader;

import com.onthegomap.planetiler.config.Bounds;
import com.onthegomap.planetiler.stats.SourceProgress;
import java.io.Closeable;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * One source of features (shapefiles, GeoPackage, GeoParquet, ...) that {@link SourceFeatureProcessor} reads together
 * with other sources in one shared pipeline.
 * <p>
 * A source may still be waiting for something when it gets created, like a download or the footer of a remote file.
 * {@link #whenReady()} says when that is over, and only then does a read worker call {@link #read(Consumer)}.
 */
public interface SourceReader extends Closeable {

  /** Returns a source that reads each of {@code paths} with a {@link SimpleReader} once {@code ready} completes. */
  static SourceReader ofFiles(String name, Supplier<List<Path>> paths,
    Function<Path, ? extends SimpleReader<?>> readerFactory, CompletableFuture<Void> ready, Bounds bounds) {
    return new SimpleSourceReader(name, paths, readerFactory, ready, bounds);
  }

  /** Returns the name profiles know this source by, also used in logs and stats. */
  String name();

  /** Returns a future that completes when {@link #read(Consumer)} can start, or fails if this source can't be read. */
  CompletableFuture<Void> whenReady();

  /**
   * Reads every feature of this source and passes them to {@code next} in batches.
   * <p>
   * Runs on one read worker. A batch may be lazy, so that the expensive part of decoding it happens on the process
   * worker that iterates through it.
   */
  @SuppressWarnings("java:S112")
  void read(Consumer<Iterable<? extends SourceFeature>> next) throws Exception;

  /** Returns how far along this source is, for the logs. */
  SourceProgress progress();

  /**
   * Returns a future with what reading this source will fetch from a remote server, once that is known, or fails if it
   * can't be found out. It is known by the time {@link #whenReady()} completes.
   * <p>
   * Downloads that have to finish before reading starts don't count, only what {@link #read(Consumer)} fetches.
   */
  default CompletableFuture<Fetch> fetchWhileReading() {
    return CompletableFuture.completedFuture(Fetch.NONE);
  }

  /** The {@code bytes} a source fetches from {@code host} while it gets read. */
  record Fetch(long bytes, String host) {

    /** Nothing to fetch. */
    public static final Fetch NONE = new Fetch(0, null);
  }
}
