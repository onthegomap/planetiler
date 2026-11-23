package com.onthegomap.planetiler.reader;

import com.onthegomap.planetiler.config.Bounds;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.shapefile.ShapefileFeature;
import com.onthegomap.planetiler.stats.SourceProgress;
import com.onthegomap.planetiler.util.FileUtils;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A source made of files that each get read by a {@link SimpleReader}. */
class SimpleSourceReader implements SourceReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleSourceReader.class);
  // Hand features off from reader to workers in batches of up to 16 features to improve throughput for many small
  // features, but pass big features off in their own batch, capping total points across all features in a batch to ~1k
  private static final int BATCH_SIZE = 16;
  private static final int BATCH_MAX_POINTS = 1_000;

  private final String name;
  private final Supplier<List<Path>> paths;
  private final Function<Path, ? extends SimpleReader<?>> readerFactory;
  private final CompletableFuture<Void> ready;
  private final Envelope latLonBounds;
  private final SourceProgress progress;
  private final AtomicLong featuresRead = new AtomicLong();

  SimpleSourceReader(String name, Supplier<List<Path>> paths, Function<Path, ? extends SimpleReader<?>> readerFactory,
    CompletableFuture<Void> ready, Bounds bounds) {
    this.name = name;
    this.paths = paths;
    this.readerFactory = readerFactory;
    this.ready = ready;
    this.latLonBounds = bounds.latLon();
    this.progress = new SourceProgress(name);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public CompletableFuture<Void> whenReady() {
    return ready;
  }

  @Override
  public SourceProgress progress() {
    return progress;
  }

  @Override
  public void read(Consumer<Iterable<? extends SourceFeature>> next) throws Exception {
    // one reader per file for both counting and reading, so expensive setup like unzipping only happens once
    List<SimpleReader<?>> readers = new ArrayList<>();
    try {
      long featureCount = 0;
      // the files of a zip or directory are only known once it is downloaded, so list them here
      for (Path path : paths.get()) {
        SimpleReader<?> reader = readerFactory.apply(path);
        readers.add(reader);
        // start each file's IDs where previous one left off so they don't collide
        reader.setFeatureIdOffset(featureCount);
        featureCount += reader.getFeatureCount();
      }
      progress.setRead(featuresRead::get, featureCount);
      var batches = new Batches(next);
      for (var reader : readers) {
        readInBounds(reader, batches);
      }
      batches.flush();
    } finally {
      FileUtils.closeAll(readers);
    }
  }

  private <F extends SourceFeature> void readInBounds(SimpleReader<F> reader, Batches batches) throws Exception {
    boolean checkBounds = !reader.filtersToBounds();
    reader.readFeatures(feature -> {
      featuresRead.incrementAndGet();
      if (!checkBounds || intersects(feature)) {
        batches.add(feature);
      }
    });
  }

  private boolean intersects(SourceFeature feature) {
    try {
      return feature.latLonGeometry().getEnvelopeInternal().intersects(latLonBounds);
    } catch (GeometryException e) {
      // features that decode their geometry lazily throw here when it can't be read
      LOGGER.warn("Skipping feature of {}: {}", name, e.getMessage());
      return false;
    }
  }

  @Override
  public void close() {
    // each reader gets closed as soon as read() is done with it
  }

  /** Groups features into the batches that get handed to the process workers. */
  private static class Batches {
    private final Consumer<Iterable<? extends SourceFeature>> next;
    private List<SourceFeature> batch = new ArrayList<>(BATCH_SIZE);
    private int points = 0;

    Batches(Consumer<Iterable<? extends SourceFeature>> next) {
      this.next = next;
    }

    void add(SourceFeature feature) {
      batch.add(feature);
      points += switch (feature) {
        case SimpleFeature simpleFeature -> simpleFeature.numPoints();
        case ShapefileFeature shapefileFeature -> shapefileFeature.numPoints();
        // features with an unknown cost get handed off on their own
        default -> BATCH_MAX_POINTS;
      };
      if (batch.size() >= BATCH_SIZE || points >= BATCH_MAX_POINTS) {
        flush();
      }
    }

    void flush() {
      if (!batch.isEmpty()) {
        next.accept(batch);
        batch = new ArrayList<>(BATCH_SIZE);
        points = 0;
      }
    }
  }
}
