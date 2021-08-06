package com.onthegomap.flatmap.reader;

import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.Profile;
import com.onthegomap.flatmap.collection.FeatureGroup;
import com.onthegomap.flatmap.collection.FeatureSort;
import com.onthegomap.flatmap.config.CommonParams;
import com.onthegomap.flatmap.render.FeatureRenderer;
import com.onthegomap.flatmap.stats.ProgressLoggers;
import com.onthegomap.flatmap.stats.Stats;
import com.onthegomap.flatmap.worker.WorkerPipeline;
import java.io.Closeable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.jetbrains.annotations.NotNull;
import org.locationtech.jts.geom.Envelope;

public abstract class Reader implements Closeable {

  protected final Stats stats;
  private final Profile profile;
  protected final String sourceName;

  public Reader(Profile profile, Stats stats, String sourceName) {
    this.stats = stats;
    this.profile = profile;
    this.sourceName = sourceName;
  }

  public final void process(FeatureGroup writer, CommonParams config) {
    var timer = stats.startTimer(sourceName);
    long featureCount = getCount();
    int threads = config.threads();
    Envelope latLonBounds = config.latLonBounds();
    AtomicLong featuresRead = new AtomicLong(0);
    AtomicLong featuresWritten = new AtomicLong(0);

    var pipeline = WorkerPipeline.start(sourceName, stats)
      .fromGenerator("read", read())
      .addBuffer("read_queue", 1000)
      .<FeatureSort.Entry>addWorker("process", threads, (prev, next) -> {
        SourceFeature sourceFeature;
        var featureCollectors = new FeatureCollector.Factory(config, stats);
        FeatureRenderer renderer = getFeatureRenderer(writer, config, next);
        while ((sourceFeature = prev.get()) != null) {
          featuresRead.incrementAndGet();
          FeatureCollector features = featureCollectors.get(sourceFeature);
          if (sourceFeature.latLonGeometry().getEnvelopeInternal().intersects(latLonBounds)) {
            profile.processFeature(sourceFeature, features);
            for (FeatureCollector.Feature renderable : features) {
              renderer.accept(renderable);
            }
          }
        }
      })
      .addBuffer("write_queue", 50_000, 1_000)
      .sinkToConsumer("write", 1, (item) -> {
        featuresWritten.incrementAndGet();
        writer.accept(item);
      });

    var loggers = new ProgressLoggers(sourceName)
      .addRatePercentCounter("read", featureCount, featuresRead)
      .addRateCounter("write", featuresWritten)
      .addFileSize(writer::getStorageSize)
      .addProcessStats()
      .addPipelineStats(pipeline);

    pipeline.awaitAndLog(loggers, config.logInterval());

    profile.finish(sourceName,
      new FeatureCollector.Factory(config, stats),
      getFeatureRenderer(writer, config, writer)
    );
    timer.stop();
  }

  @NotNull
  private FeatureRenderer getFeatureRenderer(FeatureGroup writer, CommonParams config,
    Consumer<FeatureSort.Entry> next) {
    var encoder = writer.newRenderedFeatureEncoder();
    return new FeatureRenderer(
      config,
      rendered -> next.accept(encoder.apply(rendered)),
      stats
    );
  }

  public abstract long getCount();

  public abstract WorkerPipeline.SourceStep<? extends SourceFeature> read();

  @Override
  public abstract void close();
}
