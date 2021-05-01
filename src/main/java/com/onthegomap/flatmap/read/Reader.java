package com.onthegomap.flatmap.read;

import com.onthegomap.flatmap.CommonParams;
import com.onthegomap.flatmap.FeatureRenderer;
import com.onthegomap.flatmap.Profile;
import com.onthegomap.flatmap.RenderableFeature;
import com.onthegomap.flatmap.RenderableFeatures;
import com.onthegomap.flatmap.SourceFeature;
import com.onthegomap.flatmap.collections.FeatureGroup;
import com.onthegomap.flatmap.collections.FeatureSort;
import com.onthegomap.flatmap.monitoring.ProgressLoggers;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.worker.Topology;
import java.io.Closeable;
import java.util.concurrent.atomic.AtomicLong;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Reader implements Closeable {

  protected final Stats stats;
  private final Logger LOGGER = LoggerFactory.getLogger(getClass());
  private final Profile profile;

  public Reader(Profile profile, Stats stats) {
    this.stats = stats;
    this.profile = profile;
  }

  public final void process(String name, FeatureRenderer renderer, FeatureGroup writer, CommonParams config) {
    long featureCount = getCount();
    int threads = config.threads();
    Envelope env = config.bounds();
    AtomicLong featuresRead = new AtomicLong(0);
    AtomicLong featuresWritten = new AtomicLong(0);

    var topology = Topology.start(name, stats)
      .fromGenerator("read", read())
      .addBuffer("read_queue", 1000)
      .<FeatureSort.Entry>addWorker("process", threads, (prev, next) -> {
        RenderableFeatures features = new RenderableFeatures();
        SourceFeature sourceFeature;
        while ((sourceFeature = prev.get()) != null) {
          featuresRead.incrementAndGet();
          features.reset(sourceFeature);
          if (sourceFeature.geometry().getEnvelopeInternal().intersects(env)) {
            profile.processFeature(sourceFeature, features);
            for (RenderableFeature renderable : features.all()) {
              renderer.renderFeature(renderable, next);
            }
          }
        }
      })
      .addBuffer("write_queue", 1000)
      .sinkToConsumer("write", 1, (item) -> {
        featuresWritten.incrementAndGet();
        writer.accept(item);
      });

    var loggers = new ProgressLoggers(name)
      .addRatePercentCounter("read", featureCount, featuresRead)
      .addRateCounter("write", featuresWritten)
      .addFileSize(writer::getStorageSize)
      .addProcessStats()
      .addTopologyStats(topology);

    topology.awaitAndLog(loggers, config.logInterval());
  }

  public abstract long getCount();

  public abstract Topology.SourceStep<SourceFeature> read();

  @Override
  public abstract void close();
}
