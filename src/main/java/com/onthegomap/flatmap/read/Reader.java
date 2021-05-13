package com.onthegomap.flatmap.read;

import com.onthegomap.flatmap.CommonParams;
import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.FeatureRenderer;
import com.onthegomap.flatmap.Profile;
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

  public final void process(String name, FeatureGroup writer, CommonParams config) {
    long featureCount = getCount();
    int threads = config.threads();
    Envelope latLonBounds = config.latLonBounds();
    AtomicLong featuresRead = new AtomicLong(0);
    AtomicLong featuresWritten = new AtomicLong(0);

    var topology = Topology.start(name, stats)
      .fromGenerator("read", read())
      .addBuffer("read_queue", 1000)
      .<FeatureSort.Entry>addWorker("process", threads, (prev, next) -> {
        SourceFeature sourceFeature;
        var featureCollectors = new FeatureCollector.Factory(config);
        var encoder = writer.newRenderedFeatureEncoder();
        FeatureRenderer renderer = new FeatureRenderer(
          config,
          rendered -> next.accept(encoder.apply(rendered))
        );
        while ((sourceFeature = prev.get()) != null) {
          featuresRead.incrementAndGet();
          FeatureCollector features = featureCollectors.get(sourceFeature);
          if (sourceFeature.latLonGeometry().getEnvelopeInternal().intersects(latLonBounds)) {
            profile.processFeature(sourceFeature, features);
            for (FeatureCollector.Feature<?> renderable : features) {
              renderer.renderFeature(renderable);
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
