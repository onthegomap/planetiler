package com.onthegomap.flatmap.reader;

import com.onthegomap.flatmap.FeatureRenderer;
import com.onthegomap.flatmap.Profile;
import com.onthegomap.flatmap.ProgressLoggers;
import com.onthegomap.flatmap.RenderableFeature;
import com.onthegomap.flatmap.RenderableFeatures;
import com.onthegomap.flatmap.SourceFeature;
import com.onthegomap.flatmap.stats.Stats;
import com.onthegomap.flatmap.worker.Worker;
import com.onthegomap.flatmap.worker.Worker.WorkerSource;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Reader {

  private final Stats stats;
  private final Envelope envelope;
  private Logger LOGGER = LoggerFactory.getLogger(getClass());

  public Reader(Stats stats, Envelope envelope) {
    this.stats = stats;
    this.envelope = envelope;
  }

  protected void log(String message) {
    LOGGER.info("[" + getName() + "] " + message);
  }

  protected abstract String getName();

  public final void process(FeatureRenderer renderer, Profile profile, int threads) {
    threads = Math.max(threads, 1);
    long featureCount = getCount();
    AtomicLong featuresRead = new AtomicLong(0);
    log("Reading with " + threads + " threads");
    try (
      var source = open(getName() + "-reader");
      var sink = renderer.newWriterQueue(getName() + "-writer")
    ) {
      var worker = new Worker(getName() + "-processor", stats, threads, i -> {
        SourceFeature sourceFeature;
        RenderableFeatures features = new RenderableFeatures();
        var sourceQueue = source.queue();
        while ((sourceFeature = sourceQueue.getNext()) != null) {
          featuresRead.incrementAndGet();
          features.reset(sourceFeature);
          if (sourceFeature.getGeometry().getEnvelopeInternal().intersects(envelope)) {
            profile.processFeature(sourceFeature, features);
            for (RenderableFeature renderable : features.all()) {
              renderer.renderFeature(renderable);
            }
          }
        }
      });

//      TODO:
//      -where should this go ?
//        -should the renderer hold a reusable feature writer / queue ?

      var loggers = new ProgressLoggers(getName())
        .addRatePercentCounter("read", featureCount, featuresRead)
        .addRateCounter("write", featuresWritten)
        .addFileSize(featureMap::getStorageSize)
        .addProcessStats()
        .addThreadPoolStats("read", getName() + "-reader")
        .addQueueStats(readerQueue)
        .addThreadPoolStats("process", worker)
        .addQueueStats(toWrite)
        .addThreadPoolStats("write", writer);

      worker.awaitAndLong(loggers);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected abstract long getCount();

  protected abstract WorkerSource<SourceFeature> open(String workerName);

}
