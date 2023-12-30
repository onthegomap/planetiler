package com.onthegomap.planetiler.stats;

import com.onthegomap.planetiler.util.FileUtils;
import com.onthegomap.planetiler.util.MemoryEstimator;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.CounterMetricFamily;
import io.prometheus.client.GaugeMetricFamily;
import io.prometheus.client.Histogram;
import io.prometheus.client.exporter.BasicAuthHttpConnectionFactory;
import io.prometheus.client.exporter.PushGateway;
import io.prometheus.client.exporter.common.TextFormat;
import io.prometheus.client.hotspot.DefaultExports;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.net.URL;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Stats} implementation that pushes metrics to a <a href="https://prometheus.io/">prometheus</a> instance
 * through a <a href="https://github.com/prometheus/pushgateway">push gateway</a>.
 * <p>
 * See {@code grafana.json} for an example grafana dashboard you can use to monitor progress.
 */
class PrometheusStats implements Stats {

  private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusStats.class);

  private final CollectorRegistry registry = new CollectorRegistry();
  private final Timers timers = new Timers();
  private static final String BASE = "planetiler_";
  private PushGateway pg;
  private ScheduledExecutorService executor;
  private final String job;
  private final Map<String, Path> filesToMonitor = new ConcurrentSkipListMap<>();
  private final Map<String, LongSupplier> sizesOfFilesToMonitor = new ConcurrentSkipListMap<>();
  private final Map<String, Long> dataErrorCounters = new ConcurrentHashMap<>();
  private final Map<String, MemoryEstimator.HasEstimate> heapObjectsToMonitor = new ConcurrentSkipListMap<>();

  /** Constructs a new instance but does not start polling (for tests). */
  PrometheusStats(String job) {
    this.job = job;
    DefaultExports.register(registry);
    new ThreadDetailsExports().register(registry);
    new InProgressTasks().register(registry);
    new FileSizeCollector().register(registry);
    new HeapObjectSizeCollector().register(registry);
    new PostGcMemoryCollector().register(registry);
  }

  private PrometheusStats(String destination, String job, Duration interval) {
    this(job);
    try {
      URL url = new URL(destination);
      pg = new PushGateway(url);
      if (url.getUserInfo() != null) {
        String[] parts = url.getUserInfo().split(":");
        if (parts.length == 2) {
          pg.setConnectionFactory(new BasicAuthHttpConnectionFactory(parts[0], parts[1]));
        }
      }
      this.push();
      executor = Executors.newScheduledThreadPool(1, r -> {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        thread.setName("prometheus-pusher");
        return thread;
      });
      executor.scheduleAtFixedRate(this::push, 0, Math.max(interval.getSeconds(), 5), TimeUnit.SECONDS);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns a new {@code PrometheusStats} that and schedules it to push to {@code destination} every {@code interval}.
   */
  static PrometheusStats createAndStartPushing(String destination, String job, Duration interval) {
    return new PrometheusStats(destination, job, interval);
  }

  private void push() {
    try {
      pg.push(registry, job);
    } catch (IOException e) {
      LOGGER.error("Error pushing stats to prometheus", e);
    }
  }

  @Override
  public void gauge(String name, Supplier<Number> value) {
    new Collector() {
      @Override
      public List<MetricFamilySamples> collect() {
        return List.of(new GaugeMetricFamily(BASE + sanitizeMetricName(name), "", value.get().doubleValue()));
      }
    }.register(registry);
  }

  private final io.prometheus.client.Counter processedElements = io.prometheus.client.Counter
    .build(BASE + "renderer_elements_processed", "Number of source elements processed")
    .labelNames("type", "layer")
    .register(registry);

  @Override
  public void processedElement(String elemType, String layer) {
    processedElements.labels(elemType, layer).inc();
  }

  private final io.prometheus.client.Counter dataErrors = io.prometheus.client.Counter
    .build(BASE + "bad_input_data", "Number of data inconsistencies encountered in source data")
    .labelNames("type")
    .register(registry);

  @Override
  public void dataError(String errorCode) {
    Stats.super.dataError(errorCode);
    dataErrors.labels(errorCode).inc();
  }

  private final io.prometheus.client.Counter emittedFeatures = io.prometheus.client.Counter
    .build(BASE + "renderer_features_emitted", "Features enqueued for writing to feature DB")
    .labelNames("zoom", "layer")
    .register(registry);

  @Override
  public void emittedFeatures(int z, String layer, int numFeatures) {
    emittedFeatures.labels(Integer.toString(z), layer).inc(numFeatures);
  }

  /** Returns the full payload that we would send to push gateway for a poll right way. */
  public String getMetricsAsString() {
    try (StringWriter writer = new StringWriter()) {
      TextFormat.write004(writer, registry.metricFamilySamples());
      return writer.toString();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private final Histogram tilesWrittenBytes = Histogram
    .build(BASE + "archive_tile_written_bytes", "Written tile sizes by zoom level")
    .buckets(1_000, 10_000, 100_000, 500_000)
    .labelNames("zoom")
    .register(registry);

  @Override
  public void wroteTile(int zoom, int bytes) {
    tilesWrittenBytes.labels(Integer.toString(zoom)).observe(bytes);
  }

  @Override
  public Timers timers() {
    return timers;
  }

  @Override
  public Map<String, Path> monitoredFiles() {
    return filesToMonitor;
  }

  @Override
  public Map<String, LongSupplier> monitoredFileSizes() {
    return sizesOfFilesToMonitor;
  }

  @Override
  public void monitorInMemoryObject(String name, MemoryEstimator.HasEstimate object) {
    heapObjectsToMonitor.put(name, object);
  }

  @Override
  public void counter(String name, Supplier<Number> supplier) {
    new Collector() {
      @Override
      public List<MetricFamilySamples> collect() {
        return List.of(new CounterMetricFamily(BASE + sanitizeMetricName(name), "", supplier.get().doubleValue()));
      }
    }.register(registry);
  }

  @Override
  public void counter(String name, String label, Supplier<Map<String, LongSupplier>> values) {
    new Collector() {
      @Override
      public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> result = new ArrayList<>();
        CounterMetricFamily family = new CounterMetricFamily(BASE + sanitizeMetricName(name), "", List.of(label));
        result.add(family);
        for (var entry : values.get().entrySet()) {
          family.addMetric(List.of(entry.getKey()), entry.getValue().getAsLong());
        }
        return result;
      }
    }.register(registry);
  }

  @Override
  public Map<String, Long> dataErrors() {
    return dataErrorCounters;
  }

  @Override
  public void close() {
    executor.shutdown();
    push();
  }

  private static GaugeMetricFamily gaugeMetric(String name, double value) {
    return new GaugeMetricFamily(BASE + name, BASE + name + " value", value);
  }

  /** Reports stats on all tasks being timed through {@link #timers()}. */
  private class InProgressTasks extends Collector {

    @Override
    public List<MetricFamilySamples> collect() {
      List<MetricFamilySamples> result = new ArrayList<>();
      for (var entry : timers.all().entrySet()) {
        String name = entry.getKey();
        Timer timer = entry.getValue().timer();
        result.add(gaugeMetric(name + "_running", timer.running() ? 1 : 0));
        ProcessTime time = timer.elapsed();
        result.add(gaugeMetric(name + "_elapsed_time_seconds", time.wall().toNanos() / NANOSECONDS_PER_SECOND));
        result.add(gaugeMetric(name + "_cpu_time_seconds",
          time.cpu().orElse(Duration.ZERO).toNanos() / NANOSECONDS_PER_SECOND));
      }
      return result;
    }
  }

  /** Reports stats on all file sizes being monitored through {@link #monitorFile(String, Path)}. */
  private class FileSizeCollector extends Collector {

    private boolean logged = false;

    @Override
    public List<MetricFamilySamples> collect() {
      List<Collector.MetricFamilySamples> results = new ArrayList<>();
      for (var file : filesToMonitor.entrySet()) {
        String name = sanitizeMetricName(file.getKey());
        Path path = file.getValue();
        var sizeSupplier = monitoredFileSizes().getOrDefault(file.getKey(), () -> FileUtils.size(path));
        long size = sizeSupplier.getAsLong();
        results.add(new GaugeMetricFamily(BASE + "file_" + name + "_size_bytes", "Size of " + name + " in bytes",
          size));
        if (Files.exists(path)) {
          try {
            FileStore fileStore = Files.getFileStore(path);
            results
              .add(
                new GaugeMetricFamily(BASE + "file_" + name + "_total_space_bytes", "Total space available on disk",
                  fileStore.getTotalSpace()));
            results.add(
              new GaugeMetricFamily(BASE + "file_" + name + "_unallocated_space_bytes", "Unallocated space on disk",
                fileStore.getUnallocatedSpace()));
            results
              .add(new GaugeMetricFamily(BASE + "file_" + name + "_usable_space_bytes", "Usable space on disk",
                fileStore.getUsableSpace()));
          } catch (IOException e) {
            // let the user know once
            if (!logged) {
              LOGGER.warn("unable to get usable space on device", e);
              logged = true;
            }
          }
        }
      }
      return results;
    }
  }

  /**
   * Reports stats on all in-memory objects sizes being monitored through
   * {@link #monitorInMemoryObject(String, MemoryEstimator.HasEstimate)}.
   */
  private class HeapObjectSizeCollector extends Collector {

    @Override
    public List<MetricFamilySamples> collect() {
      List<Collector.MetricFamilySamples> results = new ArrayList<>();
      for (var entry : heapObjectsToMonitor.entrySet()) {
        String name = sanitizeMetricName(entry.getKey());
        MemoryEstimator.HasEstimate heapObject = entry.getValue();
        results
          .add(new GaugeMetricFamily(BASE + "heap_object_" + name + "_size_bytes", "Bytes of memory used by " + name,
            heapObject.estimateMemoryUsageBytes()));
      }
      return results;
    }
  }

  /** Reports stats on post-GC memory consumption of each memory pool. */
  private static class PostGcMemoryCollector extends Collector {

    @Override
    public List<MetricFamilySamples> collect() {
      GaugeMetricFamily postGcPoolSizes = new GaugeMetricFamily(
        "jvm_memory_pool_post_gc_bytes_total",
        "Memory used by each pool after last GC",
        List.of("pool")
      );
      for (var entry : ProcessInfo.getPostGcPoolSizes().entrySet()) {
        postGcPoolSizes.addMetric(List.of(entry.getKey()), entry.getValue());
      }
      return List.of(postGcPoolSizes);
    }
  }

  /** Reports more detailed stats on CPU usage statistics by thread than prometheus collects by default. */
  private static class ThreadDetailsExports extends Collector {

    private final OperatingSystemMXBean osBean;

    public ThreadDetailsExports() {
      this.osBean = ManagementFactory.getOperatingSystemMXBean();
    }

    private final Map<Long, ProcessInfo.ThreadState> threads = new ConcurrentSkipListMap<>();

    public List<MetricFamilySamples> collect() {

      List<MetricFamilySamples> mfs = new ArrayList<>(List.of(
        new GaugeMetricFamily("jvm_available_processors", "Result of Runtime.getRuntime().availableProcessors()",
          Runtime.getRuntime().availableProcessors()),
        new GaugeMetricFamily("jvm_system_load_avg", "Result of OperatingSystemMXBean.getSystemLoadAverage()",
          osBean.getSystemLoadAverage())
      ));

      CounterMetricFamily threadCpuTimes = new CounterMetricFamily("jvm_thread_cpu_time_seconds",
        "CPU time used by each thread", List.of("name", "id"));
      mfs.add(threadCpuTimes);
      CounterMetricFamily threadUserTimes = new CounterMetricFamily("jvm_thread_user_time_seconds",
        "User time used by each thread", List.of("name", "id"));
      mfs.add(threadUserTimes);
      threads.putAll(ProcessInfo.getThreadStats());
      for (ProcessInfo.ThreadState thread : threads.values()) {
        var labels = List.of(thread.name(), Long.toString(thread.id()));
        threadUserTimes.addMetric(labels, thread.userTime().toNanos() / NANOSECONDS_PER_SECOND);
        threadCpuTimes.addMetric(labels, thread.cpuTime().toNanos() / NANOSECONDS_PER_SECOND);
      }

      return mfs;
    }
  }
}
