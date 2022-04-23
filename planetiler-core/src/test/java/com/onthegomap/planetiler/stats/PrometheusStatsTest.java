package com.onthegomap.planetiler.stats;

import static io.prometheus.client.Collector.NANOSECONDS_PER_SECOND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.io.TempDir;

class PrometheusStatsTest {

  @TestFactory
  Stream<DynamicTest> testInitialStat() {
    PrometheusStats stats = new PrometheusStats("job");
    String metrics = stats.getMetricsAsString();
    return testContains(metrics,
      "^jvm_thread_cpu_time_seconds_total\\{",
      "^jvm_thread_user_time_seconds_total\\{",
      "^jvm_system_load_avg ",
      "^jvm_available_processors [0-9\\.]+$"
    );
  }

  @Test
  void testTimer() {
    PrometheusStats stats = new PrometheusStats("job");
    var timer = stats.startStage("task1");
    assertContainsStat("^planetiler_task1_running 1", stats);
    assertContainsStat("^planetiler_task1_elapsed_time_seconds [0-9\\.]+$", stats);
    assertContainsStat("^planetiler_task1_cpu_time_seconds [0-9\\.]+$", stats);
    timer.stop();

    assertContainsStat("^planetiler_task1_running 0", stats);
    assertContainsStat("^planetiler_task1_elapsed_time_seconds [0-9\\.]+$", stats);
    assertContainsStat("^planetiler_task1_cpu_time_seconds [0-9\\.]+$", stats);

    assertFalse(stats.timers().all().get("task1").timer().running());
  }

  @Test
  void testGauge() {
    PrometheusStats stats = new PrometheusStats("job");
    stats.gauge("gauge1", 1);
    stats.gauge("gauge2", () -> 2);
    assertContainsStat("^planetiler_gauge1 1", stats);
    assertContainsStat("^planetiler_gauge2 2", stats);
  }

  @Test
  void testProcessedElement() {
    PrometheusStats stats = new PrometheusStats("job");
    stats.processedElement("type1", "layer1");
    stats.processedElement("type1", "layer1");
    stats.processedElement("type1", "layer2");
    assertContainsStat("^planetiler_renderer_elements_processed_total\\{.*layer1.* 2", stats);
    assertContainsStat("^planetiler_renderer_elements_processed_total\\{.*layer2.* 1", stats);
  }

  @Test
  void testDataError() {
    PrometheusStats stats = new PrometheusStats("job");
    stats.dataError("err1");
    stats.dataError("err1");
    stats.dataError("err2");
    assertContainsStat("^planetiler_bad_input_data_total\\{.*err1.* 2", stats);
    assertContainsStat("^planetiler_bad_input_data_total\\{.*err2.* 1", stats);
  }

  @Test
  void testEmittedFeatures() {
    PrometheusStats stats = new PrometheusStats("job");
    stats.emittedFeatures(0, "layer1", 2);
    stats.emittedFeatures(0, "layer1", 2);
    stats.emittedFeatures(0, "layer2", 1);
    assertContainsStat("^planetiler_renderer_features_emitted_total\\{.*layer1.* 4", stats);
    assertContainsStat("^planetiler_renderer_features_emitted_total\\{.*layer2.* 1", stats);
  }

  @Test
  void testWroteTile() {
    PrometheusStats stats = new PrometheusStats("job");
    stats.wroteTile(0, 10);
    stats.wroteTile(0, 10_000);
    assertContainsStat("^planetiler_mbtiles_tile_written_bytes_bucket\\{.*le=\"1000\\..* 1", stats);
    assertContainsStat("^planetiler_mbtiles_tile_written_bytes_bucket\\{.*le=\"10000\\..* 2", stats);
  }

  @Test
  void testMonitorFile(@TempDir Path path) throws IOException {
    PrometheusStats stats = new PrometheusStats("job");
    stats.monitorFile("test", path);
    assertContainsStat("^planetiler_file_test_size_bytes 0", stats);

    Files.writeString(path.resolve("data"), "abc");
    assertContainsStat("^planetiler_file_test_size_bytes [0-9]", stats);
  }

  @Test
  void testMonitorInMemoryObject() {
    PrometheusStats stats = new PrometheusStats("job");
    stats.monitorInMemoryObject("test", () -> 10);
    assertContainsStat("^planetiler_heap_object_test_size_bytes 10", stats);
  }

  private static Counter.Readable counterAt(int num) {
    var result = Counter.newSingleThreadCounter();
    result.incBy(num);
    return result;
  }

  @Test
  void testCounter() {
    PrometheusStats stats = new PrometheusStats("job");
    stats.counter("counter1", () -> 1);
    stats.counter("counter2", "label", () -> Map.of(
      "value1", counterAt(1),
      "value2", counterAt(2)
    ));
    var longCounter = stats.longCounter("long");
    longCounter.incBy(100);
    assertEquals(100, longCounter.get());
    stats.nanoCounter("nanos").incBy((long) (NANOSECONDS_PER_SECOND / 2));
    assertContainsStat("^planetiler_counter1_total 1", stats);
    assertContainsStat("^planetiler_counter2_total\\{.*label=\"value1\".* 1", stats);
    assertContainsStat("^planetiler_counter2_total\\{.*label=\"value2\".* 2", stats);
    assertContainsStat("^planetiler_long_total 100", stats);
    assertContainsStat("^planetiler_nanos_total 0.5", stats);
  }

  private static Stream<DynamicTest> testContains(String stats, String... regexes) {
    return Stream.of(regexes).map(re -> dynamicTest(re, () -> assertContainsStat(re, stats)));
  }

  private static void assertContainsStat(String regex, PrometheusStats stats) {
    assertContainsStat(regex, stats.getMetricsAsString());
  }

  private static void assertContainsStat(String regex, String stats) {
    Pattern pattern = Pattern.compile(regex);
    for (String line : stats.split("[\r\n]+")) {
      if (!line.startsWith("#") && pattern.matcher(line).find()) {
        return;
      }
    }
    System.err.println(stats);
    fail("could not find " + regex);
  }
}
