package com.onthegomap.planetiler.config;

import static org.junit.jupiter.api.Assertions.*;

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.reader.osm.OsmInputFile;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Envelope;

public class ArgumentsTest {

  @Test
  public void testEmpty() {
    assertEquals("fallback", Arguments.of().getString("key", "key", "fallback"));
  }

  @Test
  public void testMapBased() {
    assertEquals("value", Arguments.of(
      "key", "value"
    ).getString("key", "key", "fallback"));
  }

  @Test
  public void testOrElse() {
    Arguments args = Arguments.of("key1", "value1a", "key2", "value2a")
      .orElse(Arguments.of("key2", "value2b", "key3", "value3b"));

    assertEquals("value1a", args.getString("key1", "key", "fallback"));
    assertEquals("value2a", args.getString("key2", "key", "fallback"));
    assertEquals("value3b", args.getString("key3", "key", "fallback"));
    assertEquals("fallback", args.getString("key4", "key", "fallback"));
  }

  @Test
  public void testConfigFileParsing() {
    Arguments args = Arguments.fromConfigFile(TestUtils.pathToResource("test.properties"));

    assertEquals("value1fromfile", args.getString("key1", "key", "fallback"));
    assertEquals("fallback", args.getString("key3", "key", "fallback"));
  }

  @Test
  public void testGetConfigFileFromArgs() {
    Arguments args = Arguments.fromArgsOrConfigFile(
      "config=" + TestUtils.pathToResource("test.properties"),
      "key2=value2fromargs"
    );

    assertEquals("value1fromfile", args.getString("key1", "key", "fallback"));
    assertEquals("value2fromargs", args.getString("key2", "key", "fallback"));
    assertEquals("fallback", args.getString("key3", "key", "fallback"));
  }

  @Test
  public void testDefaultsMissingConfigFile() {
    Arguments args = Arguments.fromArgsOrConfigFile(
      "key=value"
    );

    assertEquals("value", args.getString("key", "key", "fallback"));
    assertEquals("fallback", args.getString("key2", "key", "fallback"));
  }

  @Test
  public void testDuration() {
    Arguments args = Arguments.of(
      "duration", "1h30m"
    );

    assertEquals(Duration.ofMinutes(90), args.getDuration("duration", "key", "10m"));
    assertEquals(Duration.ofSeconds(10), args.getDuration("duration2", "key", "10s"));
  }

  @Test
  public void testInteger() {
    Arguments args = Arguments.of(
      "integer", "30"
    );

    assertEquals(30, args.getInteger("integer", "key", 10));
    assertEquals(10, args.getInteger("integer2", "key", 10));
  }

  @Test
  public void testLong() {
    long maxInt = Integer.MAX_VALUE;
    Arguments args = Arguments.of(
      "long", Long.toString(maxInt * 2)
    );

    assertEquals(maxInt * 2, args.getLong("long", "key", maxInt + 1L));
    assertEquals(maxInt + 1L, args.getLong("long2", "key", maxInt + 1L));
  }

  @Test
  public void testThreads() {
    assertEquals(2, Arguments.of("threads", "2").threads());
    assertTrue(Arguments.of().threads() > 0);
  }

  @Test
  public void testList() {
    assertEquals(List.of("1", "2", "3"),
      Arguments.of("list", "1,2,3").getList("list", "list", List.of("1")));
    assertEquals(List.of("1"),
      Arguments.of().getList("list", "list", List.of("1")));
  }

  @Test
  public void testBoolean() {
    assertTrue(Arguments.of("boolean", "true").getBoolean("boolean", "list", false));
    assertFalse(Arguments.of("boolean", "false").getBoolean("boolean", "list", true));
    assertFalse(Arguments.of("boolean", "true1").getBoolean("boolean", "list", true));
    assertFalse(Arguments.of().getBoolean("boolean", "list", false));
  }

  @Test
  public void testFile() {
    assertNotNull(
      Arguments.of("file", TestUtils.pathToResource("test.properties")).inputFile("file", "file", Path.of("")));
    assertThrows(IllegalArgumentException.class,
      () -> Arguments.of("file", TestUtils.pathToResource("test.Xproperties")).inputFile("file", "file", Path.of("")));
    assertNotNull(
      Arguments.of("file", TestUtils.pathToResource("test.Xproperties")).file("file", "file", Path.of("")));
  }

  @Test
  public void testBounds() {
    assertEquals(new Envelope(1, 3, 2, 4),
      new Bounds(Arguments.of("bounds", "1,2,3,4").bounds("bounds", "bounds")).latLon());
    assertEquals(new Envelope(-180.0, 180.0, -85.0511287798066, 85.0511287798066),
      new Bounds(Arguments.of("bounds", "world").bounds("bounds", "bounds")).latLon());
    assertEquals(new Envelope(7.409205, 7.448637, 43.72335, 43.75169),
      new Bounds(Arguments.of().bounds("bounds", "bounds"))
        .setFallbackProvider(new OsmInputFile(TestUtils.pathToResource("monaco-latest.osm.pbf")))
        .latLon());
  }

  @Test
  public void testStats() {
    assertNotNull(Arguments.of().getStats());
  }

  @Test
  public void testArgsKeyPresentImplies() {
    Arguments args = Arguments.fromArgs(
      "--force"
    );

    assertTrue(args.getBoolean("force", "force", false));
  }

  @Test
  public void testUnderscoreDashSame() {
    assertTrue(Arguments.fromArgs(
      "--force-down-load=true"
    ).getBoolean("force_down_load", "force", false));
    assertTrue(Arguments.fromArgs(
      "--force-download=true"
    ).getBoolean("force_download", "force", false));
    assertTrue(Arguments.fromArgs(
      "--force_download=true"
    ).getBoolean("force-download", "force", false));
    assertTrue(Arguments.fromArgs(
      "--force_download=true"
    ).getBoolean("force_download", "force", false));
  }
}
