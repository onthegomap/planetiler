package com.onthegomap.planetiler.config;

import static org.junit.jupiter.api.Assertions.*;

import com.onthegomap.planetiler.ExpectedException;
import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.reader.osm.OsmInputFile;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Envelope;

class ArgumentsTest {

  @Test
  void testEmpty() {
    assertEquals("fallback", Arguments.of().getString("key", "key", "fallback"));
  }

  @Test
  void testMapBased() {
    assertEquals("value", Arguments.of(
      "key", "value"
    ).getString("key", "key", "fallback"));
  }

  @Test
  void testOrElse() {
    Arguments args = Arguments.of("key1", "value1a", "key2", "value2a")
      .orElse(Arguments.of("key2", "value2b", "key3", "value3b"));

    assertEquals("value1a", args.getString("key1", "key", "fallback"));
    assertEquals("value2a", args.getString("key2", "key", "fallback"));
    assertEquals("value3b", args.getString("key3", "key", "fallback"));
    assertEquals("fallback", args.getString("key4", "key", "fallback"));
  }

  @Test
  void testConfigFileParsing() {
    Arguments args = Arguments.fromConfigFile(TestUtils.pathToResource("test.properties"));

    assertEquals("value1fromfile", args.getString("key1", "key", "fallback"));
    assertEquals("fallback", args.getString("key3", "key", "fallback"));
  }

  @Test
  void testGetConfigFileFromArgs() {
    Arguments args = Arguments.fromArgsOrConfigFile(
      "config=" + TestUtils.pathToResource("test.properties"),
      "key2=value2fromargs"
    );

    assertEquals("value1fromfile", args.getString("key1", "key", "fallback"));
    assertEquals("value2fromargs", args.getString("key2", "key", "fallback"));
    assertEquals("fallback", args.getString("key3", "key", "fallback"));
  }

  @Test
  void testDefaultsMissingConfigFile() {
    Arguments args = Arguments.fromArgsOrConfigFile(
      "key=value"
    );

    assertEquals("value", args.getString("key", "key", "fallback"));
    assertEquals("fallback", args.getString("key2", "key", "fallback"));
  }

  @Test
  void testDuration() {
    Arguments args = Arguments.of(
      "duration", "1h30m"
    );

    assertEquals(Duration.ofMinutes(90), args.getDuration("duration", "key", "10m"));
    assertEquals(Duration.ofSeconds(10), args.getDuration("duration2", "key", "10s"));
  }

  @Test
  void testInteger() {
    Arguments args = Arguments.of(
      "integer", "30"
    );

    assertEquals(30, args.getInteger("integer", "key", 10));
    assertEquals(10, args.getInteger("integer2", "key", 10));
  }

  @Test
  void testLong() {
    long maxInt = Integer.MAX_VALUE;
    Arguments args = Arguments.of(
      "long", Long.toString(maxInt * 2)
    );

    assertEquals(maxInt * 2, args.getLong("long", "key", maxInt + 1L));
    assertEquals(maxInt + 1L, args.getLong("long2", "key", maxInt + 1L));
  }

  @Test
  void testThreads() {
    assertEquals(2, Arguments.of("threads", "2").threads());
    assertTrue(Arguments.of().threads() > 0);
  }

  @Test
  void testList() {
    assertEquals(List.of("1", "2", "3"),
      Arguments.of("list", "1,2,3").getList("list", "list", List.of("1")));
    assertEquals(List.of("1"),
      Arguments.of().getList("list", "list", List.of("1")));
  }

  @Test
  void testBoolean() {
    assertTrue(Arguments.of("boolean", "true").getBoolean("boolean", "list", false));
    assertFalse(Arguments.of("boolean", "false").getBoolean("boolean", "list", true));
    assertFalse(Arguments.of("boolean", "true1").getBoolean("boolean", "list", true));
    assertFalse(Arguments.of().getBoolean("boolean", "list", false));
  }

  @Test
  void testFile() {
    assertNotNull(
      Arguments.of("file", TestUtils.pathToResource("test.properties")).inputFile("file", "file", Path.of("")));
    assertThrows(IllegalArgumentException.class,
      () -> Arguments.of("file", TestUtils.pathToResource("test.Xproperties")).inputFile("file", "file", Path.of("")));
    assertNotNull(
      Arguments.of("file", TestUtils.pathToResource("test.Xproperties")).file("file", "file", Path.of("")));
  }

  @Test
  void testBounds() {
    assertEquals(new Envelope(1, 3, 2, 4),
      new Bounds(Arguments.of("bounds", "1,2,3,4").bounds("bounds", "bounds")).latLon());
    assertEquals(new Envelope(-180.0, 180.0, -85.0511287798066, 85.0511287798066),
      new Bounds(Arguments.of("bounds", "world").bounds("bounds", "bounds")).latLon());
    assertEquals(new Envelope(7.409205, 7.448637, 43.72335, 43.75169),
      new Bounds(Arguments.of().bounds("bounds", "bounds"))
        .addFallbackProvider(new OsmInputFile(TestUtils.pathToResource("monaco-latest.osm.pbf")))
        .latLon());
  }

  @Test
  void testStats() {
    assertNotNull(Arguments.of().getStats());
  }

  @Test
  void testArgsKeyPresentImplies() {
    Arguments args = Arguments.fromArgs(
      "--force"
    );

    assertTrue(args.getBoolean("force", "force", false));
  }

  @Test
  void testUnderscoreDashSame() {
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

  @Test
  void testSpaceBetweenArgs() {
    Arguments args = Arguments.fromArgs(
      "--key value --key2 value2 --force1 --force2".split("\\s+")
    );

    assertEquals("value", args.getString("key", "key", null));
    assertEquals("value2", args.getString("key2", "key2", null));
    assertTrue(args.getBoolean("force1", "force1", false));
    assertTrue(args.getBoolean("force2", "force2", false));
  }

  @Test
  void testListArgumentValuesFromCommandLine() {
    assertEquals(Map.of(), Arguments.fromArgs().toMap());
    assertEquals(Map.of(
      "key", "value",
      "key2", "value2",
      "force1", "true",
      "force2", "true"
    ), Arguments.fromArgs(
      "--key value --key2 value2 --force1 --force2".split("\\s+")
    ).toMap());
  }

  @Test
  void testListArgumentValuesFromMap() {
    assertEquals(Map.of(), Arguments.of(Map.of()).toMap());
    assertEquals(Map.of("a", "1", "b", "2"), Arguments.of(Map.of("a", "1", "b", "2")).toMap());
  }

  @Test
  void testListArgumentValuesFromConfigFile() {
    Arguments args = Arguments.fromConfigFile(TestUtils.pathToResource("test.properties"));
    assertEquals(Map.of("key1", "value1fromfile", "key2", "value2fromfile"), args.toMap());
  }

  @Test
  void testListArgumentsFromEnvironment() {
    Map<String, String> env = Map.of(
      "OTHER", "value",
      "PLANETILEROTHER", "VALUE",
      "PLANETILER_KEY1", "value1",
      "PLANETILER_KEY2", "value2"
    );
    Arguments args = Arguments.fromEnvironment(env::get, env::keySet);
    assertEquals(Map.of(
      "key1", "value1",
      "key2", "value2"
    ), args.toMap());
  }

  @Test
  void testListArgumentsFromJvmProperties() {
    Map<String, String> jvm = Map.of(
      "OTHER", "value",
      "PLANETILEROTHER", "VALUE",
      "PLANETILER_KEY1", "value1",
      "PLANETILER_KEY2", "value2",
      "planetiler.key3", "value4"
    );
    Arguments args = Arguments.fromJvmProperties(jvm::get, jvm::keySet);
    assertEquals(Map.of(
      "key3", "value4"
    ), args.toMap());
  }

  @Test
  void testListArgumentsFromMerged() {
    Map<String, String> env = Map.of(
      "OTHER", "value",
      "PLANETILEROTHER", "VALUE",
      "PLANETILER_KEY1", "value1",
      "PLANETILER_KEY2", "value2",
      "planetiler.key3", "value3"
    );
    Map<String, String> jvm = Map.of(
      "other", "value",
      "PLANETILEROTHER", "VALUE",
      "PLANETILER_KEY1", "value1",
      "PLANETILER_KEY2", "value2",
      "planetiler.key3", "value4"
    );
    Arguments args = Arguments.fromJvmProperties(jvm::get, jvm::keySet)
      .orElse(Arguments.fromEnvironment(env::get, env::keySet));
    assertEquals(Map.of(
      "key3", "value4",
      "key1", "value1",
      "key2", "value2"
    ), args.toMap());
  }

  @Test
  void testDontAccessArgListUntilUsed() {
    Map<String, String> env = Map.of(
      "OTHER", "value",
      "PLANETILEROTHER", "VALUE",
      "PLANETILER_KEY1", "value1",
      "PLANETILER_KEY2", "value2",
      "planetiler.key3", "value3"
    );
    Arguments args = Arguments.fromEnvironment(env::get, () -> {
      throw new ExpectedException();
    });
    assertEquals("value1", args.getString("key1", ""));
    assertThrows(ExpectedException.class, args::toMap);
  }

  @Test
  void testBooleanObject() {
    Map<String, String> env = Map.of(
      "BOOL_TRUE", "true",
      "BOOL_FALSE", "false",
      "BOOL_NO", "no"
    );
    Arguments args = Arguments.of(env);
    assertNull(args.getBooleanObject("BOOL_NULL", "test"));
    assertEquals(true, args.getBooleanObject("BOOL_TRUE", "test"));
    assertEquals(false, args.getBooleanObject("BOOL_FALSE", "test"));
    assertEquals(false, args.getBooleanObject("BOOL_NO", "test"));
  }

  @Test
  void testDeprecatedArgs() {
    assertEquals("newvalue",
      Arguments.of("oldkey", "oldvalue", "newkey", "newvalue")
        .getString("newkey|oldkey", "key", "fallback"));
    assertEquals("oldvalue",
      Arguments.of("oldkey", "oldvalue")
        .getString("newkey|oldkey", "key", "fallback"));
    assertEquals("fallback",
      Arguments.of()
        .getString("newkey|oldkey", "key", "fallback"));
  }
}
