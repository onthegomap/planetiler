package com.onthegomap.flatmap.config;

import static org.junit.jupiter.api.Assertions.*;

import com.onthegomap.flatmap.TestUtils;
import com.onthegomap.flatmap.reader.osm.OsmInputFile;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Envelope;

public class ArgumentsTest {

  @Test
  public void testEmpty() {
    assertEquals("fallback", Arguments.of().get("key", "key", "fallback"));
  }

  @Test
  public void testMapBased() {
    assertEquals("value", Arguments.of(
      "key", "value"
    ).get("key", "key", "fallback"));
  }

  @Test
  public void testOrElse() {
    Arguments args = Arguments.of("key1", "value1a", "key2", "value2a")
      .orElse(Arguments.of("key2", "value2b", "key3", "value3b"));

    assertEquals("value1a", args.get("key1", "key", "fallback"));
    assertEquals("value2a", args.get("key2", "key", "fallback"));
    assertEquals("value3b", args.get("key3", "key", "fallback"));
    assertEquals("fallback", args.get("key4", "key", "fallback"));
  }

  @Test
  public void testConfigFileParsing() {
    Arguments args = Arguments.fromConfigFile(TestUtils.pathToResource("test.properties"));

    assertEquals("value1fromfile", args.get("key1", "key", "fallback"));
    assertEquals("fallback", args.get("key3", "key", "fallback"));
  }

  @Test
  public void testGetConfigFileFromArgs() {
    Arguments args = Arguments.fromArgsOrConfigFile(
      "config=" + TestUtils.pathToResource("test.properties"),
      "key2=value2fromargs"
    );

    assertEquals("value1fromfile", args.get("key1", "key", "fallback"));
    assertEquals("value2fromargs", args.get("key2", "key", "fallback"));
    assertEquals("fallback", args.get("key3", "key", "fallback"));
  }

  @Test
  public void testDefaultsMissingConfigFile() {
    Arguments args = Arguments.fromArgsOrConfigFile(
      "key=value"
    );

    assertEquals("value", args.get("key", "key", "fallback"));
    assertEquals("fallback", args.get("key2", "key", "fallback"));
  }

  @Test
  public void testDuration() {
    Arguments args = Arguments.of(
      "duration", "1h30m"
    );

    assertEquals(Duration.ofMinutes(90), args.duration("duration", "key", "10m"));
    assertEquals(Duration.ofSeconds(10), args.duration("duration2", "key", "10s"));
  }

  @Test
  public void testInteger() {
    Arguments args = Arguments.of(
      "integer", "30"
    );

    assertEquals(30, args.integer("integer", "key", 10));
    assertEquals(10, args.integer("integer2", "key", 10));
  }

  @Test
  public void testThreads() {
    assertEquals(2, Arguments.of("threads", "2").threads());
    assertTrue(Arguments.of().threads() > 0);
  }

  @Test
  public void testList() {
    assertEquals(List.of("1", "2", "3"),
      Arguments.of("list", "1,2,3").get("list", "list", List.of("1")));
    assertEquals(List.of("1"),
      Arguments.of().get("list", "list", List.of("1")));
  }

  @Test
  public void testBoolean() {
    assertTrue(Arguments.of("boolean", "true").get("boolean", "list", false));
    assertFalse(Arguments.of("boolean", "false").get("boolean", "list", true));
    assertFalse(Arguments.of("boolean", "true1").get("boolean", "list", true));
    assertFalse(Arguments.of().get("boolean", "list", false));
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
      Arguments.of("bounds", "1,2,3,4").bounds("bounds", "bounds", BoundsProvider.WORLD));
    assertEquals(new Envelope(-180.0, 180.0, -85.0511287798066, 85.0511287798066),
      Arguments.of("bounds", "world").bounds("bounds", "bounds", BoundsProvider.WORLD));
    assertEquals(new Envelope(7.409205, 7.448637, 43.72335, 43.75169),
      Arguments.of().bounds("bounds", "bounds", new OsmInputFile(TestUtils.pathToResource("monaco-latest.osm.pbf"))));
  }

  @Test
  public void testStats() {
    assertNotNull(Arguments.of().getStats());
  }
}
