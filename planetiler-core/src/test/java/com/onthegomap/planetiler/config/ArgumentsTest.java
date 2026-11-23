package com.onthegomap.planetiler.config;

import static org.junit.jupiter.api.Assertions.*;

import com.onthegomap.planetiler.ExpectedException;
import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.reader.osm.OsmInputFile;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
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
  void testWithFallback() {
    Arguments args = Arguments.of("key1", "value1a", "key2", "value2a")
      .withDefault("key2", "value2b")
      .withDefault("--key3", "value3b")
      .withDefault("key_5", true);

    assertEquals("value1a", args.getString("key1", "key", "fallback"));
    assertEquals("value2a", args.getString("key2", "key", "fallback"));
    assertEquals("value3b", args.getString("key3", "key", "fallback"));
    assertEquals("fallback", args.getString("key4", "key", "fallback"));
    assertTrue(args.getBoolean("key-5", "key", false));
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
    var invalid = Arguments.of("boolean", "true1");
    assertThrows(IllegalArgumentException.class, () -> invalid.getBoolean("boolean", "list", true));
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

  @Test
  void testWithPrefix() {
    var args = Arguments.of("prefix_a", "a_val", "prefix-b", "b_val", "other", "other_val").withPrefix("prefix");
    assertEquals("a_val", args.getArg("a"));
    assertEquals("b_val", args.getArg("b"));
    assertNull(args.getArg("other"));
    assertNull(args.getArg("prefix_a"));
    assertNull(args.getArg("prefix_b"));
    assertNull(args.getArg("prefix_other"));
    assertEquals(Set.of("a", "b"), args.toMap().keySet());
  }

  @Test
  void testPrefixFromEnvironment() {
    Map<String, String> env = Map.of(
      "OTHER", "value",
      "PLANETILEROTHER", "VALUE",
      "PLANETILER_MBTILES_KEY1", "value1",
      "PLANETILER_PMTILES_KEY2", "value2"
    );
    Arguments args = Arguments.fromEnvironment(env::get, env::keySet).withPrefix("mbtiles");
    assertEquals(Map.of(
      "key1", "value1"
    ), args.toMap());
    assertEquals("value1", args.getArg("key1"));
  }

  @Test
  void testSubset() {
    var args = Arguments.of(Map.of(
      "key_1", "val_1",
      "key-2", "val_2",
      "key-3", "val_3"
    )).subset("key-1", "key-2");
    assertEquals(Map.of(
      "key_1", "val_1",
      "key_2", "val_2"
    ), args.toMap());
    assertEquals("val_1", args.getArg("key-1"));
    assertNull(args.getArg("key-3"));
  }

  @Test
  void testFalseForInt() {
    var args = Arguments.of(Map.of(
      "true", "true",
      "false", "false",
      "3", "3"
    ));
    assertEquals(1, args.getInteger("true", "", 1));
    assertEquals(0, args.getInteger("false", "", 1));
    assertEquals(3, args.getInteger("3", "", 1));
  }

  @Test
  void testOptionsRecordGroupDefaultValueAndSource() {
    var args = Arguments.fromArgs("--threads=3", "--minzoom=2").orElse(Arguments.of("maxzoom", "12"));
    args.withGroup(ArgumentGroup.OUTPUT).getInteger("minzoom", "lowest zoom", 0);
    args.getInteger("maxzoom", "highest zoom", 14);
    args.withGroup(ArgumentGroup.OUTPUT).getBoolean("force|overwrite", "overwrite the output", false);
    args.getString("tile_format", "tile format", "mvt");

    assertEquals(List.of(
      Arguments.Option.builder()
        .name("minzoom")
        .description("lowest zoom")
        .group(ArgumentGroup.OUTPUT)
        .placeholder("<n>")
        .defaultValue("0")
        .value("2")
        .source("command line")
        .build(),
      Arguments.Option.builder()
        .name("maxzoom")
        .description("highest zoom")
        .placeholder("<n>")
        .defaultValue("14")
        .value("12")
        .source("default")
        .build(),
      Arguments.Option.builder()
        .name("force")
        .description("overwrite the output")
        .group(ArgumentGroup.OUTPUT)
        .defaultValue("false")
        .value("false")
        .source("default")
        .build(),
      Arguments.Option.builder()
        .name("tile-format")
        .description("tile format")
        .placeholder("<value>")
        .defaultValue("mvt")
        .value("mvt")
        .source("default")
        .build()
    ), args.options());
  }

  @Test
  void testOptionsListedOnceInTheOrderFirstRead() {
    var args = Arguments.of();
    args.getInteger("b", "first description", 1);
    args.getInteger("a", "", 2);
    args.getInteger("b", "second description", 3);
    assertEquals(List.of("b", "a"), args.options().stream().map(Arguments.Option::name).toList());
    assertEquals("first description", args.options().getFirst().description());
  }

  @Test
  void testOptionsSharedByDerivedViews() {
    var args = Arguments.of("a", "1");
    args.copy().getString("a", "", null);
    args.orElse(Arguments.of()).getString("b", "", null);
    args.subset("c").getString("c", "", null);
    args.withGroup("Profile group").getString("d", "", null);
    args.withPrefix("prefix").getString("e", "", null);
    args.copy().silence().getString("f", "", null);
    assertEquals(List.of("a", "b", "c", "d", "prefix-e"), args.options().stream().map(Arguments.Option::name).toList());
    assertEquals(ArgumentGroup.basic("Profile group"), args.options().get(3).group());
  }

  @Test
  void testOptionSources() {
    var args = Arguments.fromArgs("--a=1")
      .orElse(Arguments.fromJvmProperties(Map.of("planetiler.b", "2")::get, () -> List.of("planetiler.b")))
      .orElse(Arguments.fromEnvironment(Map.of("PLANETILER_C", "3")::get, () -> Set.of("PLANETILER_C")));
    for (var key : List.of("a", "b", "c", "d")) {
      args.getString(key, "", null);
    }
    assertEquals(List.of("command line", "JVM property", "environment", "default"),
      args.options().stream().map(Arguments.Option::source).toList());
  }

  @Test
  void testHelpText() {
    var args = Arguments.of();
    args.withGroup(ArgumentGroup.OUTPUT).getBoolean("force", "overwrite the output", false);
    args.getInteger("layers", "how many layers", 3);
    String help = HelpPage.create()
      .line("Usage: x")
      .append(ArgumentHelp.options(
        args.options(),
        "Profile options",
        Map.of(ArgumentGroup.OUTPUT, HelpPage.create().line("  Output notes")),
        true))
      .render();
    assertEquals("""
      Usage: x

      Profile options:
        --layers=<n>  how many layers [default: 3]

      Output:
        --force  overwrite the output
        Output notes
      """, help);
  }

  @Test
  void testCommandLineForms() {
    var args = Arguments.fromArgs(
      "openmaptiles minzoom=2 --area=monaco --maxzoom 12 --force --no-download -o out.pmtiles -- --not-an-option"
        .split(" "));
    assertEquals("monaco", args.getString("area", "", null));
    assertEquals(12, args.getInteger("maxzoom", "", 14));
    assertTrue(args.getBoolean("force", "", false));
    assertFalse(args.getBoolean("download", "", true));
    assertEquals(2, args.getInteger("minzoom", "", 0));
    assertEquals("out.pmtiles", args.getString("output|-o", "", null));
    assertEquals(List.of("openmaptiles", "--not-an-option"), args.positional());
  }

  @Test
  void testShortNameOnlyMatchesOneDash() {
    assertNull(Arguments.fromArgs("--o=out.pmtiles").getString("output|-o", "", null));
    assertEquals("x", Arguments.fromArgs("-o=x").getString("output|-o", "", null));
  }

  @Test
  void testShortNameCanOnlyStandForOneOption() {
    var args = Arguments.fromArgs("-o", "out.pmtiles");
    args.getString("output|-o", "", null);
    args.getString("output|-o", "", null);
    assertThrows(IllegalStateException.class, () -> args.getString("other|-o", "", null));
  }

  @ParameterizedTest
  @CsvSource({
    "true, true", "TRUE, true", "yes, true", "on, true", "1, true",
    "false, false", "no, false", "off, false", "0, false",
  })
  void testBooleanValues(String value, boolean expected) {
    assertEquals(expected, Arguments.of("flag", value).getBoolean("flag", "", !expected));
    assertEquals(expected, Arguments.of("flag", value).getBooleanObject("flag", ""));
  }

  @Test
  void testBadValuesNameTheOption() {
    var args = Arguments.fromArgs("--maxzoom=abc --force=maybe --loginterval=soon".split(" "));
    var maxzoom = assertThrows(IllegalArgumentException.class, () -> args.getInteger("maxzoom", "", 14));
    assertEquals("--maxzoom: expected a whole number, got \"abc\"", maxzoom.getMessage());
    var force = assertThrows(IllegalArgumentException.class, () -> args.getBoolean("force|-f", "", false));
    assertEquals("--force: expected true or false, got \"maybe\"", force.getMessage());
    var interval = assertThrows(IllegalArgumentException.class, () -> args.getDuration("loginterval", "", "10s"));
    assertEquals("--loginterval: expected a duration like 30s, 5m or 1h30m, got \"soon\"", interval.getMessage());
  }

  @Test
  void testHelpShowsEvenWithBadOrMissingValues() {
    var args = Arguments.fromArgs("--help --maxzoom=abc".split(" "));
    assertEquals(14, args.getInteger("maxzoom", "", 14));
    assertNull(args.getString("input", "required input"));
  }

  @Test
  void testUnknownOptions() {
    var args = Arguments.fromArgs("--maxzom=15 --area=monaco --mbtiles-compact=false shortbread".split(" "))
      .orElse(
        Arguments.fromEnvironment(Map.of("PLANETILER_UNRELATED", "x")::get, () -> Set.of("PLANETILER_UNRELATED")));
    args.getString("area", "", null);
    args.getInteger("maxzoom", "", 14);
    args.withPrefix("mbtiles").getBoolean("compact", "", true);
    assertEquals(List.of(
      "unknown option --maxzom (did you mean --maxzoom?)",
      "unexpected argument \"shortbread\" (the profile or command goes first, before its options)"
    ), args.unknownOptions());
    assertEquals(List.of(), args.unknownOptions());
  }

  @Test
  void testOverrides() {
    var args = Arguments.fromArgs("--mbtiles-compact=false --mbtiles-vacuum=true".split(" ")).withPrefix("mbtiles")
      .withOverrides(Map.of("compact", "true"), "output URI");
    assertTrue(args.getBoolean("compact", "", false));
    assertTrue(args.getBoolean("vacuum", "", false));
    assertEquals(List.of("output URI", "command line"), args.options().stream().map(Arguments.Option::source).toList());
    assertEquals(List.of("mbtiles-compact", "mbtiles-vacuum"),
      args.options().stream().map(Arguments.Option::name).toList());
  }

  @Test
  void testPrefixedViewWithFallbackRecordsPrefixedNames() {
    var args = Arguments.fromArgs("--compact-db=false");
    var options = args.withPrefix("mbtiles").orElse(args.subset("compact_db"));
    assertFalse(options.getBoolean("compact|compact_db", "", true));
    assertEquals(List.of("mbtiles-compact"), args.options().stream().map(Arguments.Option::name).toList());
    assertEquals(List.of(), args.unknownOptions());
  }

  @Test
  void testPlaceholder() {
    var args = Arguments.fromArgs("-o", "out.pmtiles");
    assertEquals("out.pmtiles", args.getString("output=<file>|old_output|-o", "where to write", null));
    args.getBoolean("force=<bool>", "overwrite", false);
    args.getString("name", "", null);
    var options = args.options();
    assertEquals(List.of("output", "o", "<file>"),
      List.of(options.get(0).name(), options.get(0).shortName(), options.get(0).placeholder()));
    assertNull(options.get(1).placeholder());
    assertEquals("<value>", options.get(2).placeholder());
  }

  @ParameterizedTest
  @ValueSource(strings = {"output=file", "output=<file", "=<file>", "output=<file>=<x>"})
  void testBadKey(String key) {
    var args = Arguments.of();
    assertThrows(IllegalStateException.class, () -> args.getString(key, "", null));
  }

  @Test
  void testBooleanBeforePositional() {
    var args = Arguments.fromArgs("--verbose", "a.pmtiles", "--strict", "false", "b.pmtiles");
    assertTrue(args.getBoolean("verbose", "", false));
    assertFalse(args.getBoolean("strict", "", true));
    assertEquals(List.of("a.pmtiles", "b.pmtiles"), args.positional());
    assertEquals(List.of(), args.unknownOptions());
  }

  @Test
  void testWordAfterStringOptionIsItsValue() {
    var args = Arguments.fromArgs("--output", "a.pmtiles", "b.pmtiles");
    assertEquals("a.pmtiles", args.getString("output", "", null));
    assertEquals(List.of("b.pmtiles"), args.positional());
  }

  @Test
  void testBadBooleanValueStillFails() {
    var args = Arguments.fromArgs("--verbose=a.pmtiles");
    assertThrows(IllegalArgumentException.class, () -> args.getBoolean("verbose", "", false));
  }

  @ParameterizedTest
  @CsvSource({
    "--help=false --maxzoom=abc, true",
    "--help openmaptiles --maxzoom=abc, false",
    "--help=true --maxzoom=abc, false",
  })
  void testOnlySkipErrorsWhenHelpShows(String commandLine, boolean fails) {
    var args = Arguments.fromArgs(commandLine.split(" "));
    if (fails) {
      assertThrows(IllegalArgumentException.class, () -> args.getInteger("maxzoom", "", 14));
    } else {
      assertEquals(14, args.getInteger("maxzoom", "", 14));
    }
  }

  @Test
  void testToMapDoesNotCountAsReadingOptions() {
    var args = Arguments.fromArgs("--maxzoomm=3");
    assertEquals(Map.of("maxzoomm", "3"), args.toMap());
    assertEquals(List.of("unknown option --maxzoomm"), args.unknownOptions());
  }

  @Test
  void testMissingRequiredOptionNamesIt() {
    var args = Arguments.of();
    var exception = assertThrows(IllegalArgumentException.class,
      () -> args.getString("input=<archive>|-i", "archive to read"));
    assertEquals("missing required parameter: --input (archive to read)", exception.getMessage());
  }

  @Test
  void testUnknownOptionsInConfigFile() {
    var args = Arguments.fromArgsOrConfigFile("config=" + TestUtils.pathToResource("test.properties"));
    args.getString("key1", "", null);
    args.getString("key3", "", null);
    assertEquals(List.of("unknown option --key2 (did you mean --key1?)"), args.unknownOptions());
  }
}
