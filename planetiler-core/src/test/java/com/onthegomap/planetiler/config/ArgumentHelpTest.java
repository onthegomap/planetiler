package com.onthegomap.planetiler.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ArgumentHelpTest {

  @ParameterizedTest
  @CsvSource(nullValues = "null", value = {
    "planetiler, /path/to/planetiler.jar --area=monaco, null, planetiler",
    "planetiler, /path/to/planetiler.jar, openmaptiles, planetiler openmaptiles",
    "planetiler, jdk.compiler/com.sun.tools.javac.launcher.SourceLauncher Roads.java --area=monaco, null, planetiler Roads.java",
    "null, /path/to/planetiler.jar --area=monaco, null, java -jar planetiler.jar",
    "null, target/planetiler.jar, shortbread, java -jar planetiler.jar shortbread",
    "null, jdk.compiler/com.sun.tools.javac.launcher.SourceLauncher Roads.java, null, java -cp planetiler.jar Roads.java",
    "null, com.onthegomap.planetiler.Main --area=monaco, null, ''",
    "null, com.onthegomap.planetiler.Main openmaptiles --area=monaco, openmaptiles, openmaptiles",
    "null, com.example.MyProfile --area=monaco, null, java com.example.MyProfile",
  })
  void testPlanetilerCommand(String launcher, String javaCommand, String subcommand, String expected) {
    assertEquals(expected, ArgumentHelp.planetilerCommand(launcher, javaCommand, subcommand));
  }

  @Test
  void testAdvancedGroupsOnlyListedWithoutHelpAll() {
    var args = Arguments.of();
    args.withGroup(ArgumentGroup.OUTPUT_ADVANCED).getDouble("simplify_tolerance", "simplification tolerance", 0.1);
    args.withGroup(ArgumentGroup.OUTPUT_ADVANCED).getDouble("min_feature_size", "smallest feature", 1);
    args.withGroup(ArgumentGroup.OUTPUT).getInteger("maxzoom", "highest zoom level", 14);

    String help = ArgumentHelp.options(args.options(), "Profile options", Map.of(), false).render();
    assertTrue(help.contains("Output:\n  --maxzoom=<n>  highest zoom level [default: 14]\n"), help);
    assertFalse(help.contains("--simplify-tolerance"), help);
    assertTrue(help.endsWith("\nShow all options with --help-all\n"), help);

    String helpAll = ArgumentHelp.options(args.options(), "Profile options", Map.of(), true).render();
    assertTrue(helpAll.contains("""
      Output (advanced):
        --simplify-tolerance=<n>  simplification tolerance [default: 0.1]
        --min-feature-size=<n>    smallest feature [default: 1.0]
      """), helpAll);
    assertFalse(helpAll.contains("--help-all"), helpAll);
  }

  @Test
  void testProfileGroupsFirstThenCoreGroupsInFixedOrder() {
    var args = Arguments.of();
    args.withGroup(ArgumentGroup.LOGGING).getBoolean("verbose", "", false);
    args.withGroup("Layers").getBoolean("water", "", false);
    args.withGroup(ArgumentGroup.INPUT).getBoolean("refresh_sources", "", false);
    args.getBoolean("transliterate", "", false);

    String help = ArgumentHelp.options(args.options(), "Profile options", Map.of(), false).render();
    List<Integer> positions = List.of("Profile options:\n", "Layers:\n", "Input:\n", "Logging:\n").stream()
      .map(help::indexOf)
      .toList();
    assertEquals(positions.stream().sorted().toList(), positions, help);
    assertFalse(positions.contains(-1), help);
  }

  @Test
  void testLongListDefaultsShowACount() {
    var args = Arguments.of();
    args.getList("languages", "label languages", List.of("en", "fr", "de", "es", "it", "nl", "pl", "pt", "ru", "zh",
      "ja", "ko", "ar", "he"));
    String help = ArgumentHelp.options(args.options(), "Options", Map.of(), false).render();
    assertTrue(help.contains("label languages [default: 14 values]"), help);
  }

  @Test
  void testVerboseList() {
    var args = Arguments.fromArgs("--area=monaco");
    args.getString("area", "", null);
    args.withGroup(ArgumentGroup.PROCESSING).getInteger("threads", "", 4);
    args.withGroup(ArgumentGroup.PROCESSING).getString("a_very_long_option_name_that_does_not_fit", "", "value");
    args.getString("unset", "", null);
    args.getString("empty", "", "");
    assertEquals("""
      Option values:

      Profile options:
        --area=monaco  (command line)
        --unset
        --empty=''

      Processing:
        --threads=4
        --a-very-long-option-name-that-does-not-fit=value
      """, ArgumentHelp.verboseList(args.options(), "Profile options"));
  }

  @Test
  void testMemoryLimitListedUnderProcessing() {
    var args = Arguments.of();
    args.withGroup(ArgumentGroup.PROCESSING).getInteger("threads", "worker threads", 4);
    String help = ArgumentHelp.options(args.options(), "Options", Map.of(), false).render();
    assertTrue(
      help.startsWith("Processing:\n  --threads=<n>  worker threads [default: 4]\n  -Xmx<size>     Java memory"),
      help);
  }

  @Test
  void testNoMemoryLimitWithoutProcessingOptions() {
    var args = Arguments.of();
    args.getString("schema", "the schema to test", null);
    String help = ArgumentHelp.options(args.options(), "Options", Map.of(), true).render();
    assertFalse(help.contains("Xmx"), help);
  }

  @Test
  void testCommonOptionsFirstInTheirSection() {
    var args = Arguments.of();
    args.withGroup(ArgumentGroup.INPUT).getString("config", "", null);
    args.withGroup(ArgumentGroup.INPUT).getString("bounds", "", null);
    args.withGroup(ArgumentGroup.INPUT).getString("area", "", null);
    args.withGroup(ArgumentGroup.OUTPUT).getInteger("maxzoom", "", 14);
    args.withGroup(ArgumentGroup.OUTPUT).getString("output|-o", "", null);

    String help = ArgumentHelp.options(args.options(), "Options", Map.of(), false).render();
    List<Integer> positions = List.of("--area", "--bounds", "--config", "--output", "--maxzoom").stream()
      .map(help::indexOf)
      .toList();
    assertEquals(positions.stream().sorted().toList(), positions, help);
  }

  @Test
  void testCommandHelpListsOnlyItsOwnOptionsUnlessHelpAll() {
    var args = Arguments.of();
    args.getBoolean("strict", "fail on any diff", true);
    args.withGroup(ArgumentGroup.PROCESSING).getInteger("threads", "worker threads", 4);

    String help =
      ArgumentHelp.commandHelp(args.options(), "compare: compare two tile archives", "<a> <b>", false).render();
    assertTrue(help.startsWith(ArgumentHelp.title() + "\n\ncompare: compare two tile archives\n\nUsage: "), help);
    assertTrue(help.contains("""
      <a> <b>

      Options:
        --strict=<bool>  fail on any diff [default: true]

      Show all options with --help-all
      """), help);
    assertFalse(help.contains("--threads"), help);

    String helpAll = ArgumentHelp.commandHelp(args.options(), null, "<a> <b>", true).render();
    assertTrue(helpAll.contains("\nProcessing:\n  --threads=<n>"), helpAll);
    assertFalse(helpAll.contains("--help-all"), helpAll);
  }
}
