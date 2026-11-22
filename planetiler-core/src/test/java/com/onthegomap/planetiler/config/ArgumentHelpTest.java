package com.onthegomap.planetiler.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
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
  void testCommand(String launcher, String javaCommand, String subcommand, String expected) {
    assertEquals(expected, ArgumentHelp.command(launcher, javaCommand, subcommand));
  }

  @Test
  void testAdvancedGroupsOnlyListedWithoutHelpAll() {
    var args = Arguments.of();
    args.withGroup(ArgumentGroup.OUTPUT_ADVANCED).getDouble("simplify_tolerance", "simplification tolerance", 0.1);
    args.withGroup(ArgumentGroup.OUTPUT_ADVANCED).getDouble("min_feature_size", "smallest feature", 1);
    args.withGroup(ArgumentGroup.OUTPUT).getInteger("maxzoom", "highest zoom level", 14);

    String help = ArgumentHelp.render(args.options(), "Profile options", Map.of(), false);
    assertTrue(help.contains("Output\n  --maxzoom=<n>  highest zoom level [default: 14]\n"), help);
    assertFalse(help.contains("--simplify-tolerance"), help);
    assertTrue(help.endsWith("\nShow all options with --help-all\n"), help);

    String helpAll = ArgumentHelp.render(args.options(), "Profile options", Map.of(), true);
    assertTrue(helpAll.contains("""
      Output (advanced)
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

    String help = ArgumentHelp.render(args.options(), "Profile options", Map.of(), false);
    List<Integer> positions = List.of("Profile options\n", "Layers\n", "Input\n", "Logging\n").stream()
      .map(help::indexOf)
      .toList();
    assertEquals(positions.stream().sorted().toList(), positions, help);
    assertFalse(positions.contains(-1), help);
  }

  @Test
  void testLongDescriptionsWrapAndLongFlagsGetTheirOwnLine() {
    var args = Arguments.of();
    args.getBoolean("a_very_long_option_name_that_does_not_fit_in_the_column", "short", false);
    args.getString("b", "word ".repeat(40).strip(), null);

    String help = ArgumentHelp.render(args.options(), "Options", Map.of(), false);
    assertTrue(
      help.contains("  --a-very-long-option-name-that-does-not-fit-in-the-column\n" + " ".repeat(38) + "short\n"),
      help);
    assertTrue(help.lines().allMatch(line -> line.length() <= 118), help);
  }

  @Test
  void testLongListDefaultsShowACount() {
    var args = Arguments.of();
    args.getList("languages", "label languages", List.of("en", "fr", "de", "es", "it", "nl", "pl", "pt", "ru", "zh",
      "ja", "ko", "ar", "he"));
    String help = ArgumentHelp.render(args.options(), "Options", Map.of(), false);
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

      Profile options
        --area=monaco  (command line)
        --unset
        --empty=''

      Processing
        --threads=4
        --a-very-long-option-name-that-does-not-fit=value
      """, ArgumentHelp.verboseList(args.options(), "Profile options"));
  }

  @Test
  void testMemoryLimitListedUnderProcessing() {
    String help = ArgumentHelp.render(Arguments.of().options(), "Options", Map.of(), false);
    assertTrue(help.startsWith("Processing\n  -Xmx<size>  Java memory limit"), help);
  }

  @Test
  void testVerboseListWrapsLongListsAfterCommas() {
    var args = Arguments.of();
    List<String> languages = Collections.nCopies(40, "abcd");
    args.getList("languages", "", languages);
    String list = ArgumentHelp.verboseList(args.options(), "Options");
    assertTrue(list.lines().allMatch(line -> line.length() <= 118), list);
    assertTrue(list.replaceAll("\n +", "").contains("--languages=" + String.join(",", languages) + "\n"), list);
  }
}
