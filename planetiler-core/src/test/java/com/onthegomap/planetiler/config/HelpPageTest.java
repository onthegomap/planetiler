package com.onthegomap.planetiler.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import org.junit.jupiter.api.Test;

class HelpPageTest {

  private static final String WORDS = "word ".repeat(30).strip();

  @Test
  void testSectionsAndAlignedColumns() {
    assertEquals("""
      Usage: x

      Section:
        a      first
        bbbbb  second

      Other:
        c  third
      """, HelpPage.create()
      .line("Usage: x")
      .section("Section")
      .row("a", "first")
      .row("bbbbb", "second")
      .section("Other")
      .row("c", "third")
      .render(100));
  }

  @Test
  void testNoBlankLineBeforeFirstSectionOrAfterBlank() {
    assertEquals("""
      A:
        x

      B:
      """, HelpPage.create().section("A").line("  x").line().section("B").render(100));
  }

  @Test
  void testLongFlagsLineUpWithShortFlags() {
    assertEquals("""
      Options:
            --area=<name>  region
        -o, --output       output
      """, HelpPage.create()
      .section("Options")
      .flag("--area=<name>", "region")
      .flag("-o", "--output", "output")
      .render(100));
  }

  @Test
  void testLastColumnWrapsUnderItself() {
    String rendered = HelpPage.create().row("stats", WORDS).render(60);
    assertTrue(rendered.startsWith("  stats  word word"), rendered);
    assertTrue(rendered.lines().skip(1).allMatch(line -> line.startsWith(" ".repeat(9) + "word")), rendered);
    assertTrue(rendered.lines().allMatch(line -> line.length() <= 60), rendered);
  }

  @Test
  void testLongFirstCellGetsItsOwnLineAndTheColumnStaysNarrow() {
    String flag = "--" + "x".repeat(40);
    String column = " ".repeat(38);
    assertEquals("""
      Options:
        --a%sshort
        %s
      %slong
      """.formatted(" ".repeat(33), flag, column), HelpPage.create()
      .section("Options")
      .row("--a", "short")
      .row(flag, "long")
      .render(100));
  }

  @Test
  void testLinesAndCommandsNeverWrap() {
    String rendered = HelpPage.create().line("Usage: " + WORDS).planetilerCommand(WORDS).render(60);
    assertEquals("Usage: " + WORDS + "\n  " + ArgumentHelp.planetilerCommand(WORDS) + "\n", rendered);
  }

  @Test
  void testValuesWrapOnlyAfterCommas() {
    String list = "--languages=" + String.join(",", Collections.nCopies(40, "abcd"));
    String rendered = HelpPage.create().row(list, "").render(100);
    assertTrue(rendered.startsWith("  --languages=abcd,"), rendered);
    assertTrue(rendered.lines().allMatch(line -> line.length() <= 100), rendered);
    assertTrue(rendered.lines().skip(1).allMatch(line -> line.startsWith("      abcd")), rendered);
    assertEquals(list, rendered.lines().map(String::strip).reduce("", String::concat));
  }

  @Test
  void testTableColumnsAndIndent() {
    assertEquals("""
        Sources  path
          a      data/a.pbf  host
      notes:
          --x  y
      """, HelpPage.create()
      .row("Sources", "path")
      .row("  a", "data/a.pbf", "host")
      .line("notes:")
      .table(4)
      .row("--x", "y")
      .render(100));
  }

  @Test
  void testShortFlagIndentOnlyInTablesWithShortFlags() {
    assertEquals("""
      A:
        -o, --output  out
            --force   force

      B:
        --verbose  logs
      """, HelpPage.create()
      .section("A")
      .flag("-o", "--output", "out")
      .flag("--force", "force")
      .section("B")
      .flag("--verbose", "logs")
      .render(100));
  }

  @Test
  void testThreeColumnsWithLastOneWrapping() {
    assertEquals("""
        a   x.pbf  one two three
                   four five six
        bb  y      z
      """, HelpPage.create()
      .row("a", "x.pbf", "one two three four five six")
      .row("bb", "y", "z")
      .render(30));
  }

  @Test
  void testEmptyCellsSkipped() {
    assertEquals("""
        a  b
        c     d
      """, HelpPage.create()
      .row("a", "b", "")
      .row("c", "", "d")
      .render(100));
  }
}
