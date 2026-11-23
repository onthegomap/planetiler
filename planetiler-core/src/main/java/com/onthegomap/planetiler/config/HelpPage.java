package com.onthegomap.planetiler.config;

import com.onthegomap.planetiler.util.Terminal;
import java.util.ArrayList;
import java.util.List;

/**
 * Text for {@code --help} and similar output, built from lines, section headings and tables, then laid out for the
 * terminal when rendered.
 * <p>
 * Lines are never wrapped, so commands people copy stay in one piece (the terminal soft-wraps them instead). Rows of a
 * table line up in columns, and the last column wraps under itself.
 * {@snippet :
 * String help = HelpPage.create()
 *   .usage("[options]")
 *   .section("Options")
 *   .flag("--area=<name>", "region to build")
 *   .flag("-o", "--output=<file>", "where to write the tiles")
 *   .render();
 * }
 */
public final class HelpPage {

  // The width when output doesn't go to a terminal, so piped help and tests don't depend on the terminal size.
  private static final int DEFAULT_WIDTH = 100;
  private static final int MIN_WIDTH = 60;
  private static final int MAX_WIDTH = 120;
  // A first column wider than this goes on its own line, so one long flag doesn't push every description right.
  private static final int MAX_SECOND_COLUMN_START = 38;
  private static final int GAP = 2;
  private static final int TABLE_INDENT = 2;

  private final List<Block> blocks = new ArrayList<>();

  private HelpPage() {}

  /** Returns the start of a new help page. */
  public static HelpPage create() {
    return new HelpPage();
  }

  /**
   * Adds {@code line} to {@code result} and clears it. A line with only a first cell, like a flag and its value in
   * {@code --verbose}, can only break after commas (e.g. in a list of languages).
   */
  private static void endLine(StringBuilder line, boolean onlyFirstCell, int valueIndent, int width,
    StringBuilder result) {
    String text = line.toString().stripTrailing();
    result.append(onlyFirstCell ? wrapAfterCommas(text, valueIndent, width) : text).append('\n');
    line.setLength(0);
  }

  private static List<String> cells(Row row, boolean anyShortFlags) {
    if (!anyShortFlags) {
      return row.cells();
    }
    List<String> result = new ArrayList<>(row.cells());
    result.set(0, (row.shortFlag() == null ? "    " : row.shortFlag() + ", ") + result.getFirst());
    return result;
  }

  private static int[] columnStarts(int indent, List<List<String>> rows) {
    int columns = rows.stream().mapToInt(List::size).max().orElse(0);
    int[] result = new int[columns];
    result[0] = indent;
    for (int i = 1; i < columns; i++) {
      result[i] = result[i - 1] + widestCell(rows, i - 1) + GAP;
    }
    if (columns > 1) {
      result[1] = Math.min(result[1], MAX_SECOND_COLUMN_START);
    }
    return result;
  }

  /** Returns the width of the widest cell in {@code column}, counting only rows with cells after it. */
  private static int widestCell(List<List<String>> rows, int column) {
    return rows.stream()
      .filter(cells -> cells.size() > column + 1)
      .mapToInt(cells -> cells.get(column).length())
      .max()
      .orElse(0);
  }

  /** Adds the words of {@code text} to {@code line}, which is filled up to {@code column}, wrapping under column. */
  private static void appendWrapped(String text, int column, int width, StringBuilder line, StringBuilder result) {
    for (String word : text.split(" ")) {
      if (line.length() > column && line.length() + 1 + word.length() > width) {
        result.append(line).append('\n');
        line.setLength(0);
        line.append(" ".repeat(column));
      }
      if (line.length() > column) {
        line.append(' ');
      }
      line.append(word);
    }
  }

  /** Wraps a cell that is a single value, like a long comma-separated list, only after its commas. */
  private static String wrapAfterCommas(String text, int rest, int width) {
    if (text.length() <= width) {
      return text;
    }
    StringBuilder result = new StringBuilder();
    StringBuilder line = new StringBuilder();
    for (String part : text.split("(?<=,)")) {
      if (line.length() > rest && line.length() + part.length() > width) {
        result.append(line).append('\n');
        line = new StringBuilder(" ".repeat(rest));
      }
      line.append(part);
    }
    return result.append(line).toString();
  }

  /** Adds a line as it is, never wrapped. */
  public HelpPage line(String text) {
    blocks.add(new Line(text));
    return this;
  }

  /** Adds a line with "Usage: planetiler {@code args}". */
  public HelpPage usage(String args) {
    return line("Usage: " + ArgumentHelp.planetilerCommand(args));
  }

  /** Adds an empty line. */
  public HelpPage line() {
    return line("");
  }

  /** Adds a planetiler command with {@code planetilerArgs} that people may copy, indented and never wrapped. */
  public HelpPage planetilerCommand(String planetilerArgs) {
    return command(ArgumentHelp.planetilerCommand(planetilerArgs));
  }

  /** Adds a command people may copy, indented and never wrapped. */
  public HelpPage command(String command) {
    return line(" ".repeat(TABLE_INDENT) + command);
  }

  /** Adds {@code Heading:}, after an empty line unless it is the first thing on the page. */
  public HelpPage section(String heading) {
    blocks.add(new Heading(heading));
    return this;
  }

  /** Starts a new table at {@code indent} for the rows that follow, instead of adding them to the one above. */
  public HelpPage table(int indent) {
    blocks.add(new Table(indent, new ArrayList<>()));
    return this;
  }

  /**
   * Adds a row to the table above (or a new one): each cell starts at the same column as the cells above and below it,
   * and the last cell wraps under itself.
   */
  public HelpPage row(String... cells) {
    return addRow(new Row(null, List.of(cells)));
  }

  /**
   * Adds a row for an option: its flag (with its one-letter form like {@code -o}, or {@code null}) then its
   * description. Flags without a one-letter form are indented in tables that have some, so every {@code --} lines up.
   */
  public HelpPage flag(String shortFlag, String flag, String description) {
    return addRow(new Row(shortFlag, List.of(flag, description)));
  }

  /** Alias for {@link #flag(String, String, String)} when there is no short flag. */
  public HelpPage flag(String flag, String description) {
    return flag(null, flag, description);
  }

  /** Adds everything on {@code other} to this page. */
  public HelpPage append(HelpPage other) {
    blocks.addAll(other.blocks);
    return this;
  }

  public boolean isEmpty() {
    return blocks.isEmpty();
  }

  private HelpPage addRow(Row row) {
    if (blocks.isEmpty() || !(blocks.getLast() instanceof Table)) {
      table(TABLE_INDENT);
    }
    ((Table) blocks.getLast()).rows().add(row);
    return this;
  }

  /** Returns the page laid out for the terminal's width, or a fixed width when output doesn't go to a terminal. */
  public String render() {
    int width = Terminal.width()
      .map(columns -> Math.clamp(columns, MIN_WIDTH, MAX_WIDTH))
      .orElse(DEFAULT_WIDTH);
    return render(width);
  }

  /** Returns the page laid out for {@code width} columns, each line ending with a newline. */
  public String render(int width) {
    StringBuilder result = new StringBuilder();
    for (var block : blocks) {
      switch (block) {
        case Line(String text) -> result.append(text).append('\n');
        case Heading(String text) -> {
          if (!result.isEmpty() && !result.toString().endsWith("\n\n")) {
            result.append('\n');
          }
          result.append(text).append(":\n");
        }
        case Table table -> renderTable(table, width, result);
      }
    }
    return result.toString();
  }

  private static void renderTable(Table table, int width, StringBuilder result) {
    boolean anyShortFlags = table.rows().stream().anyMatch(row -> row.shortFlag() != null);
    List<List<String>> rows = table.rows().stream().map(row -> cells(row, anyShortFlags)).toList();
    int[] columnStarts = columnStarts(table.indent(), rows);
    int valueIndent = table.indent() + 4;
    for (var cells : rows) {
      StringBuilder line = new StringBuilder(" ".repeat(table.indent())).append(cells.getFirst());
      boolean onlyFirstCell = true;
      for (int i = 1; i < cells.size(); i++) {
        String cell = cells.get(i);
        if (cell.isEmpty()) {
          continue;
        }
        if (line.length() + GAP > columnStarts[i]) {
          endLine(line, onlyFirstCell, valueIndent, width, result);
        }
        line.append(" ".repeat(columnStarts[i] - line.length()));
        onlyFirstCell = false;
        if (i == cells.size() - 1) {
          appendWrapped(cell, columnStarts[i], width, line, result);
        } else {
          line.append(cell);
        }
      }
      endLine(line, onlyFirstCell, valueIndent, width, result);
    }
  }

  private sealed interface Block {}

  private record Line(String text) implements Block {}

  private record Heading(String text) implements Block {}

  private record Row(String shortFlag, List<String> cells) {}

  private record Table(int indent, List<Row> rows) implements Block {}
}
