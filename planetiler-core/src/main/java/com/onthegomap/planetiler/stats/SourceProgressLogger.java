package com.onthegomap.planetiler.stats;

import com.onthegomap.planetiler.util.Format;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Shows how far along each in-progress {@link SourceProgress} is in the logs:
 *
 * <pre>
 * water_polygons   download: [ 1.4M  67% 1.9M/s ] read: [    9  64%    4/s ] write: [  23k 9.1k/s ]
 * natural_earth    download: [  31M  79%   1M/s ]
 * lake_centerlines download:                      read: [  120  12%   80/s ] write: [   1k  700/s ]
 * </pre>
 *
 * <p>
 * Not thread safe, only the thread that logs progress may use it.
 */
final class SourceProgressLogger {

  private final Format format;
  private final List<Row> rows;
  private final int nameWidth;

  SourceProgressLogger(Format format, List<SourceProgress> sources) {
    this.format = format;
    this.rows = sources.stream().map(Row::new).toList();
    this.nameWidth = sources.stream().mapToInt(source -> source.name().length()).max().orElse(0);
  }

  /** Returns one line per row to show, or an empty string when there are none. */
  String table() {
    var shown = rows.stream().filter(Row::isShown).toList();
    shown.forEach(Row::initCells);
    // keeps the other columns lined up when only some of the sources download anything
    int downloadWidth = shown.stream().filter(row -> row.download != null)
      .mapToInt(row -> row.download.width()).max().orElse(0);
    return shown.stream().map(row -> row.render(downloadWidth)).collect(Collectors.joining(System.lineSeparator()));
  }

  /** Returns the cells of the first source, with an empty {@code read:} until its reader has opened it. */
  String cells() {
    return rows.getFirst().cells();
  }

  private final class Row {
    private final SourceProgress source;
    // created once the source starts counting each of them
    private Cell download;
    private Cell read;
    private Cell written;
    private boolean wasInProgress;
    private boolean shownWhenDone = false;

    Row(SourceProgress source) {
      this.source = source;
      // so that something that finishes before the first log, like a short download, still shows up in the last one
      this.wasInProgress = source.isInProgress();
    }

    // Show rows for sources one more time after they finish
    boolean isShown() {
      boolean inProgress = source.isInProgress();
      boolean shown = inProgress || (!shownWhenDone && (wasInProgress || source.isFinishedReading()));
      if (shown && !inProgress) {
        shownWhenDone = true;
      }
      wasInProgress = inProgress;
      return shown;
    }

    String render(int downloadWidth) {
      StringBuilder result = new StringBuilder(Format.padRight(source.name(), nameWidth));
      if (download != null) {
        result.append(" ").append(download.render(false));
      } else if (downloadWidth > 0) {
        result.append(" ").append(Format.padRight("download:", downloadWidth));
      }
      // skip showing num written until we actually start reading something
      if (read != null) {
        result.append(" ").append(readAndWritten());
      }
      return result.toString();
    }

    String cells() {
      initCells();
      StringBuilder result = new StringBuilder();
      if (download != null) {
        result.append(" ").append(download.render(false));
      }
      result.append(" ").append(read == null ? "read:" : readAndWritten());
      return result.toString();
    }

    private String readAndWritten() {
      // stays green while a process worker is busy with this source's features, even when no count moves, like when
      // one worker spends seconds on a huge polygon
      String result = read.render(source.hasWorkLeft());
      return written == null ? result : result + " " + written.render(false);
    }

    void initCells() {
      if (download == null) {
        download = cell("download", source.download(), n -> format.storage(n, true), true);
      }
      if (read == null) {
        read = cell("read", source.read(), n -> format.numeric(n, true), true);
      }
      if (written == null) {
        written = cell("write", source.written(), n -> format.numeric(n, true), false);
      }
    }

    private Cell cell(String name, SourceProgress.Count count, Function<Number, String> formatPadded, boolean color) {
      return count == null ? null : new Cell(name, new RateCell(format, count.done(), count.fractionDone(),
        formatPadded, color, count.startValue(), count.startNanos()));
    }
  }

  private record Cell(String name, RateCell value) {
    String render(boolean busy) {
      return name + ": " + value.render(busy);
    }

    int width() {
      return name.length() + ": ".length() + value.width();
    }
  }
}
