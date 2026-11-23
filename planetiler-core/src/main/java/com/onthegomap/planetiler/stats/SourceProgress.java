package com.onthegomap.planetiler.stats;

import com.onthegomap.planetiler.util.Format;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;
import java.util.function.LongToDoubleFunction;

/**
 * How far along one source is in downloading, reading and writing, which {@link ProgressLoggers#addSourceProgress}
 * shows in the progress logs, and how long it took, which {@link #summary()} shows once it is finished.
 * <p>
 * Whoever does the work sets the counts that apply ({@link #setDownload}, {@link #setRead}, {@link #setTrackWritten})
 * and the others stay out of the logs.
 */
@SuppressWarnings("java:S3077")
public class SourceProgress {

  private static final Format FORMAT = Format.defaultInstance();
  private final String name;
  private volatile Count download;
  private volatile BooleanSupplier downloadFinished;
  private volatile Count read;
  private volatile Count written;
  private final Counter.MultiThreadCounter featuresWritten = Counter.newMultiThreadCounter();
  // batches handed to the process workers plus features they took out of them, minus the ones they are done with
  private final Counter.MultiThreadCounter workLeft = Counter.newMultiThreadCounter();
  private volatile long startNanos = -1;
  private volatile long endNanos = -1;
  private long readStartCpuNanos;
  private volatile long readCpuNanos;
  // a plain AtomicLong is enough since each process worker adds its total for this source once, when it finishes
  private final AtomicLong processCpuNanos = new AtomicLong();

  public SourceProgress(String name) {
    this.name = name;
  }

  /** Returns the name of the source this row is for. */
  public String name() {
    return name;
  }

  /**
   * Tracks {@code done} of {@code total} bytes downloaded, where {@code total} returns 0 until it is known, until
   * {@code finished} returns true.
   */
  public SourceProgress setDownload(LongSupplier done, LongSupplier total, BooleanSupplier finished) {
    download = Count.startingNow(done, fraction(total));
    downloadFinished = finished;
    return this;
  }

  /** Tracks {@code done} of {@code total} features read, where 0 means the total is not known. */
  public SourceProgress setRead(LongSupplier done, long total) {
    read = Count.startingNow(done, fraction(() -> total));
    return this;
  }

  /**
   * Tracks {@code done} features read, with {@code fractionDone} as the percent complete, for sources that know how
   * much of their data they have read but not how many features are in it. {@code fractionDone} returns NaN while not
   * known.
   */
  public SourceProgress setRead(LongSupplier done, DoubleSupplier fractionDone) {
    read = Count.startingNow(done, value -> fractionDone.getAsDouble());
    return this;
  }

  /** Tracks the features written so far, which each process worker counts with {@link #writtenCounterForThread}. */
  public SourceProgress setTrackWritten() {
    written = Count.startingNow(featuresWritten, null);
    return this;
  }

  /** Returns the counter for the current thread to count features written from this source. */
  public Counter writtenCounterForThread() {
    return featuresWritten.counterForThread();
  }

  /**
   * Returns the counter for the current thread to track work the process workers have left for this source: +1 for each
   * batch handed to them and each feature taken out of a batch, -1 for each batch emptied and each feature processed.
   */
  public Counter workLeftCounterForThread() {
    return workLeft.counterForThread();
  }

  /** Marks that reading this source started, called from the thread that reads it. */
  public void start() {
    startNanos = System.nanoTime();
    readStartCpuNanos = ProcessInfo.getCurrentThreadCpuNanos();
  }

  /** Marks that this source has been read completely, called from the thread that read it. */
  public void finish() {
    endNanos = System.nanoTime();
    readCpuNanos = ProcessInfo.getCurrentThreadCpuNanos() - readStartCpuNanos;
  }

  /** Adds the CPU time a process worker spent on features from this source. */
  public void addProcessCpu(long nanos) {
    processCpuNanos.addAndGet(nanos);
  }

  /** Returns true once this source has nothing left to download. */
  public boolean isDownloaded() {
    var finished = downloadFinished;
    return finished == null || finished.getAsBoolean();
  }

  boolean hasWorkLeft() {
    return workLeft.get() > 0;
  }

  boolean isFinishedReading() {
    return endNanos >= 0;
  }

  /**
   * Returns true while this source is downloading or has features left to read or process. Readers set the read count
   * once they have opened the source, so until then only a download counts.
   */
  boolean isInProgress() {
    return read != null ? (endNanos < 0 || hasWorkLeft()) : !isDownloaded();
  }

  Count download() {
    return download;
  }

  Count read() {
    return read;
  }

  Count written() {
    return written;
  }

  /**
   * Returns a one-line summary of what was read and downloaded and how long it took, for when it is finished.
   * <p>
   * Once finished, it also shows the CPU time spent on the one thread that read it and on the process workers, to tell
   * a single-threaded bottleneck from work spread over many threads.
   */
  public String summary() {
    List<String> parts = new ArrayList<>();
    if (download != null) {
      parts.add("downloaded: " + FORMAT.storage(download.done().getAsLong(), false));
    }
    if (read != null) {
      String total = "read: " + FORMAT.numeric(read.done().getAsLong(), false);
      if (endNanos >= 0) {
        total += " in " + elapsed() + " cpu:" + FORMAT.duration(Duration.ofNanos(readCpuNanos));
      }
      parts.add(total);
    }
    if (written != null) {
      parts.add("process cpu:" + FORMAT.duration(Duration.ofNanos(processCpuNanos.get())));
      parts.add("written: " + FORMAT.numeric(written.done().getAsLong(), false));
    }
    return String.join("  ", parts);
  }

  private String elapsed() {
    return FORMAT.duration(Duration.ofNanos(endNanos - startNanos));
  }

  private static LongToDoubleFunction fraction(LongSupplier total) {
    return done -> {
      long totalNow = total.getAsLong();
      return totalNow > 0 ? 1d * done / totalNow : Double.NaN;
    };
  }

  /**
   * A count that was {@code startValue} at {@code startNanos}, with the fraction of the work it stands for done at a
   * given value, or null if it has no total.
   */
  record Count(LongSupplier done, LongToDoubleFunction fractionDone, long startValue, long startNanos) {

    static Count startingNow(LongSupplier done, LongToDoubleFunction fractionDone) {
      return new Count(done, fractionDone, done.getAsLong(), System.nanoTime());
    }
  }
}
