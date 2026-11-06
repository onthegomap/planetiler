package com.onthegomap.planetiler.util;

import com.onthegomap.planetiler.util.Downloader.ByteRange;
import java.util.ArrayList;
import java.util.List;

/**
 * Decides which HTTP range requests to make to fetch parts of one remote file, and how many of them to make at once.
 *
 * The same rules serve sparse reads (row groups of a GeoParquet file in the map bounds) and full downloads (the whole
 * file), so the requests ramp from a few small ones toward a full download as more of a file is needed:
 * <ol>
 * <li>HTTP requests aim for {@code 1/threadsPerFile} of the bytes needed, between {@link #MIN_REQUEST_BYTES} and
 * {@code maxRequestBytes}.</li>
 * <li>Neighboring small requested ranges share an HTTP request up to that size when the gap between them is at most 1%
 * of the bytes needed, fetching the gap rather than making another request. Requests start and end on needed ranges,
 * never in a gap.</li>
 * <li>A range larger than the target request size gets split evenly.</li>
 * <li>{@code threadsPerFile} requests run at once, up to 4 times as many when the needed ranges are spread out in small
 * runs, since each small request moves fewer bytes per round trip.</li>
 * </ol>
 *
 * @param threadsPerFile  requests to make at once for large runs of ranges ({@code download_threads})
 * @param maxRequestBytes largest request to make ({@code download_chunk_size_mb})
 */
public record RequestPlanner(int threadsPerFile, long maxRequestBytes) {

  /**
   * Requests don't get split smaller than 2MB since below that, the round trip of each request costs more than parallel
   * requests gain. This is the same as the largest request DuckDB 2 makes to read GeoParquet.
   */
  public static final int MIN_REQUEST_BYTES = 2 << 20;
  private static final long SMALL_SPAN_BYTES_IN_FLIGHT = 16L * MIN_REQUEST_BYTES;
  private static final int MAX_SMALL_SPAN_THREADS_FACTOR = 4;
  private static final int MAX_GAP_DIVISOR = 100;

  public RequestPlanner {
    if (threadsPerFile < 1 || maxRequestBytes < 1) {
      throw new IllegalArgumentException("threadsPerFile and maxRequestBytes must be positive");
    }
  }

  /** The requests to make to fetch some ranges of a file, in file order, and how many of them to make at once. */
  public record Plan(List<ByteRange> httpRequests, int maxInFlight) {

    public static final Plan EMPTY = new Plan(List.of(), 1);

    /** Returns the number of bytes the requests fetch, including the gaps between needed ranges they span. */
    public long bytes() {
      return httpRequests.stream().mapToLong(ByteRange::length).sum();
    }

    /** Returns the average number of bytes that requests in flight fetch at once, for memory-budgeting purposes. */
    public long bytesInFlight() {
      return httpRequests.isEmpty() ? 0 : bytes() / httpRequests.size() * Math.min(maxInFlight, httpRequests.size());
    }
  }

  /**
   * Returns the requests that fetch {@code needed}, which must be in ascending order and must not overlap. Empty ranges
   * get skipped.
   *
   * @throws IllegalArgumentException if {@code needed} is out of order or overlaps
   */
  public Plan plan(List<ByteRange> needed) {
    List<ByteRange> neededRanges = needed.stream().filter(range -> range.length() > 0).toList();
    if (neededRanges.isEmpty()) {
      return Plan.EMPTY;
    }
    long neededBytes = neededRanges.stream().mapToLong(ByteRange::length).sum();
    long maxGapBytes = neededBytes / MAX_GAP_DIVISOR;
    // mike: MA streaming 176 s vs 142 s before: rows are processed in order, so a 30-60 MB request at ~1.3 MB/s stalls
    // them (10 MB cap: 161 s). Cap sparse-read requests near 2-4 row groups (~8 MB), or keep as is?
    long targetRequestBytes = Math.clamp(
      Math.ceilDiv(neededBytes, threadsPerFile),
      Math.min(MIN_REQUEST_BYTES, maxRequestBytes),
      maxRequestBytes);
    List<ByteRange> httpRequests = new ArrayList<>();
    // the HTTP request being built, from requestStart to the end of the last needed range added to it
    long requestStart = neededRanges.getFirst().offset();
    long requestEnd = requestStart;
    // runs of needed ranges with small gaps between them
    int spans = 1;
    for (var neededRange : neededRanges) {
      if (neededRange.offset() < requestEnd) {
        throw new IllegalArgumentException("Ranges must be in ascending order and not overlap: " + needed);
      }
      boolean gapTooBig = neededRange.offset() - requestEnd > maxGapBytes;
      boolean requestWouldBeTooBig =
        neededRange.offset() > requestStart && neededRange.end() - requestStart > targetRequestBytes;
      if (gapTooBig) {
        spans++;
      }
      if (gapTooBig || requestWouldBeTooBig) {
        // close the request and start a new one
        httpRequests.add(new ByteRange(requestStart, requestEnd - requestStart));
        requestStart = neededRange.offset();
      }
      if (neededRange.length() > targetRequestBytes) {
        // range too big, split into ~even pieces no smaller than the target
        long pieces = Math.max(neededRange.length() / targetRequestBytes,
          Math.ceilDiv(neededRange.length(), maxRequestBytes));
        for (long i = 1; i < pieces; i++) {
          long pieceEnd = neededRange.offset() + neededRange.length() * i / pieces;
          httpRequests.add(new ByteRange(requestStart, pieceEnd - requestStart));
          requestStart = pieceEnd;
        }
      }
      requestEnd = neededRange.end();
    }
    httpRequests.add(new ByteRange(requestStart, requestEnd - requestStart));
    // more requests at once only help when the needed bytes come in many small spans, since a small request spends
    // most of its time on the round trip. This goes by the average span, not the average request so a whole file is
    // one big span and gets threadsPerFile requests at once, however small download_chunk_size_mb cuts them
    long averageSpanBytes = httpRequests.stream().mapToLong(ByteRange::length).sum() / spans;
    int maxInFlight = (int) Math.clamp(SMALL_SPAN_BYTES_IN_FLIGHT / Math.max(1, averageSpanBytes), threadsPerFile,
      (long) threadsPerFile * MAX_SMALL_SPAN_THREADS_FACTOR);
    return new Plan(List.copyOf(httpRequests), maxInFlight);
  }
}
