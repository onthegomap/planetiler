package com.onthegomap.planetiler.util;

import static com.google.common.net.HttpHeaders.*;

import com.google.common.util.concurrent.RateLimiter;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.reader.remote.CachedRangeFile;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.worker.RunnableThatThrows;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpHeaders;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility for making external HTTP requests and downloading remote HTTP files to disk.
 * <p>
 * All HTTP requests from planetiler should go through one shared Downloader instance, which sets a consistent user
 * agent and enforces global {@code download_max_requests} and {@code download_max_bandwidth} limits.
 * <p>
 * Downloads resume where a previous run stopped. After downloading a file once, it won't be downloaded again unless the
 * remote file changes.
 * <p>
 * For example:
 * {@snippet :
 * try (var downloader = Downloader.create(PlanetilerConfig.defaults())) {
 *   downloader
 *     .add("natural_earth", "http://url/of/natural_earth.zip", Path.of("natural_earth.zip"))
 *     .add("osm", "http://url/of/file.osm.pbf", Path.of("file.osm.pbf"))
 *     .run();
 * }
 * }
 * <p>
 * Close the downloader when done with it to stop its threads; {@link #run()} leaves them running so the same downloader
 * can make more requests afterwards (they are virtual threads, so an unclosed downloader costs little).
 * <p>
 * As a shortcut to find the URL of a file to download from the <a href="https://download.geofabrik.de/">Geofabrik
 * download site</a>, you can use "geofabrik:extract name" (i.e. "geofabrik:monaco" or "geofabrik:australia") to look up
 * a {@code .osm.pbf} download URL in the <a href="https://download.geofabrik.de/technical.html">Geofabrik JSON
 * index</a>.
 * <p>
 * Use "aws:latest" to download the latest {@code planet.osm.pbf} file from the
 * <a href="https://registry.opendata.aws/osm/">AWS Open Data Registry</a>, or "overture:latest" to download the latest
 * <a href="https://overturemaps.org/">Overture Maps Foundation</a> release.
 */
@SuppressWarnings("UnusedReturnValue")
public class Downloader implements Closeable {

  private static final int MAX_REDIRECTS = 5;
  // read enough of an error response's body to say what went wrong, without filling the log with an HTML error page
  private static final int MAX_ERROR_BODY_BYTES = 500;
  private static final Format FORMAT = Format.defaultInstance();
  // write 1 MB blocks to disk instead of 16 KB to reduce CPU usage when downloading large files on big machines
  private static final int BUFFER_SIZE = 1 << 20;
  private static final Pattern CONTENT_RANGE_PATTERN = Pattern.compile("bytes (\\d+)-(\\d+)/(\\d+)");
  private static final Logger LOGGER = LoggerFactory.getLogger(Downloader.class);
  private final PlanetilerConfig config;
  private final List<ResourceToDownload> toDownloadList = new ArrayList<>();
  private final ExecutorService executor;
  private final RequestPlanner planner;
  private final ResourceUsage diskSpaceCheck = new ResourceUsage("download");
  private final RateLimiter rateLimiter;
  private final Semaphore globalRequestLimiter;

  static {
    // HttpURLConnection keeps only 5 idle connections per server for reuse by default, limiting the throughput of many
    // small concurrent downloads, so increase it here (unless the user set it) before the first request reads it.
    if (System.getProperty("http.maxConnections") == null) {
      System.setProperty("http.maxConnections", "1000");
    }
  }

  Downloader(PlanetilerConfig config, long maxRequestBytes) {
    this.rateLimiter = config.downloadMaxBandwidth() == 0 ? null : RateLimiter.create(config.downloadMaxBandwidth());
    this.planner = new RequestPlanner(config.downloadThreads(), maxRequestBytes);
    this.config = config;
    this.globalRequestLimiter = new Semaphore(Math.max(config.downloadMaxRequests(), config.downloadThreads()));
    this.executor = Executors.newVirtualThreadPerTaskExecutor();
  }

  public static Downloader create(PlanetilerConfig config) {
    return new Downloader(config, config.downloadChunkSizeMB() * 1_000_000L);
  }

  /**
   * Returns the contents of {@code url} read with a short-lived downloader.
   *
   * @deprecated use {@link #openStream(String)} on the shared downloader so requests share its limits; this only
   *             remains for planetiler-openmaptiles' code generator until it moves over
   */
  @Deprecated(forRemoval = true)
  public static InputStream openStream(String url, PlanetilerConfig config) throws IOException {
    try (var downloader = create(config); var in = downloader.openStream(url)) {
      return new ByteArrayInputStream(in.readAllBytes());
    }
  }

  /**
   * Returns an input stream reading {@code url} with a GET request, following redirects and retrying failures to
   * connect.
   *
   * @throws IOException if the request fails after all retries
   */
  public InputStream openStream(String url) throws IOException {
    var uri = URI.create(url);
    if (!"http".equalsIgnoreCase(uri.getScheme()) && !"https".equalsIgnoreCase(uri.getScheme())) {
      // local file:///... don't need retries or request limiting
      return uri.toURL().openStream();
    }
    return withRetries("fetching " + url, () -> getFollowingRedirects(url, null, null).body());
  }

  /**
   * Adds a new resource to download but does not start downloading it until {@link #run()} is called.
   * <p>
   * Only fetches files or parts of files that haven't been fetched yet.
   *
   * @param id     short name to use for this download when logging progress
   * @param url    the external resource to fetch, "aws:latest" (for the latest planet .osm.pbf), "overture:latest" (for
   *               the latest Overture Maps release) or "geofabrik:extract-name" as a shortcut to use
   *               {@link Geofabrik#getDownloadUrl(String, Downloader)} to look up a {@code .osm.pbf}
   *               <a href="https://download.geofabrik.de/">Geofabrik</a> extract URL by partial match on area name
   * @param output where to download the file to
   * @return {@code this} for chaining
   */
  public Downloader add(String id, String url, Path output) {
    if (url.startsWith("geofabrik:")) {
      url = Geofabrik.getDownloadUrl(url.replaceFirst("^geofabrik:", ""), this);
    } else if (url.startsWith("aws:")) {
      url = AwsOsm.OSM_PDS.getDownloadUrl(url.replaceFirst("^aws:", ""), this);
    } else if (url.startsWith("overture:")) {
      url = AwsOsm.OVERTURE.getDownloadUrl(url.replaceFirst("^overture:", ""), this);
    }
    toDownloadList.add(new ResourceToDownload(id, url, output));
    return this;
  }

  /**
   * Starts downloading all resources added since the last call in parallel, logging progress until complete.
   *
   * @throws IllegalStateException if an error occurs downloading any resource, will be thrown after all resources
   *                               finish
   */
  public void run() {
    if (toDownloadList.isEmpty()) {
      return;
    }
    var batch = List.copyOf(toDownloadList);
    toDownloadList.clear();
    var downloads = CompletableFuture
      .allOf(batch.stream()
        .map(this::downloadIfNecessary)
        .toArray(CompletableFuture[]::new)
      );

    ProgressLoggers loggers = ProgressLoggers.create();

    var groupedById = batch.stream().collect(Collectors.groupingBy(ResourceToDownload::id));

    for (var group : groupedById.entrySet()) {
      long size = 0;
      for (var item : group.getValue()) {
        try {
          size += item.length.get(config.maxHttpRequestDuration().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException("Error getting size of " + item.url, e);
        } catch (ExecutionException | TimeoutException e) {
          throw new IllegalStateException("Error getting size of " + item.url, e);
        }
      }
      loggers.addStorageRatePercentCounter(group.getKey(), size,
        () -> group.getValue().stream().mapToLong(ResourceToDownload::bytesDownloaded).sum(), true);
    }
    loggers.add(" ").addProcessStats()
      .awaitAndLog(downloads, config.logInterval());
  }

  /** Stops the threads this downloader uses once requests in progress finish. */
  @Override
  public void close() {
    executor.shutdown();
  }

  CompletableFuture<Void> downloadIfNecessary(ResourceToDownload resource) {
    return CompletableFuture.runAsync(RunnableThatThrows.wrap(() -> {
      LogUtil.setStage("download", resource.id);
      try {
        download(resource);
      } catch (Exception e) { // NOSONAR
        resource.length.completeExceptionally(e);
        LOGGER.error("Error downloading {} to {}", resource.url, resource.output, e);
        throw e;
      }
    }), executor);
  }

  private void download(ResourceToDownload resource) throws IOException {
    Path output = resource.output;
    try {
      if (CachedRangeFile.isCompleteCopy(output, resource.url, true, this)) {
        LOGGER.info("Skipping {}: {} already up-to-date", resource.id, output);
        resource.length.complete(0L);
        return;
      }
      boolean resuming = CachedRangeFile.isPartial(output);
      // use CachedRangeFile for full downloads so that:
      // 1) full downloads are resumable
      // 2) switching from cached to full download skips bytes already downloaded (and vice versa if full was canceled)
      try (var file = CachedRangeFile.open(output, resource.url, 0, this)) {
        long missing = file.bytesMissing();
        // progress of a resumed download continues from what previous runs fetched
        resource.progress.add(file.length() - missing);
        resource.length.complete(file.length());
        String redirectInfo = file.url().equals(resource.url) ? "" : " (redirected to " + file.url() + ")";
        String resumeInfo =
          resuming ? " (resuming, " + FORMAT.storage(missing) + "B of " + FORMAT.storage(file.length()) + "B left)" :
            "";
        LOGGER.info("Downloading {}{} to {}{}", resource.url, redirectInfo, output, resumeInfo);
        diskSpaceCheck.addDisk(output, missing, resource.id);
        diskSpaceCheck.checkAgainstLimits(config.force(), false);
        file.fetchMissing(resource.progress::add);
      }
    } catch (RangesNotSupportedException _) {
      downloadWhole(resource);
    }
    LOGGER.info("Finished downloading {} to {}", resource.url, output);
  }

  /**
   * Downloads {@code url} to {@code output} with one GET request, for servers that don't support range requests; the
   * download starts over if interrupted.
   *
   * @throws IOException if the download fails after all retries
   */
  public void downloadWhole(String url, Path output) throws IOException {
    downloadWhole(new ResourceToDownload(output.getFileName().toString(), url, output));
    LOGGER.info("Finished downloading {} to {}", url, output);
  }

  private void downloadWhole(ResourceToDownload resource) throws IOException {
    LOGGER.info("{} does not support range requests, downloading it to {} in one request that can't resume",
      resource.url, resource.output);
    withRetries("downloading " + resource.url, () -> {
      var written = new Object() {
        long bytes = 0;
      };
      try (var response = getFollowingRedirects(resource.url, null, null)) {
        var version = response.version();
        // servers that stream generated content may not send a Content-Length, so progress gets logged without a total
        resource.length.complete(Math.max(0, version.length()));
        try (var file = CachedRangeFile.create(resource.output, version)) {
          Sink sink = (position, buffer, offset, length) -> {
            file.write(position, buffer, offset, length);
            written.bytes += length;
            resource.progress.add(length);
          };
          if (version.length() >= 0) {
            response.readInto(0, version.length(), sink);
          } else {
            // no Content-Length: read until the body ends, then record its length
            file.setLength(response.readToEnd(0, sink));
          }
        }
      } catch (IOException e) {
        resource.progress.add(-written.bytes);
        throw e;
      }
      return null;
    });
  }

  /**
   * Fetches the last {@code length} bytes of {@code url} (or all of it if it is shorter) with a range request.
   *
   * @throws IOException                 if the request fails after all retries
   * @throws RangesNotSupportedException if the server does not support range requests
   */
  public RangeResponse fetchTail(String url, int length) throws IOException {
    try {
      return requestRange(url, "bytes=-" + length);
    } catch (IOException e) {
      // GitHub release downloads answer suffix ranges with 501 Not Implemented
      if (!(e instanceof RangesNotSupportedException || (e instanceof BadResponseException bad && bad.status == 501))) {
        throw e;
      }
      // some servers don't support suffix ranges: get the file's length from its first byte, then ask for the tail
      long fileLength = fetchRange(url, new ByteRange(0, 1)).version().length();
      // if server doesn't support range requests at all, this will fail hard with RangesNotSupportedException
      long start = Math.max(0, fileLength - length);
      return fetchRange(url, new ByteRange(start, fileLength - start));
    }
  }

  /**
   * Fetches {@code range} of {@code url} with one range request, following redirects.
   *
   * @throws IOException                 if the request fails after all retries
   * @throws RangesNotSupportedException if the server does not support range requests
   */
  public RangeResponse fetchRange(String url, ByteRange range) throws IOException {
    return requestRange(url, "bytes=" + range.offset() + "-" + (range.end() - 1));
  }

  /** Returns the bytes of one GET request for {@code range} of {@code url}, read into memory. */
  private RangeResponse requestRange(String url, String range) throws IOException {
    return withRetries("fetching " + range + " of " + url, () -> {
      try (var response = getFollowingRedirects(url, range, null)) {
        ByteRange contentRange = response.responseBytes();
        byte[] bytes = new byte[Math.toIntExact(contentRange.length())];
        response.readInto(contentRange.offset(), bytes.length, (position, buffer, offset, length) -> System
          .arraycopy(buffer, offset, bytes, Math.toIntExact(position - contentRange.offset()), length));
        return new RangeResponse(response.url(), response.version(), contentRange.offset(), bytes);
      }
    });
  }

  /**
   * Fetches {@code range} of the remote file at {@code url} with one range request, passing the bytes to {@code sink}
   * in order as they arrive, on the calling thread. A retry after a dropped connection continues from the next byte not
   * passed to {@code sink}.
   *
   * @throws IOException                if the request fails after all retries
   * @throws RemoteFileChangedException if the file is no longer {@code expected}
   */
  public void fetch(String url, RemoteVersion expected, ByteRange range, Sink sink) throws IOException {
    // not for thread safety (bytes arrive on this thread), just a counter the lambdas below can update
    var bytesDone = new AtomicLong();
    withRetries("fetching " + range + " of " + url, bytesDone::get, () -> {
      var remaining = new ByteRange(range.offset() + bytesDone.get(), range.length() - bytesDone.get());
      try (
        var response = getFollowingRedirects(url, "bytes=" + remaining.offset() + "-" + (range.end() - 1),
          expected)
      ) {
        ByteRange contentRange = response.responseBytes();
        if (!contentRange.equals(remaining)) {
          throw new IllegalStateException("Requested " + remaining + " of " + url + " but got " + contentRange);
        }
        // double check precondition in case the server does not honor it or the file has no validators
        if (!isSameVersion(expected, response.version())) {
          throw new RemoteFileChangedException(url, expected + " -> " + response.version());
        }
        response.readInto(remaining.offset(), remaining.length(), (position, buffer, offset, length) -> {
          sink.accept(position, buffer, offset, length);
          bytesDone.addAndGet(length);
        });
      }
      return null;
    });
  }

  /**
   * Returns true if {@code actual} is the same version of a file as {@code expected} unless user explicitly sets
   * {@code download_fail_if_changed=false}.
   */
  public boolean isSameVersion(RemoteVersion expected, RemoteVersion actual) {
    return config.downloadFailIfChanged() ? expected.matches(actual) : expected.length() == actual.length();
  }

  /**
   * Starts {@code task}, which makes requests through this downloader, on a virtual thread of its own, so that tasks
   * that mostly wait for the network can run at once.
   * <p>
   * The future fails with whatever {@code task} throws.
   */
  public <T> CompletableFuture<T> async(Callable<T> task) {
    return CompletableFutures.supplyAsync(task, executor);
  }

  /** Returns the planner that decides which requests to make to fetch parts of a remote file. */
  public RequestPlanner planner() {
    return planner;
  }

  /** Returns the config this downloader was created with. */
  public PlanetilerConfig config() {
    return config;
  }

  /**
   * Sends {@code request} without following redirects and returns what {@code reader} reads from the body of a
   * successful response, retrying like the other requests; reading the body gets retried too, since a connection can
   * drop part way through it.
   *
   * @throws IOException if the request or reading its response fails after all retries
   */
  <T> T sendWithRetries(Request request, BodyReader<T> reader) throws IOException {
    return withRetries("sending request to " + request.url(), () -> {
      try (var response = sendWithLimits(request)) {
        if (response.status() / 100 != 2) {
          throw badResponse(response);
        }
        return reader.read(response.body());
      }
    });
  }

  /** Reads the body of a response. */
  @FunctionalInterface
  interface BodyReader<T> {

    T read(InputStream body) throws IOException;
  }

  private <T> T withRetries(String what, Callable<T> attempt) throws IOException {
    return withRetries(what, () -> 0, attempt);
  }

  /**
   * Like {@link #withRetries(String, Callable)}, but an attempt that failed after {@code progress} went up gets retried
   * right away and the retries start over.
   */
  private <T> T withRetries(String what, LongSupplier progress, Callable<T> attempt) throws IOException {
    for (int retry = 0;; retry++) {
      long progressBefore = progress.getAsLong();
      try {
        return attempt.call();
      } catch (IOException e) {
        int retriesRemaining = config.httpRetries() - retry;
        if (!isRetryable(e)) {
          throw e;
        } else if (progress.getAsLong() > progressBefore) {
          // if we downloaded some bytes then the initial request succeeded and the server closed the idle connection;
          // in that case, restart the request where we left off and don't count against our retries
          retry = -1;
        } else if (retriesRemaining <= 0) {
          throw e;
        } else {
          waitBeforeRetry(what, retriesRemaining, e);
        }
      } catch (InterruptedException _) {
        Thread.currentThread().interrupt();
        throw new InterruptedIOException("Interrupted " + what);
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
  }

  private static boolean isRetryable(IOException e) {
    return switch (e) {
      case BadResponseException bad -> bad.retryable();
      case RangesNotSupportedException _ -> false;
      default -> true;
    };
  }

  private void waitBeforeRetry(String what, int retriesRemaining, IOException e) throws IOException {
    Duration wait = config.httpRetryWait();
    Duration retryAfter = e instanceof BadResponseException bad ? bad.retryAfter : null;
    String reason = "";
    if (retryAfter != null) {
      boolean capped = retryAfter.compareTo(config.httpRetryMaxWait()) > 0;
      Duration asked = capped ? config.httpRetryMaxWait() : retryAfter;
      if (asked.compareTo(wait) > 0) {
        wait = asked;
        reason = capped ? " (--http-retry-max-wait)" : " as the server asked";
      }
    }
    String cause = e instanceof BadResponseException bad ? "server responded: " + bad.response : e.getMessage();
    if (LOGGER.isWarnEnabled()) {
      LOGGER.warn("Error {}, retrying in {}{}, {}, {} {} left", what, FORMAT.duration(wait), reason, cause,
        retriesRemaining, retriesRemaining == 1 ? "retry" : "retries");
    }
    try {
      retrySleep(wait);
    } catch (InterruptedException _) {
      Thread.currentThread().interrupt();
      throw new InterruptedIOException("Interrupted " + what);
    }
  }

  /**
   * Sends a GET request for {@code range} of {@code url} (the whole file if null), following redirects, with
   * ETag/Last-Modified preconditions, and returns a 200 or 206 response the caller must close.
   */
  private Response getFollowingRedirects(String url, String range, RemoteVersion expected)
    throws IOException, InterruptedException {
    for (int redirects = 0; redirects <= MAX_REDIRECTS; redirects++) {
      Map<String, String> headers = new HashMap<>();
      if (range != null) {
        headers.put(RANGE, range);
      }
      if (expected != null && config.downloadFailIfChanged()) {
        addPreconditions(headers, expected);
      }
      var response = sendWithLimits(new Request(url, headers, null));
      int status = response.status();
      if (status == 200 || status == 206) {
        return response;
      }
      try (response) {
        if (status >= 300 && status < 400) {
          String location = response.headers().firstValue(LOCATION)
            .orElseThrow(() -> new IllegalStateException(
              "Received " + status + " but no location header from " + response.url()));
          url = URI.create(url).resolve(location).toString();
        } else if (status == 412) {
          throw new RemoteFileChangedException(url, "this file changed on the server mid-download");
        } else {
          throw badResponse(response);
        }
      }
    }
    throw new IllegalStateException("Exceeded " + MAX_REDIRECTS + " redirects for " + url);
  }

  /** Returns the exception for an error {@code response}, with the start of its body that says what went wrong. */
  private static BadResponseException badResponse(Response response) {
    String body;
    try {
      body = new String(response.body().readNBytes(MAX_ERROR_BODY_BYTES), StandardCharsets.UTF_8)
        .replaceAll("\\s+", " ").trim();
    } catch (IOException e) {
      body = "error reading body: " + e;
    }
    int status = response.status();
    Duration retryAfter = response.headers().firstValue(RETRY_AFTER)
      .map(value -> parseRetryAfter(value, Instant.now()))
      .orElse(null);
    return new BadResponseException(response.url(), status +
      (retryAfter == null ? "" : " (Retry-After: " + FORMAT.duration(retryAfter) + ")") +
      (body.isEmpty() ? "" : " " + body), status, retryAfter);
  }

  /**
   * Returns how long a {@code Retry-After} header value (seconds or an HTTP date, RFC 9110 10.2.3) says to wait from
   * {@code now}, or null if it can't be parsed.
   */
  static Duration parseRetryAfter(String value, Instant now) {
    try {
      if (!value.isBlank() && value.strip().chars().allMatch(Character::isDigit)) {
        return Duration.ofSeconds(Long.parseLong(value.strip()));
      }
      var wait = Duration.between(now, ZonedDateTime.parse(value.strip(), DateTimeFormatter.RFC_1123_DATE_TIME));
      return wait.isNegative() ? Duration.ZERO : wait;
    } catch (DateTimeParseException | NumberFormatException _) {
      return null;
    }
  }

  private static void addPreconditions(Map<String, String> headers, RemoteVersion expected) {
    // make the server reject the request if the file changed instead of returning bytes from the new version
    // weak etags (W/"...") only guarantee equivalent content not identical bytes, so don't rely on them
    if (expected.etag() != null && !expected.etag().startsWith("W/")) {
      headers.put(IF_MATCH, expected.etag());
    } else if (expected.lastModified() != null) {
      headers.put(IF_UNMODIFIED_SINCE, expected.lastModified());
    }
  }

  /**
   * Sends {@code request} once it is one of the {@code download_max_requests} requests in flight, returning a response
   * whose body counts against the bandwidth limit and must be closed to let the next request start.
   */
  Response sendWithLimits(Request request) throws IOException, InterruptedException {
    globalRequestLimiter.acquire();
    try {
      var response = sendOverNetwork(request);
      return response.withBodyReader(new BandwidthLimitedReader(response.body));
    } catch (IOException | RuntimeException e) {
      globalRequestLimiter.release();
      throw e;
    }
  }

  /**
   * Sends {@code request} over the network without following redirects, failing if connecting or any read takes longer
   * than {@code http_timeout}; the only method that sends requests, so tests override it to simulate a server.
   */
  Response sendOverNetwork(Request request) throws IOException {
    // HttpURLConnection instead of the newer HttpClient: on larger machines, HttpUrlConnection can sustain up to
    // ~7GB/s vs. only ~2.6 GB/s for HttpClient.
    // TODO revisit once https://bugs.openjdk.org/browse/JDK-8277519 (HttpClient performance) is fixed
    var connection = (HttpURLConnection) URI.create(request.url()).toURL().openConnection();
    connection.setInstanceFollowRedirects(false);
    connection.setConnectTimeout(Math.toIntExact(config.httpTimeout().toMillis()));
    connection.setReadTimeout(Math.toIntExact(config.httpTimeout().toMillis()));
    connection.setRequestProperty(USER_AGENT, config.httpUserAgent());
    request.headers().forEach(connection::setRequestProperty);
    if (request.body() != null) {
      connection.setDoOutput(true);
      try (var out = connection.getOutputStream()) {
        out.write(request.body().getBytes(StandardCharsets.UTF_8));
      }
    }
    int status = connection.getResponseCode();
    InputStream body = status >= 400 ? connection.getErrorStream() : connection.getInputStream();
    return new Response(request.url(), status, toHttpHeaders(connection.getHeaderFields()),
      body == null ? InputStream.nullInputStream() : body);
  }

  static HttpHeaders toHttpHeaders(Map<String, List<String>> fields) {
    // header names keep the case the server sent them in, so the same header can show up under two keys (Vary and
    // vary), which HttpHeaders.of rejects as duplicates
    Map<String, List<String>> headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    fields.forEach((name, values) -> {
      if (name != null) { // the status line
        headers.computeIfAbsent(name, _ -> new ArrayList<>()).addAll(values);
      }
    });
    return HttpHeaders.of(headers, (name, value) -> true);
  }

  /** An HTTP request to send: a GET, or a POST if {@code body} is not null. */
  record Request(String url, Map<String, String> headers, String body) {}

  /** A response body that applies the bandwidth limit and frees its request slot when closed. */
  private final class BandwidthLimitedReader extends FilterInputStream {

    private final AtomicBoolean closed = new AtomicBoolean(false);

    BandwidthLimitedReader(InputStream in) {
      super(in);
    }

    @Override
    public int read() throws IOException {
      byte[] one = new byte[1];
      return read(one, 0, 1) < 0 ? -1 : one[0] & 0xff;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int read = super.read(b, off, len);
      if (read > 0 && rateLimiter != null) {
        rateLimiter.acquire(read);
      }
      return read;
    }

    @Override
    public void close() throws IOException {
      if (closed.compareAndSet(false, true)) {
        try {
          // closing a body before its end is fine, HttpURLConnection just may not reuse its connection
          super.close();
        } finally {
          globalRequestLimiter.release();
        }
      }
    }
  }

  record Response(String url, int status, HttpHeaders headers, InputStream body) implements Closeable {

    ByteRange responseBytes() throws RangesNotSupportedException {
      if (status == 200) {
        throw new RangesNotSupportedException(url, version());
      }
      // Content-Range: bytes <start>-<end>/<length>
      String contentRange = headers.firstValue(CONTENT_RANGE).orElse("");
      var matcher = CONTENT_RANGE_PATTERN.matcher(contentRange);
      if (!matcher.matches()) {
        throw new IllegalStateException("Bad Content-Range from " + url + ": " + contentRange);
      }
      long start = Long.parseLong(matcher.group(1));
      return new ByteRange(start, Long.parseLong(matcher.group(2)) - start + 1);
    }

    /** Returns the version of the file in this response, with length -1 if the server didn't send it. */
    RemoteVersion version() {
      long length;
      if (status == 200) {
        length = headers.firstValueAsLong(CONTENT_LENGTH).orElse(-1);
      } else {
        var matcher = CONTENT_RANGE_PATTERN.matcher(headers.firstValue(CONTENT_RANGE).orElse(""));
        length = matcher.matches() ? Long.parseLong(matcher.group(3)) : -1;
      }
      return new RemoteVersion(length, headers.firstValue(ETAG).orElse(null),
        headers.firstValue(LAST_MODIFIED).orElse(null));
    }

    void readInto(long position, long length, Sink sink) throws IOException {
      byte[] buffer = new byte[(int) Math.min(BUFFER_SIZE, length)];
      long end = position + length;
      while (position < end) {
        int wanted = (int) Math.min(buffer.length, end - position);
        int read = body.readNBytes(buffer, 0, wanted);
        if (read > 0) {
          accept(sink, position, buffer, read);
          position += read;
        }
        if (read < wanted) {
          throw new IOException("Unexpected EOF at " + position + "/" + end + " of " + url);
        }
      }
    }

    /**
     * Passes the rest of the body, which starts at {@code position} of the file, to {@code sink}; returns its length.
     */
    long readToEnd(long position, Sink sink) throws IOException {
      long start = position;
      byte[] buffer = new byte[BUFFER_SIZE];
      for (int read; (read = body.read(buffer)) >= 0; position += read) {
        accept(sink, position, buffer, read);
      }
      return position - start;
    }

    private static void accept(Sink sink, long position, byte[] buffer, int length) {
      try {
        sink.accept(position, buffer, 0, length);
      } catch (IOException e) {
        // a failure to store the bytes is no reason to fetch them again
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public void close() throws IOException {
      body.close();
    }

    public Response withBodyReader(InputStream newBody) {
      return new Response(url, status, headers, newBody);
    }
  }

  /** Receiver for bytes fetched from a remote file. */
  @FunctionalInterface
  public interface Sink {

    /**
     * Receives {@code length} bytes of {@code buffer} from {@code offset}, which are at {@code position} in the file.
     */
    void accept(long position, byte[] buffer, int offset, int length) throws IOException;
  }

  public record ByteRange(long offset, long length) {

    /** Returns the offset just past the last byte. */
    public long end() {
      return offset + length;
    }

    @Override
    public String toString() {
      return "bytes " + offset + "-" + (end() - 1);
    }
  }

  /**
   * One version of a remote file: its length and the {@code ETag} / {@code Last-Modified} validators the server sent
   * for it, {@code null} when it didn't send one.
   */
  public record RemoteVersion(long length, String etag, String lastModified) {

    /** Returns true if {@code other} has the same length and none of the validators both versions have differ. */
    public boolean matches(RemoteVersion other) {
      return length == other.length && (etag == null || other.etag == null || etag.equals(other.etag)) &&
        (lastModified == null || other.lastModified == null || lastModified.equals(other.lastModified));
    }
  }

  public record RangeResponse(String url, RemoteVersion version, long offset, byte[] bytes) {

    @Override
    @SuppressWarnings("java:S6878")
    public boolean equals(Object o) {
      return this == o || (o instanceof RangeResponse other && Objects.equals(url, other.url) &&
        Objects.equals(version, other.version) && offset == other.offset && Arrays.equals(bytes, other.bytes));
    }

    @Override
    public int hashCode() {
      return Objects.hash(url, version, offset, Arrays.hashCode(bytes));
    }

    @Override
    public String toString() {
      return "RangeResponse[url=" + url + ", version=" + version + ", offset=" + offset + ", length=" + bytes.length +
        "]";
    }
  }

  /** Thrown when a remote file changed on the server while reading it in parts. */
  public static class RemoteFileChangedException extends IllegalStateException {

    public RemoteFileChangedException(String url, String details) {
      super(url + " changed on the server while reading it (" + details +
        "), re-run to read the new version. If this keeps happening, use --download-fail-if-changed=false");
    }
  }

  /**
   * Thrown when a server answers a range request with the whole file, with {@link #version()} from its response
   * headers.
   */
  public static class RangesNotSupportedException extends IOException {

    private final transient RemoteVersion version;

    public RangesNotSupportedException(String url, RemoteVersion version) {
      super(url + " does not support range requests");
      this.version = version;
    }

    /** Returns the version of the whole file the server sent, with length -1 if it didn't send its length. */
    public RemoteVersion version() {
      return version;
    }
  }

  /**
   * An unexpected HTTP response code from the server (or 0 if the response was fine but not usable). 429 and 5xx (but
   * not 501) errors are {@link #retryable()}.
   */
  private static class BadResponseException extends IOException {

    private final String response;
    private final int status;
    private final Duration retryAfter;

    BadResponseException(String url, String response, int status, Duration retryAfter) {
      super("Bad response from " + url + ": " + response);
      this.response = response;
      this.status = status;
      this.retryAfter = retryAfter;
    }

    boolean retryable() {
      // 501 Not Implemented won't change on retry
      return status == 429 || (status >= 500 && status != 501);
    }
  }

  /** Waits {@code wait} before retrying a request; tests override it to retry right away. */
  protected void retrySleep(Duration wait) throws InterruptedException {
    Thread.sleep(wait);
  }

  record ResourceToDownload(
    String id, String url, Path output, CompletableFuture<Long> length, LongAdder progress
  ) {

    ResourceToDownload(String id, String url, Path output) {
      this(id, url, output, new CompletableFuture<>(), new LongAdder());
    }

    public long bytesDownloaded() {
      return progress.sum();
    }
  }
}
