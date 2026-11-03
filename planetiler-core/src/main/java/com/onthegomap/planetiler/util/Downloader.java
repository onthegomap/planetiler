package com.onthegomap.planetiler.util;

import static com.google.common.net.HttpHeaders.*;
import static java.nio.file.StandardOpenOption.WRITE;

import com.google.common.util.concurrent.RateLimiter;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.stats.Counter;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.worker.RunnableThatThrows;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLConnection;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility for downloading files to disk in parallel over HTTP.
 * <p>
 * After downloading a file once, it won't be downloaded again unless the {@code Content-Length} of the resource
 * changes.
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
  private static final Pattern CONTENT_RANGE_PATTERN = Pattern.compile("bytes (\\d+)-(\\d+)/(\\d+)");
  private static final Logger LOGGER = LoggerFactory.getLogger(Downloader.class);
  private final PlanetilerConfig config;
  private final List<ResourceToDownload> toDownloadList = new ArrayList<>();
  private final HttpClient client;
  private final ExecutorService executor;
  private final long chunkSizeBytes;
  private final ResourceUsage diskSpaceCheck = new ResourceUsage("download");
  private final RateLimiter rateLimiter;
  private final Map<String, Semaphore> perFileLimiters = new ConcurrentHashMap<>();
  private final Semaphore rangeRequestLimiter;

  Downloader(PlanetilerConfig config, long chunkSizeBytes) {
    this.rateLimiter = config.downloadMaxBandwidth() == 0 ? null : RateLimiter.create(config.downloadMaxBandwidth());
    this.chunkSizeBytes = chunkSizeBytes;
    this.config = config;
    this.rangeRequestLimiter = new Semaphore(config.downloadRangeConcurrency());
    this.executor = Executors.newVirtualThreadPerTaskExecutor();
    this.client = HttpClient.newBuilder()
      // explicitly follow redirects to capture final redirect url
      .followRedirects(HttpClient.Redirect.NEVER)
      .executor(executor)
      .build();
  }

  public static Downloader create(PlanetilerConfig config) {
    return new Downloader(config, config.downloadChunkSizeMB() * 1_000_000L);
  }

  private static URLConnection getUrlConnection(String urlString, PlanetilerConfig config) throws IOException {
    var url = URI.create(urlString).toURL();
    var connection = url.openConnection();
    connection.setConnectTimeout((int) config.httpTimeout().toMillis());
    connection.setReadTimeout((int) config.httpTimeout().toMillis());
    connection.setRequestProperty(USER_AGENT, config.httpUserAgent());
    return connection;
  }

  /**
   * Returns an input stream reading from a remote URL with timeout and user-agent set from planetiler config.
   *
   * @param urlString remote URL
   * @param config    planetiler config containing the user agent and timeout parameter
   * @return an input stream that will read from the remote URL
   * @throws IOException if an error occurs making the network request
   */
  public static InputStream openStream(String urlString, PlanetilerConfig config) throws IOException {
    return getUrlConnection(urlString, config).getInputStream();
  }

  private static InputStream openStreamRange(String urlString, PlanetilerConfig config, long start, long end)
    throws IOException {
    URLConnection connection = getUrlConnection(urlString, config);
    connection.setRequestProperty(RANGE, "bytes=%d-%d".formatted(start, end));
    return connection.getInputStream();
  }

  InputStream openStream(String url) throws IOException {
    return openStream(url, config);
  }

  InputStream openStreamRange(String url, long start, long end) throws IOException {
    return openStreamRange(url, config, start, end);
  }

  /**
   * Adds a new resource to download but does not start downloading it until {@link #run()} is called.
   * <p>
   * The resource won't be downloaded if size on disk is the same as {@code Content-Length} header reported from a
   * {@code HEAD} request to the resource.
   *
   * @param id     short name to use for this download when logging progress
   * @param url    the external resource to fetch, "aws:latest" (for the latest planet .osm.pbf), "overture:latest" (for
   *               the latest Overture Maps release) or "geofabrik:extract-name" as a shortcut to use
   *               {@link Geofabrik#getDownloadUrl(String, PlanetilerConfig)} to look up a {@code .osm.pbf}
   *               <a href="https://download.geofabrik.de/">Geofabrik</a> extract URL by partial match on area name
   * @param output where to download the file to
   * @return {@code this} for chaining
   */
  public Downloader add(String id, String url, Path output) {
    if (url.startsWith("geofabrik:")) {
      url = Geofabrik.getDownloadUrl(url.replaceFirst("^geofabrik:", ""), config);
    } else if (url.startsWith("aws:")) {
      url = AwsOsm.OSM_PDS.getDownloadUrl(url.replaceFirst("^aws:", ""), config);
    } else if (url.startsWith("overture:")) {
      url = AwsOsm.OVERTURE.getDownloadUrl(url.replaceFirst("^overture:", ""), config);
    }
    toDownloadList.add(new ResourceToDownload(id, url, output));
    return this;
  }

  /**
   * Starts downloading all resources in parallel, logging progress until complete.
   *
   * @throws IllegalStateException if an error occurs downloading any resource, will be thrown after all resources
   *                               finish
   */
  public void run() {
    var downloads = CompletableFuture
      .allOf(toDownloadList.stream()
        .map(this::downloadIfNecessary)
        .toArray(CompletableFuture[]::new)
      );

    ProgressLoggers loggers = ProgressLoggers.create();

    var groupedById = toDownloadList.stream().collect(Collectors.groupingBy(ResourceToDownload::id));

    for (var group : groupedById.entrySet()) {
      long size = 0;
      for (var item : group.getValue()) {
        try {
          size += item.metadata.get(config.maxHttpRequestDuration().toMillis(), TimeUnit.MILLISECONDS).size.orElse(0);
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

  CompletableFuture<Void> downloadIfNecessary(ResourceToDownload resourceToDownload) {
    return CompletableFuture.runAsync(RunnableThatThrows.wrap(() -> {
      LogUtil.setStage("download", resourceToDownload.id);
      long existingSize = FileUtils.size(resourceToDownload.output);
      try {
        resourceToDownload.metadata.complete(httpHeadFollowRedirects(resourceToDownload.url, 0));
      } catch (Exception e) {
        resourceToDownload.metadata.completeExceptionally(e);
        throw e;
      }
      Path tmpPath = resourceToDownload.tmpPath();
      try {
        var metadata = resourceToDownload.metadata.get();
        if (metadata.size.orElse(-1) == existingSize) {
          LOGGER.info("Skipping {}: {} already up-to-date", resourceToDownload.id, resourceToDownload.output);
          return;
        }
        String redirectInfo = metadata.canonicalUrl.equals(resourceToDownload.url) ? "" :
          " (redirected to " + metadata.canonicalUrl + ")";
        LOGGER.info("Downloading {}{} to {}", resourceToDownload.url, redirectInfo, resourceToDownload.output);
        FileUtils.delete(resourceToDownload.output);
        FileUtils.createParentDirectories(resourceToDownload.output);
        FileUtils.delete(tmpPath);
        FileUtils.deleteOnExit(tmpPath);
        if (metadata.size.isPresent()) {
          diskSpaceCheck.addDisk(tmpPath, metadata.size.getAsLong(), resourceToDownload.id);
        }
        diskSpaceCheck.checkAgainstLimits(config.force(), false);
        httpDownload(resourceToDownload, tmpPath);
        Files.move(tmpPath, resourceToDownload.output);
        LOGGER.info("Finished downloading {} to {}", resourceToDownload.url, resourceToDownload.output);
      } catch (Exception e) { // NOSONAR
        LOGGER.error("Error downloading {} to {}", resourceToDownload.url, resourceToDownload.output, e);
        throw e;
      } finally {
        FileUtils.delete(tmpPath);
      }
    }), executor);
  }

  private ResourceMetadata httpHeadFollowRedirects(String url, int redirects) throws IOException, InterruptedException {
    if (redirects > MAX_REDIRECTS) {
      throw new IllegalStateException("Exceeded " + redirects + " redirects for " + url);
    }
    var response = httpHead(url);
    return response.redirect.isPresent() ? httpHeadFollowRedirects(response.redirect.get(), redirects + 1) : response;
  }

  ResourceMetadata httpHead(String url) throws IOException, InterruptedException {
    for (int retry = 0; retry <= config.httpRetries(); retry++) {
      boolean lastTry = retry == config.httpRetries();
      int retriesRemaining = config.httpRetries() - retry;
      try {
        return client.send(newHttpRequest(url).HEAD().build(),
          responseInfo -> {
            int status = responseInfo.statusCode();
            Optional<String> location = Optional.empty();
            OptionalLong contentLength = OptionalLong.empty();
            HttpHeaders headers = responseInfo.headers();
            if (status >= 300 && status < 400) {
              location = responseInfo.headers().firstValue(LOCATION);
              if (location.isEmpty()) {
                throw new IllegalStateException("Received " + status + " but no location header from " + url);
              }
            } else if (responseInfo.statusCode() != 200) {
              throw new IllegalStateException("Bad response: " + responseInfo.statusCode());
            } else {
              contentLength = headers.firstValueAsLong(CONTENT_LENGTH);
            }
            boolean supportsRangeRequest = headers.allValues(ACCEPT_RANGES).contains("bytes");
            ResourceMetadata metadata = new ResourceMetadata(location, url, contentLength, supportsRangeRequest);
            return HttpResponse.BodyHandlers.replacing(metadata).apply(responseInfo);
          }).body();
      } catch (IOException e) {
        if (lastTry) {
          throw e;
        } else {
          LOGGER.warn("Error fetching head for {}, retries remaining: {} {}", url, retriesRemaining, e.getMessage());
          retrySleep();
        }
      }
    }
    throw new IllegalStateException();
  }

  private void httpDownload(ResourceToDownload resource, Path tmpPath)
    throws ExecutionException, InterruptedException {
    var metadata = resource.metadata().get();
    String canonicalUrl = metadata.canonicalUrl();
    record Range(long start, long end) {}
    List<Range> chunks = new ArrayList<>();
    boolean ranges = metadata.acceptRange && config.downloadThreads() > 1;
    long fileSize = metadata.size.orElse(Long.MAX_VALUE);
    long chunkSize = ranges ? chunkSizeBytes : fileSize;
    for (long start = 0; start < fileSize; start += chunkSize) {
      long end = Math.min(start + chunkSize, fileSize);
      chunks.add(new Range(start, end));
    }
    FileUtils.setLength(tmpPath, metadata.size.orElse(1));
    Semaphore perFileLimiter =
      perFileLimiters.computeIfAbsent(resource.id(), id -> new Semaphore(config.downloadThreads()));
    CompletableFutures
      .joinFutures(chunks.stream().map(range -> CompletableFuture.runAsync(RunnableThatThrows.wrap(() -> {
        LogUtil.setStage("download", resource.id);
        perFileLimiter.acquire();
        try {
          var counter = resource.progress.counterForThread();
          for (int retry = 0; retry <= config.httpRetries(); retry++) {
            boolean lastTry = retry == config.httpRetries();
            int retriesRemaining = config.httpRetries() - retry;
            int countToRewind = 0;
            try (
              var fc = FileChannel.open(tmpPath, WRITE);
              var inputStream = (ranges || range.start > 0) ?
                openStreamRange(canonicalUrl, range.start, range.end) :
                openStream(canonicalUrl);
            ) {
              long offset = range.start;
              byte[] buffer = new byte[16384];
              int read;
              while (offset < range.end && (read = inputStream.read(buffer, 0, 16384)) >= 0) {
                counter.incBy(read);
                countToRewind += read;
                if (rateLimiter != null) {
                  rateLimiter.acquire(read);
                }
                int position = 0;
                int remaining = read;
                while (remaining > 0) {
                  int written = fc.write(ByteBuffer.wrap(buffer, position, remaining), offset);
                  if (written <= 0) {
                    throw new IOException("Failed to write to " + tmpPath);
                  }
                  position += written;
                  remaining -= written;
                  offset += written;
                }
              }
              if (offset < range.end && range.end != Long.MAX_VALUE) {
                throw new IOException("Unexpected EOF at " + offset + "/" + range.end);
              }
              // successfully downloaded file
              break;
            } catch (IOException e) {
              if (lastTry) {
                throw e;
              } else {
                counter.incBy(-countToRewind);
                LOGGER.warn("Error downloading {}, retries remaining: {} {}", canonicalUrl, retriesRemaining,
                  e.getMessage());
                retrySleep();
              }
            }
          }
        } finally {
          perFileLimiter.release();
        }
      }), executor)).toArray(CompletableFuture[]::new)).get();
  }

  /**
   * Fetches the last {@code length} bytes of {@code url} (or all of it if it is shorter) with one range request,
   * following redirects, or three for servers that don't support suffix ranges ({@code bytes=-length}).
   *
   * @throws IOException           if the request fails after all retries
   * @throws IllegalStateException if the server does not support range requests
   */
  public RangeResponse fetchTail(String url, int length) throws IOException {
    try {
      return sendRangeRequest(url, null, "bytes=-" + length);
    } catch (IllegalStateException _) {
      // some servers don't support suffix ranges: get the file's length from its first byte, then ask for the tail
      long fileLength = sendRangeRequest(url, null, "bytes=0-0").version().length();
      return sendRangeRequest(url, null, "bytes=" + Math.max(0, fileLength - length) + "-" + (fileLength - 1));
    }
  }

  /**
   * Starts fetching {@code length} bytes at {@code offset} of {@code url} with one range request, following redirects.
   * <p>
   * If {@code expected} is not null, the request only succeeds if the remote file is still that version: it sends
   * {@code If-Match} with a strong ETag, or else {@code If-Unmodified-Since} with the Last-Modified date. The future
   * fails with an {@link IOException} if the request fails after all retries, a {@link RemoteFileChangedException} if
   * the server reports that the file changed, or an {@link IllegalStateException} if the server does not support range
   * requests.
   */
  public CompletableFuture<RangeResponse> fetchRange(String url, RemoteVersion expected, long offset, int length) {
    return supplyAsync(() -> sendRangeRequest(url, expected, "bytes=%d-%d".formatted(offset, offset + length - 1)));
  }

  /**
   * Starts {@code task}, which makes requests through this downloader, on a virtual thread of its own, so that tasks
   * that mostly wait for the network can run at once.
   * <p>
   * The future fails with whatever {@code task} throws.
   */
  public <T> CompletableFuture<T> supplyAsync(Callable<T> task) {
    return CompletableFutures.supplyAsync(task, executor);
  }

  /** Returns the config this downloader was created with. */
  public PlanetilerConfig config() {
    return config;
  }

  private RangeResponse sendRangeRequest(String url, RemoteVersion expected, String range) throws IOException {
    for (int redirects = 0; redirects <= MAX_REDIRECTS; redirects++) {
      var request = newHttpRequest(url).header(RANGE, range);
      if (expected != null) {
        addPreconditions(request, expected);
      }
      var attempt = sendWithRetries(url, request.build(), range);
      if (attempt.redirect == null) {
        return attempt.response;
      }
      url = attempt.redirect;
    }
    throw new IllegalStateException("Exceeded " + MAX_REDIRECTS + " redirects for " + url);
  }

  private static void addPreconditions(HttpRequest.Builder request, RemoteVersion expected) {
    // make the server reject the request if the file changed instead of returning bytes from the new version
    if (expected.etag() != null && !expected.etag().startsWith("W/")) {
      request.header(IF_MATCH, expected.etag());
    } else if (expected.lastModified() != null) {
      // A weak ETag (W/"...") only promises equivalent content, not identical bytes, so it can't guarantee that ranges
      // fetched at different times fit together, and servers must reject it in If-Match (RFC 9110 13.1.1).
      request.header(IF_UNMODIFIED_SINCE, expected.lastModified());
    }
  }

  private record Attempt(String redirect, RangeResponse response) {}

  private Attempt sendWithRetries(String url, HttpRequest request, String range) throws IOException {
    try {
      return sendWithRetriesInterruptibly(url, request, range);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
  }

  private Attempt sendWithRetriesInterruptibly(String url, HttpRequest request, String range)
    throws IOException, InterruptedException {
    for (int retry = 0;; retry++) {
      try {
        return sendWithLimit(url, request);
      } catch (IOException e) {
        int retriesRemaining = config.httpRetries() - retry;
        if (retriesRemaining <= 0) {
          throw e;
        }
        LOGGER.warn("Error fetching {} of {}, retries remaining: {} {}", range, url, retriesRemaining, e.getMessage());
      }
      retrySleep();
    }
  }

  private Attempt sendWithLimit(String url, HttpRequest request) throws IOException, InterruptedException {
    rangeRequestLimiter.acquire();
    try {
      var response = send(request);
      try (var body = response.body()) {
        return parseRangeResponse(url, response, body);
      }
    } finally {
      rangeRequestLimiter.release();
    }
  }

  /** Sends a range request; tests override this to simulate a server. */
  HttpResponse<InputStream> send(HttpRequest request) throws IOException, InterruptedException {
    return client.send(request, HttpResponse.BodyHandlers.ofInputStream());
  }

  private Attempt parseRangeResponse(String url, HttpResponse<InputStream> response, InputStream body)
    throws IOException {
    int status = response.statusCode();
    var headers = response.headers();
    if (status >= 300 && status < 400) {
      String location = headers.firstValue(LOCATION)
        .orElseThrow(() -> new IllegalStateException("Received " + status + " but no location header from " + url));
      return new Attempt(URI.create(url).resolve(location).toString(), null);
    } else if (status == 412) {
      throw new RemoteFileChangedException(url, "this file changed on the server mid-download");
    } else if (status == 200) {
      throw new IllegalStateException(url + " does not support range requests");
    } else if (status == 429 || status >= 500) {
      throw new RetryableHttpException("Bad response: " + status);
    } else if (status != 206) {
      throw new IllegalStateException("Bad response from " + url + ": " + status);
    }
    // Content-Range: bytes <start>-<end>/<length>
    String contentRange = headers.firstValue(CONTENT_RANGE).orElse("");
    var matcher = CONTENT_RANGE_PATTERN.matcher(contentRange);
    if (!matcher.matches()) {
      throw new IllegalStateException("Bad Content-Range from " + url + ": " + contentRange);
    }
    long start = Long.parseLong(matcher.group(1));
    long end = Long.parseLong(matcher.group(2));
    long fileLength = Long.parseLong(matcher.group(3));
    byte[] bytes = new byte[Math.toIntExact(end - start + 1)];
    int read = 0;
    while (read < bytes.length) {
      int n = body.read(bytes, read, Math.min(16384, bytes.length - read));
      if (n < 0) {
        throw new IOException("Unexpected EOF at " + (start + read) + "/" + (end + 1) + " of " + url);
      }
      if (rateLimiter != null) {
        rateLimiter.acquire(n);
      }
      read += n;
    }
    var version =
      new RemoteVersion(fileLength, headers.firstValue(ETAG).orElse(null),
        headers.firstValue(LAST_MODIFIED).orElse(null));
    return new Attempt(null, new RangeResponse(url, version, start, bytes));
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

  /**
   * The bytes starting at {@code offset} of the remote file at {@code url} (after redirects) and the version of the
   * file the server read them from.
   */
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

  private static class RetryableHttpException extends IOException {
    RetryableHttpException(String message) {
      super(message);
    }
  }

  protected void retrySleep() throws InterruptedException {
    Thread.sleep(config.httpRetryWait());
  }

  private HttpRequest.Builder newHttpRequest(String url) {
    return HttpRequest.newBuilder(URI.create(url))
      .timeout(config.httpTimeout())
      .header(USER_AGENT, config.httpUserAgent());
  }

  record ResourceMetadata(Optional<String> redirect, String canonicalUrl, OptionalLong size, boolean acceptRange) {}

  record ResourceToDownload(
    String id, String url, Path output, CompletableFuture<ResourceMetadata> metadata,
    Counter.MultiThreadCounter progress
  ) {

    ResourceToDownload(String id, String url, Path output) {
      this(id, url, output, new CompletableFuture<>(), Counter.newMultiThreadCounter());
    }

    public Path tmpPath() {
      return output.resolveSibling(output.getFileName() + "_inprogress");
    }

    public long bytesDownloaded() {
      return progress.get();
    }
  }
}
