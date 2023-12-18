package com.onthegomap.planetiler.util;

import static com.google.common.net.HttpHeaders.*;
import static java.nio.file.StandardOpenOption.WRITE;

import com.google.common.util.concurrent.RateLimiter;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.stats.Counter;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.worker.RunnableThatThrows;
import com.onthegomap.planetiler.worker.Worker;
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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
 * Downloader.create(PlanetilerConfig.defaults())
 *   .add("natural_earth", "http://url/of/natural_earth.zip", Path.of("natural_earth.zip"))
 *   .add("osm", "http://url/of/file.osm.pbf", Path.of("file.osm.pbf"))
 *   .run();
 * }
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
public class Downloader {

  private static final int MAX_REDIRECTS = 5;
  private static final Logger LOGGER = LoggerFactory.getLogger(Downloader.class);
  private final PlanetilerConfig config;
  private final List<ResourceToDownload> toDownloadList = new ArrayList<>();
  private final HttpClient client;
  private final ExecutorService executor;
  private final long chunkSizeBytes;
  private final ResourceUsage diskSpaceCheck = new ResourceUsage("download");
  private final RateLimiter rateLimiter;

  Downloader(PlanetilerConfig config, long chunkSizeBytes) {
    this.rateLimiter = config.downloadMaxBandwidth() == 0 ? null : RateLimiter.create(config.downloadMaxBandwidth());
    this.chunkSizeBytes = chunkSizeBytes;
    this.config = config;
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

  public static URLConnection getUrlConnection(String urlString, PlanetilerConfig config) throws IOException {
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

  public static InputStream openStreamRange(String urlString, PlanetilerConfig config, long start, long end)
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

    for (var toDownload : toDownloadList) {
      try {
        long size = toDownload.metadata.get(10, TimeUnit.SECONDS).size;
        loggers.addStorageRatePercentCounter(toDownload.id, size, toDownload::bytesDownloaded, true);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Error getting size of " + toDownload.url, e);
      } catch (ExecutionException | TimeoutException e) {
        throw new IllegalStateException("Error getting size of " + toDownload.url, e);
      }
    }
    loggers.add(" ").addProcessStats()
      .awaitAndLog(downloads, config.logInterval());
    executor.shutdown();
  }

  CompletableFuture<Void> downloadIfNecessary(ResourceToDownload resourceToDownload) {
    return CompletableFuture.runAsync(RunnableThatThrows.wrap(() -> {
      LogUtil.setStage("download", resourceToDownload.id);
      long existingSize = FileUtils.size(resourceToDownload.output);
      var metadata = httpHeadFollowRedirects(resourceToDownload.url, 0);
      Path tmpPath = resourceToDownload.tmpPath();
      resourceToDownload.metadata.complete(metadata);
      if (metadata.size == existingSize) {
        LOGGER.info("Skipping {}: {} already up-to-date", resourceToDownload.id, resourceToDownload.output);
        return;
      }
      try {
        String redirectInfo = metadata.canonicalUrl.equals(resourceToDownload.url) ? "" :
          " (redirected to " + metadata.canonicalUrl + ")";
        LOGGER.info("Downloading {}{} to {}", resourceToDownload.url, redirectInfo, resourceToDownload.output);
        FileUtils.delete(resourceToDownload.output);
        FileUtils.createParentDirectories(resourceToDownload.output);
        FileUtils.delete(tmpPath);
        FileUtils.deleteOnExit(tmpPath);
        diskSpaceCheck.addDisk(tmpPath, metadata.size, resourceToDownload.id);
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
    return client.send(newHttpRequest(url).HEAD().build(),
      responseInfo -> {
        int status = responseInfo.statusCode();
        Optional<String> location = Optional.empty();
        long contentLength = 0;
        HttpHeaders headers = responseInfo.headers();
        if (status >= 300 && status < 400) {
          location = responseInfo.headers().firstValue(LOCATION);
          if (location.isEmpty()) {
            throw new IllegalStateException("Received " + status + " but no location header from " + url);
          }
        } else if (responseInfo.statusCode() != 200) {
          throw new IllegalStateException("Bad response: " + responseInfo.statusCode());
        } else {
          contentLength = headers.firstValueAsLong(CONTENT_LENGTH).orElseThrow();
        }
        boolean supportsRangeRequest = headers.allValues(ACCEPT_RANGES).contains("bytes");
        ResourceMetadata metadata = new ResourceMetadata(location, url, contentLength, supportsRangeRequest);
        return HttpResponse.BodyHandlers.replacing(metadata).apply(responseInfo);
      }).body();
  }

  private void httpDownload(ResourceToDownload resource, Path tmpPath)
    throws ExecutionException, InterruptedException {
    var metadata = resource.metadata().get();
    String canonicalUrl = metadata.canonicalUrl();
    record Range(long start, long end) {}
    List<Range> chunks = new ArrayList<>();
    boolean ranges = metadata.acceptRange && config.downloadThreads() > 1;
    long chunkSize = ranges ? chunkSizeBytes : metadata.size;
    for (long start = 0; start < metadata.size; start += chunkSize) {
      long end = Math.min(start + chunkSize, metadata.size);
      chunks.add(new Range(start, end));
    }
    FileUtils.setLength(tmpPath, metadata.size);
    Semaphore perFileLimiter = new Semaphore(config.downloadThreads());
    Worker.joinFutures(chunks.stream().map(range -> CompletableFuture.runAsync(RunnableThatThrows.wrap(() -> {
      LogUtil.setStage("download", resource.id);
      perFileLimiter.acquire();
      var counter = resource.progress.counterForThread();
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
      } finally {
        perFileLimiter.release();
      }
    }), executor)).toArray(CompletableFuture[]::new)).get();
  }

  private HttpRequest.Builder newHttpRequest(String url) {
    return HttpRequest.newBuilder(URI.create(url))
      .timeout(config.httpTimeout())
      .header(USER_AGENT, config.httpUserAgent());
  }

  record ResourceMetadata(Optional<String> redirect, String canonicalUrl, long size, boolean acceptRange) {}

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
