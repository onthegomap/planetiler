package com.onthegomap.planetiler.util;

import static com.google.common.net.HttpHeaders.*;
import static java.nio.file.StandardOpenOption.WRITE;

import com.google.common.util.concurrent.RateLimiter;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URLConnection;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
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
  private final HttpClient client = HttpClient.newBuilder()
    // explicitly follow redirects to capture final redirect url
    .followRedirects(HttpClient.Redirect.NEVER).build();
  private final ExecutorService executor;
  private final Stats stats;
  private final long chunkSizeBytes;
  private final ResourceUsage diskSpaceCheck = new ResourceUsage("download");
  private final RateLimiter rateLimiter;

  Downloader(PlanetilerConfig config, Stats stats, long chunkSizeBytes) {
    this.rateLimiter = config.downloadMaxBandwidth() == 0 ? null : RateLimiter.create(config.downloadMaxBandwidth());
    this.chunkSizeBytes = chunkSizeBytes;
    this.config = config;
    this.stats = stats;
    this.executor = Executors.newSingleThreadExecutor(runnable -> {
      Thread thread = new Thread(() -> {
        LogUtil.setStage("download");
        runnable.run();
      });
      thread.setDaemon(true);
      return thread;
    });
  }

  public static Downloader create(PlanetilerConfig config, Stats stats) {
    return new Downloader(config, stats, config.downloadChunkSizeMB() * 1_000_000L);
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
    long existingSize = FileUtils.size(resourceToDownload.output);

    return httpHeadFollowRedirects(resourceToDownload.url, 0)
      .whenComplete((metadata, err) -> {
        if (metadata != null) {
          resourceToDownload.metadata.complete(metadata);
        } else {
          resourceToDownload.metadata.completeExceptionally(err);
        }
      })
      .thenComposeAsync(metadata -> {
        if (metadata.size == existingSize) {
          LOGGER.info("Skipping {}: {} already up-to-date", resourceToDownload.id, resourceToDownload.output);
          return CompletableFuture.completedFuture(null);
        } else {
          String redirectInfo = metadata.canonicalUrl.equals(resourceToDownload.url) ? "" :
            " (redirected to " + metadata.canonicalUrl + ")";
          LOGGER.info("Downloading {}{} to {}", resourceToDownload.url, redirectInfo, resourceToDownload.output);
          FileUtils.delete(resourceToDownload.output);
          FileUtils.createParentDirectories(resourceToDownload.output);
          Path tmpPath = resourceToDownload.tmpPath();
          FileUtils.delete(tmpPath);
          FileUtils.deleteOnExit(tmpPath);
          diskSpaceCheck.addDisk(tmpPath, metadata.size, resourceToDownload.id);
          diskSpaceCheck.checkAgainstLimits(config.force(), false);
          return httpDownload(resourceToDownload, tmpPath)
            .thenCompose(result -> {
              try {
                Files.move(tmpPath, resourceToDownload.output);
                return CompletableFuture.completedFuture(null);
              } catch (IOException e) {
                return CompletableFuture.<Void>failedFuture(e);
              }
            })
            .whenCompleteAsync((result, error) -> {
              if (error != null) {
                LOGGER.error("Error downloading {} to {}", resourceToDownload.url, resourceToDownload.output, error);
              } else {
                LOGGER.info("Finished downloading {} to {}", resourceToDownload.url, resourceToDownload.output);
              }
              FileUtils.delete(tmpPath);
            }, executor);
        }
      }, executor);
  }

  private CompletableFuture<ResourceMetadata> httpHeadFollowRedirects(String url, int redirects) {
    if (redirects > MAX_REDIRECTS) {
      throw new IllegalStateException("Exceeded " + redirects + " redirects for " + url);
    }
    return httpHead(url).thenComposeAsync(response -> response.redirect.isPresent() ?
      httpHeadFollowRedirects(response.redirect.get(), redirects + 1) : CompletableFuture.completedFuture(response));
  }

  CompletableFuture<ResourceMetadata> httpHead(String url) {
    return client
      .sendAsync(newHttpRequest(url).HEAD().build(),
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
        })
      .thenApply(HttpResponse::body);
  }

  private CompletableFuture<?> httpDownload(ResourceToDownload resource, Path tmpPath) {
    /*
     * Alternative using async HTTP client:
     *
     *   return client.sendAsync(newHttpRequest(url).GET().build(), responseInfo -> {
     *     assertOK(responseInfo);
     *     return HttpResponse.BodyHandlers.ofFile(path).apply(responseInfo);
     *
     * But it is slower on large files
     */
    return resource.metadata.thenCompose(metadata -> {
      String canonicalUrl = metadata.canonicalUrl;
      record Range(long start, long end) {

        long size() {
          return end - start;
        }
      }
      List<Range> chunks = new ArrayList<>();
      boolean ranges = metadata.acceptRange && config.downloadThreads() > 1;
      long chunkSize = ranges ? chunkSizeBytes : metadata.size;
      for (long start = 0; start < metadata.size; start += chunkSize) {
        long end = Math.min(start + chunkSize, metadata.size);
        chunks.add(new Range(start, end));
      }
      // create an empty file
      try {
        Files.createFile(tmpPath);
      } catch (IOException e) {
        return CompletableFuture.failedFuture(new IOException("Failed to create " + resource.output, e));
      }
      return WorkerPipeline.start("download-" + resource.id, stats)
        .readFromTiny("chunks", chunks)
        .sinkToConsumer("chunk-downloader", Math.min(config.downloadThreads(), chunks.size()), range -> {
          try (var fileChannel = FileChannel.open(tmpPath, WRITE)) {
            while (range.size() > 0) {
              try (
                var inputStream = (ranges || range.start > 0) ? openStreamRange(canonicalUrl, range.start, range.end) :
                  openStream(canonicalUrl);
                var input = new ProgressChannel(Channels.newChannel(inputStream), resource.progress, rateLimiter)
              ) {
                // ensure this file has been allocated up to the start of this block
                fileChannel.write(ByteBuffer.allocate(1), range.start);
                fileChannel.position(range.start);
                long transferred = fileChannel.transferFrom(input, range.start, range.size());
                if (transferred == 0) {
                  throw new IOException("Transferred 0 bytes but " + range.size() + " expected: " + canonicalUrl);
                } else if (transferred != range.size() && !metadata.acceptRange) {
                  throw new IOException(
                    "Transferred " + transferred + " bytes but " + range.size() + " expected: " + canonicalUrl +
                      " and server does not support range requests");
                }
                range = new Range(range.start + transferred, range.end);
              }
            }
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        }).done();
    });
  }

  private HttpRequest.Builder newHttpRequest(String url) {
    return HttpRequest.newBuilder(URI.create(url))
      .timeout(config.httpTimeout())
      .header(USER_AGENT, config.httpUserAgent());
  }

  record ResourceMetadata(Optional<String> redirect, String canonicalUrl, long size, boolean acceptRange) {}

  record ResourceToDownload(
    String id, String url, Path output, CompletableFuture<ResourceMetadata> metadata, AtomicLong progress
  ) {

    ResourceToDownload(String id, String url, Path output) {
      this(id, url, output, new CompletableFuture<>(), new AtomicLong(0));
    }

    public Path tmpPath() {
      return output.resolveSibling(output.getFileName() + "_inprogress");
    }

    public long bytesDownloaded() {
      return progress.get();
    }
  }

  /**
   * Wrapper for a {@link ReadableByteChannel} that captures progress information.
   */
  private record ProgressChannel(ReadableByteChannel inner, AtomicLong progress, RateLimiter rateLimiter)
    implements ReadableByteChannel {

    @Override
    public int read(ByteBuffer dst) throws IOException {
      int n = inner.read(dst);
      if (n > 0) {
        if (rateLimiter != null) {
          rateLimiter.acquire(n);
        }
        progress.addAndGet(n);
      }
      return n;
    }

    @Override
    public boolean isOpen() {
      return inner.isOpen();
    }

    @Override
    public void close() throws IOException {
      inner.close();
    }
  }
}
