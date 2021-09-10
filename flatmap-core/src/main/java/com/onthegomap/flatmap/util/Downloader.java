package com.onthegomap.flatmap.util;

import static com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static com.google.common.net.HttpHeaders.USER_AGENT;

import com.onthegomap.flatmap.config.FlatmapConfig;
import com.onthegomap.flatmap.stats.ProgressLoggers;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
 * <pre>{@code
 * Downloader.create(FlatmapConfig.defaults())
 *   .add("natural_earth", "http://url/of/natural_earth.zip", Path.of("natural_earth.zip"))
 *   .add("osm", "http://url/of/file.osm.pbf", Path.of("file.osm.pbf"))
 *   .start();
 * }</pre>
 * <p>
 * As a shortcut to find the URL of a file to download from the <a href="https://download.geofabrik.de/">Geofabrik
 * download site</a>, you can use "geofabrik:extract name" (i.e. "geofabrik:monaco" or "geofabrik:australia") to look up
 * a {@code .osm.pbf} download URL in the <a href="https://download.geofabrik.de/technical.html">Geofabrik JSON
 * index</a>.
 */
@SuppressWarnings("UnusedReturnValue")
public class Downloader {

  private static final Logger LOGGER = LoggerFactory.getLogger(Downloader.class);
  private final FlatmapConfig config;
  private final List<ResourceToDownload> toDownloadList = new ArrayList<>();
  private final HttpClient client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build();
  private final ExecutorService executor;

  private Downloader(FlatmapConfig config) {
    this.config = config;
    this.executor = Executors.newSingleThreadExecutor((runnable) -> {
      Thread thread = new Thread(() -> {
        LogUtil.setStage("download");
        runnable.run();
      });
      thread.setDaemon(true);
      return thread;
    });
  }

  public static Downloader create(FlatmapConfig config) {
    return new Downloader(config);
  }

  private static void assertOK(HttpResponse.ResponseInfo responseInfo) {
    if (responseInfo.statusCode() != 200) {
      throw new IllegalStateException("Bad response: " + responseInfo.statusCode());
    }
  }

  /**
   * Adds a new resource to download but does not start downloading it until {@link #run()} is called.
   * <p>
   * The resource won't be downloaded if size on disk is the same as {@code Content-Length} header reported from a
   * {@code HEAD} request to the resource.
   *
   * @param id     short name to use for this download when logging progress
   * @param url    the external resource to fetch, or "geofabrik:extract name" as a shortcut to use {@link
   *               Geofabrik#getDownloadUrl(String)} to lookup a {@code .osm.pbf} <a href="https://download.geofabrik.de/">Geofabrik</a>
   *               extract URL by partial match on area name
   * @param output where to download the file to
   * @return {@code this} for chaining
   */
  public Downloader add(String id, String url, Path output) {
    if (url.startsWith("geofabrik:")) {
      url = Geofabrik.getDownloadUrl(url.replaceFirst("^geofabrik:", ""));
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
        long size = toDownload.size.get(10, TimeUnit.SECONDS);
        loggers.addRatePercentCounter(toDownload.id, size, toDownload::bytesDownloaded);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        throw new IllegalStateException("Error getting size of " + toDownload.url, e);
      }
    }
    loggers.awaitAndLog(downloads, config.logInterval());
    executor.shutdown();
  }

  private CompletableFuture<?> downloadIfNecessary(ResourceToDownload resourceToDownload) {
    long existingSize = FileUtils.size(resourceToDownload.output);

    return httpHeadContentLength(resourceToDownload)
      .whenComplete((size, err) -> {
        if (size != null) {
          resourceToDownload.size.complete(size);
        } else {
          resourceToDownload.size.completeExceptionally(err);
        }
      })
      .thenComposeAsync(size -> {
        if (size == existingSize) {
          LOGGER.info("Skipping " + resourceToDownload.id + ": " + resourceToDownload.output + " already up-to-date");
          return CompletableFuture.completedFuture(null);
        } else {
          LOGGER.info("Downloading " + resourceToDownload.url + " to " + resourceToDownload.output);
          FileUtils.delete(resourceToDownload.output);
          FileUtils.createParentDirectories(resourceToDownload.output);
          Path tmpPath = resourceToDownload.tmpPath();
          FileUtils.delete(tmpPath);
          FileUtils.deleteOnExit(tmpPath);
          return httpDownload(resourceToDownload.url, tmpPath)
            .thenCompose(result -> {
              try {
                Files.move(tmpPath, resourceToDownload.output);
                return CompletableFuture.completedFuture(result);
              } catch (IOException e) {
                return CompletableFuture.failedFuture(e);
              }
            })
            .whenCompleteAsync((result, error) -> {
              if (result != null) {
                LOGGER.info("Finished downloading " + resourceToDownload.url + " to " + resourceToDownload.output);
              } else if (error != null) {
                LOGGER.error("Error downloading " + resourceToDownload.url + " to " + resourceToDownload.output, error);
              }
              FileUtils.delete(tmpPath);
            }, executor);
        }
      }, executor);
  }

  private CompletableFuture<Long> httpHeadContentLength(ResourceToDownload resourceToDownload) {
    return client
      .sendAsync(newHttpRequest(resourceToDownload.url).method("HEAD", HttpRequest.BodyPublishers.noBody()).build(),
        responseInfo -> {
          assertOK(responseInfo);
          long contentLength = responseInfo.headers().firstValueAsLong(CONTENT_LENGTH).orElseThrow();
          return HttpResponse.BodyHandlers.replacing(contentLength).apply(responseInfo);
        }).thenApply(HttpResponse::body);
  }

  private CompletableFuture<?> httpDownload(String url, Path path) {
    return client.sendAsync(newHttpRequest(url).GET().build(), responseInfo -> {
      assertOK(responseInfo);
      return HttpResponse.BodyHandlers.ofFile(path).apply(responseInfo);
    });
  }

  private HttpRequest.Builder newHttpRequest(String url) {
    return HttpRequest.newBuilder(URI.create(url))
      .timeout(Duration.ofSeconds(30))
      .header(USER_AGENT, config.httpUserAgent());
  }

  private static record ResourceToDownload(String id, String url, Path output, CompletableFuture<Long> size) {

    ResourceToDownload(String id, String url, Path output) {
      this(id, url, output, new CompletableFuture<>());
    }

    public Path tmpPath() {
      return output.resolveSibling(output.getFileName() + "_inprogress");
    }

    public long bytesDownloaded() {
      return Files.exists(output) ? FileUtils.size(output) : FileUtils.size(tmpPath());
    }
  }
}
