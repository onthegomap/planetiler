package com.onthegomap.flatmap;

import static com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static com.google.common.net.HttpHeaders.USER_AGENT;

import com.onthegomap.flatmap.monitoring.ProgressLoggers;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility for downloading files to disk over HTTP.
 */
public class Download {

  private static final Logger LOGGER = LoggerFactory.getLogger(Download.class);
  private final CommonParams config;
  private final List<ToDownload> toDownloadList = new ArrayList<>();
  private final HttpClient client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build();

  private Download(CommonParams config) {
    this.config = config;
  }

  private static record ToDownload(String id, String url, Path output, CompletableFuture<Long> size) {

    ToDownload(String id, String url, Path output) {
      this(id, url, output, new CompletableFuture<>());
    }
  }

  public static Download create(CommonParams params) {
    return new Download(params);
  }

  public Download add(String id, String url, Path output) {
    toDownloadList.add(new ToDownload(id, url, output));
    return this;
  }

  public void start() {
    var downloads = CompletableFuture
      .allOf(toDownloadList.stream().map(this::downloadIfNecessary).toArray(CompletableFuture[]::new));

    ProgressLoggers loggers = new ProgressLoggers("download");

    for (var toDownload : toDownloadList) {
      try {
        long size = toDownload.size.get();
        loggers.addRatePercentCounter(toDownload.id, size, () -> FileUtils.fileSize(toDownload.output));
      } catch (InterruptedException | ExecutionException e) {
        throw new IllegalStateException(e);
      }
    }
    loggers.awaitAndLog(downloads, config.logInterval());
  }

  private CompletableFuture<?> downloadIfNecessary(ToDownload toDownload) {
    long existingSize = FileUtils.size(toDownload.output);

    return httpSize(toDownload).thenCompose(size -> {
      toDownload.size.complete(size);
      if (size == existingSize) {
        LOGGER.info("Skipping " + toDownload.id + ": " + toDownload.output + " already up-to-date");
        return CompletableFuture.completedFuture(null);
      } else {
        LOGGER.info("Downloading " + toDownload.url + " to " + toDownload.output);
        FileUtils.delete(toDownload.output);
        FileUtils.createParentDirectories(toDownload.output);
        return httpDownload(toDownload).whenComplete((result, error) -> {
          if (result != null) {
            LOGGER.info("Finished downloading " + toDownload.url + " to " + toDownload.output);
          } else if (error != null) {
            LOGGER.error("Error downloading " + toDownload.url + " to " + toDownload.output, error);
          }
        });
      }
    });
  }

  private CompletableFuture<Long> httpSize(ToDownload toDownload) {
    return client
      .sendAsync(newHttpRequest(toDownload.url).method("HEAD", HttpRequest.BodyPublishers.noBody()).build(),
        responseInfo -> {
          assertOK(responseInfo);
          long contentLength = responseInfo.headers().firstValueAsLong(CONTENT_LENGTH).orElseThrow();
          return HttpResponse.BodyHandlers.replacing(contentLength).apply(responseInfo);
        }).thenApply(HttpResponse::body);
  }

  private CompletableFuture<?> httpDownload(ToDownload toDownload) {
    return client.sendAsync(newHttpRequest(toDownload.url).GET().build(), responseInfo -> {
      assertOK(responseInfo);
      return HttpResponse.BodyHandlers.ofFile(toDownload.output).apply(responseInfo);
    });
  }

  private static HttpRequest.Builder newHttpRequest(String url) {
    return HttpRequest.newBuilder(URI.create(url))
      .timeout(Duration.ofSeconds(30))
      .header(USER_AGENT, "Flatmap downloader (https://github.com/onthegomap/flatmap)");
  }

  private static void assertOK(HttpResponse.ResponseInfo responseInfo) {
    if (responseInfo.statusCode() != 200) {
      throw new IllegalStateException("Bad response: " + responseInfo.statusCode());
    }
  }
}
