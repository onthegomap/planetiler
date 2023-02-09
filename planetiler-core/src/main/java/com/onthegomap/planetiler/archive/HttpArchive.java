package com.onthegomap.planetiler.archive;

import static com.google.common.net.HttpHeaders.USER_AGENT;

import com.google.common.util.concurrent.RateLimiter;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.util.CloseableIterator;
import com.onthegomap.planetiler.util.Exceptions;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpArchive implements ReadableTileArchive {
  private static final Logger LOGGER = LoggerFactory.getLogger(HttpArchive.class);
  private final String pattern;
  private final RateLimiter rateLimiter;
  private final PlanetilerConfig config;
  private final HttpClient client;

  public HttpArchive(String pattern, PlanetilerConfig config) {
    this.pattern = pattern;
    var maxBandwidth = config.downloadMaxBandwidth();
    this.config = config;
    rateLimiter = maxBandwidth <= 0 ? null : RateLimiter.create(maxBandwidth);
    client = HttpClient.newBuilder().connectTimeout(config.httpTimeout()).build();
  }

  public CompletableFuture<byte[]> getTileFuture(int x, int y, int z) {
    String urlString = pattern
      .replace("{x}", Integer.toString(x))
      .replace("{y}", Integer.toString(y))
      .replace("{z}", Integer.toString(z));
    HttpRequest request = HttpRequest.newBuilder(URI.create(urlString))
      .timeout(config.httpTimeout())
      .header(USER_AGENT, config.httpUserAgent())
      .GET()
      .build();

    var future = client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray());
    for (int i = 0; i < config.httpRetries(); i++) {
      future = future
        .thenComposeAsync(r -> r.statusCode() >= 500 ? CompletableFuture.failedFuture(new InternalError()) :
          CompletableFuture.completedFuture(r))
        .exceptionallyComposeAsync(ex -> client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()));
    }
    return future.thenApply(HttpResponse::body).thenApply(bytes -> {
      rateLimiter.acquire(bytes.length);
      return bytes;
    });
  }

  @Override
  public byte[] getTile(int x, int y, int z) {
    String urlString = pattern
      .replace("{x}", Integer.toString(x))
      .replace("{y}", Integer.toString(y))
      .replace("{z}", Integer.toString(z));
    HttpRequest request = HttpRequest.newBuilder(URI.create(urlString))
      .timeout(config.httpTimeout())
      .header(USER_AGENT, config.httpUserAgent())
      .GET()
      .build();

    for (int i = 0; i <= config.httpRetries(); i++) {
      try {
        var response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        if (response.statusCode() == 200) {
          try (var body = response.body()) {
            return body.readAllBytes();
          }
        } else if (response.statusCode() == 204 || response.statusCode() == 404) {
          return new byte[0];
        } else if (response.statusCode() < 500) {
          throw new IllegalStateException("status = " + response.statusCode());
        }
      } catch (InterruptedException | IOException e) {
        boolean lastTry = i == config.httpRetries();
        if (!lastTry) {
          LOGGER.warn("Fetch {} failed, retrying: {}", urlString, e.toString());
        } else {
          LOGGER.error("Fetch {} failed, exhausted retries: {}", urlString, e.toString());
          Exceptions.throwFatalException(e);
        }
      }
    }
    throw new IllegalStateException("exhausted retries");
  }

  @Override
  public CloseableIterator<TileCoord> getAllTileCoords() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {}
}
