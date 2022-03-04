package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.*;

import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.stats.Stats;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class DownloaderTest {

  @TempDir
  Path path;
  private final PlanetilerConfig config = PlanetilerConfig.defaults();
  private final Stats stats = Stats.inMemory();
  private long downloads = 0;

  private Downloader mockDownloader(Map<String, byte[]> resources, boolean supportsRange, int maxLength) {
    return new Downloader(config, stats, 2L) {

      @Override
      InputStream openStream(String url) {
        downloads++;
        assertTrue(resources.containsKey(url), "no resource for " + url);
        byte[] bytes = resources.get(url);
        return new ByteArrayInputStream(maxLength < bytes.length ? Arrays.copyOf(bytes, maxLength) : bytes);
      }

      @Override
      InputStream openStreamRange(String url, long start, long end) {
        assertTrue(supportsRange, "does not support range");
        downloads++;
        assertTrue(resources.containsKey(url), "no resource for " + url);
        byte[] result = new byte[Math.min(maxLength, (int) (end - start))];
        byte[] bytes = resources.get(url);
        for (int i = (int) start; i < start + result.length; i++) {
          result[(int) (i - start)] = bytes[i];
        }
        return new ByteArrayInputStream(result);
      }

      @Override
      CompletableFuture<ResourceMetadata> httpHead(String url) {
        String[] parts = url.split("#");
        if (parts.length > 1) {
          int redirectNum = Integer.parseInt(parts[1]);
          String next = redirectNum <= 1 ? parts[0] : (parts[0] + "#" + (redirectNum - 1));
          return CompletableFuture.supplyAsync(
            () -> new ResourceMetadata(Optional.of(next), url, 0, supportsRange));
        }
        byte[] bytes = resources.get(url);
        return CompletableFuture.supplyAsync(
          () -> new ResourceMetadata(Optional.empty(), url, bytes.length, supportsRange));
      }
    };
  }

  @ParameterizedTest
  @CsvSource({
    "false,100,0",
    "true,100,0",
    "true,2,0",
    "false,100,1",
    "false,100,2",
    "true,2,4",
  })
  public void testDownload(boolean range, int maxLength, int redirects) throws Exception {
    Path dest = path.resolve("out");
    String string = "0123456789";
    String url = "http://url";
    String initialUrl = url + (redirects > 0 ? "#" + redirects : "");
    Map<String, byte[]> resources = new ConcurrentHashMap<>();

    byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
    Downloader downloader = mockDownloader(resources, range, maxLength);

    // fails if no data
    var resource1 = new Downloader.ResourceToDownload("resource", initialUrl, dest);
    assertThrows(ExecutionException.class, () -> downloader.downloadIfNecessary(resource1).get());
    assertFalse(Files.exists(dest));
    assertEquals(0, resource1.bytesDownloaded());

    // succeeds with data
    var resource2 = new Downloader.ResourceToDownload("resource", initialUrl, dest);
    resources.put(url, bytes);
    downloader.downloadIfNecessary(resource2).get();
    assertEquals(string, Files.readString(dest));
    assertEquals(FileUtils.size(path), FileUtils.size(dest));
    assertEquals(10, resource2.bytesDownloaded());

    // does not re-request if size is the same
    downloads = 0;
    var resource3 = new Downloader.ResourceToDownload("resource", initialUrl, dest);
    downloader.downloadIfNecessary(resource3).get();
    assertEquals(0, downloads);
    assertEquals(string, Files.readString(dest));
    assertEquals(FileUtils.size(path), FileUtils.size(dest));
    assertEquals(0, resource3.bytesDownloaded());

    // does re-download if size changes
    var resource4 = new Downloader.ResourceToDownload("resource", initialUrl, dest);
    String newContent = "54321";
    resources.put(url, newContent.getBytes(StandardCharsets.UTF_8));
    downloader.downloadIfNecessary(resource4).get();
    assertTrue(downloads > 0, "downloads were " + downloads);
    assertEquals(newContent, Files.readString(dest));
    assertEquals(FileUtils.size(path), FileUtils.size(dest));
    assertEquals(5, resource4.bytesDownloaded());
  }

  @Test
  public void testDownloadFailsIfTooBig() {
    var downloader = new Downloader(config, stats, 2L) {

      @Override
      InputStream openStream(String url) {
        throw new AssertionError("Shouldn't get here");
      }

      @Override
      InputStream openStreamRange(String url, long start, long end) {
        throw new AssertionError("Shouldn't get here");
      }

      @Override
      CompletableFuture<ResourceMetadata> httpHead(String url) {
        return CompletableFuture.completedFuture(new ResourceMetadata(Optional.empty(), url, Long.MAX_VALUE, true));
      }
    };

    Path dest = path.resolve("out");
    String url = "http://url";

    var resource1 = new Downloader.ResourceToDownload("resource", url, dest);
    var exception = assertThrows(ExecutionException.class, () -> downloader.downloadIfNecessary(resource1).get());
    assertInstanceOf(IllegalArgumentException.class, exception.getCause());
    assertTrue(exception.getMessage().contains("--force"), exception.getMessage());
  }
}
