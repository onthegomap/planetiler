package com.onthegomap.flatmap.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.flatmap.config.FlatmapConfig;
import com.onthegomap.flatmap.stats.Stats;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class DownloaderTest {

  @TempDir
  Path path;
  private final FlatmapConfig config = FlatmapConfig.defaults();
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
      CompletableFuture<ResourceMetadata> httpHead(ResourceToDownload resource) {
        byte[] bytes = resources.get(resource.url());
        return CompletableFuture.supplyAsync(() -> new ResourceMetadata(bytes.length, supportsRange));
      }
    };
  }

  @ParameterizedTest
  @CsvSource({
    "false,100",
    "true,100",
    "true,2",
  })
  public void testDownload(boolean range, int maxLength) throws Exception {
    Path dest = path.resolve("out");
    String string = "0123456789";
    String url = "http://url";
    Map<String, byte[]> resources = new ConcurrentHashMap<>();

    byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
    Downloader downloader = mockDownloader(resources, range, maxLength);

    // fails if no data
    var resource1 = new Downloader.ResourceToDownload("resource", url, dest);
    assertThrows(ExecutionException.class, () -> downloader.downloadIfNecessary(resource1).get());
    assertFalse(Files.exists(dest));
    assertEquals(0, resource1.bytesDownloaded());

    // succeeds with data
    var resource2 = new Downloader.ResourceToDownload("resource", url, dest);
    resources.put(url, bytes);
    downloader.downloadIfNecessary(resource2).get();
    assertEquals(string, Files.readString(dest));
    assertEquals(FileUtils.size(path), FileUtils.size(dest));
    assertEquals(10, resource2.bytesDownloaded());

    // does not re-request if size is the same
    downloads = 0;
    var resource3 = new Downloader.ResourceToDownload("resource", url, dest);
    downloader.downloadIfNecessary(resource3).get();
    assertEquals(0, downloads);
    assertEquals(string, Files.readString(dest));
    assertEquals(FileUtils.size(path), FileUtils.size(dest));
    assertEquals(0, resource3.bytesDownloaded());

    // does re-download if size changes
    var resource4 = new Downloader.ResourceToDownload("resource", url, dest);
    String newContent = "54321";
    resources.put(url, newContent.getBytes(StandardCharsets.UTF_8));
    downloader.downloadIfNecessary(resource4).get();
    assertTrue(downloads > 0, "downloads were " + downloads);
    assertEquals(newContent, Files.readString(dest));
    assertEquals(FileUtils.size(path), FileUtils.size(dest));
    assertEquals(5, resource4.bytesDownloaded());
  }
}
