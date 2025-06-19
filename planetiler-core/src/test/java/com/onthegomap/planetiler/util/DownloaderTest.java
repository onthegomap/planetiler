package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.*;

import com.onthegomap.planetiler.config.PlanetilerConfig;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.UnaryOperator;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class DownloaderTest {

  @TempDir
  Path path;
  private final PlanetilerConfig config = PlanetilerConfig.defaults();
  private AtomicLong downloads = new AtomicLong(0);
  private int slept = 0;

  private Downloader mockDownloader(Map<String, byte[]> resources, boolean supportsRange,
    boolean supportsContentLength) {
    return mockDownloader(resources, supportsRange, supportsContentLength, UnaryOperator.identity());
  }

  private Downloader mockDownloader(Map<String, byte[]> resources, boolean supportsRange,
    boolean supportsContentLength, UnaryOperator<byte[]> overrideBytes) {
    return new Downloader(config, 2L) {

      @Override
      InputStream openStream(String url) {
        downloads.incrementAndGet();
        assertTrue(resources.containsKey(url), "no resource for " + url);
        byte[] bytes = overrideBytes.apply(resources.get(url));
        return new ByteArrayInputStream(bytes);
      }

      @Override
      InputStream openStreamRange(String url, long start, long end) {
        assertTrue(supportsRange, "does not support range");
        downloads.incrementAndGet();
        assertTrue(resources.containsKey(url), "no resource for " + url);
        byte[] result = new byte[(int) (end - start)];
        byte[] bytes = overrideBytes.apply(resources.get(url));
        for (int i = (int) start; i < start + result.length; i++) {
          result[(int) (i - start)] = bytes[i];
        }
        return new ByteArrayInputStream(result);
      }

      @Override
      ResourceMetadata httpHead(String url) {
        String[] parts = url.split("#");
        if (parts.length > 1) {
          int redirectNum = Integer.parseInt(parts[1]);
          String next = redirectNum <= 1 ? parts[0] : (parts[0] + "#" + (redirectNum - 1));
          return new ResourceMetadata(Optional.of(next), url,
            supportsContentLength ? OptionalLong.of(0) : OptionalLong.empty(), supportsRange);
        }
        byte[] bytes = resources.get(url);
        return new ResourceMetadata(Optional.empty(), url,
          supportsContentLength ? OptionalLong.of(bytes.length) : OptionalLong.empty(), supportsRange);
      }

      @Override
      protected void retrySleep() {
        slept++;
      }
    };
  }

  @ParameterizedTest
  @CsvSource({
    "false,0,true",
    "true,0,true",
    "false,1,true",
    "false,2,true",
    "true,4,true",

    "false,0,false",
    "true,0,false",
    "false,1,false",
    "true,1,false",
  })
  void testDownload(boolean range, int redirects, boolean supportsContentLength) throws Exception {
    Path dest = path.resolve("out");
    String string = "0123456789";
    String url = "http://url";
    String initialUrl = url + (redirects > 0 ? "#" + redirects : "");
    Map<String, byte[]> resources = new ConcurrentHashMap<>();

    byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
    Downloader downloader = mockDownloader(resources, range, supportsContentLength);

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
    if (supportsContentLength) {
      downloads.set(0);
      var resource3 = new Downloader.ResourceToDownload("resource", initialUrl, dest);
      downloader.downloadIfNecessary(resource3).get();
      assertEquals(0, downloads.get());
      assertEquals(string, Files.readString(dest));
      assertEquals(FileUtils.size(path), FileUtils.size(dest));
      assertEquals(0, resource3.bytesDownloaded());
    }

    // does re-download if size changes
    var resource4 = new Downloader.ResourceToDownload("resource", initialUrl, dest);
    String newContent = "54321";
    resources.put(url, newContent.getBytes(StandardCharsets.UTF_8));
    downloader.downloadIfNecessary(resource4).get();
    assertTrue(downloads.get() > 0, "downloads were " + downloads);
    assertEquals(newContent, Files.readString(dest));
    assertEquals(FileUtils.size(path), FileUtils.size(dest));
    assertEquals(5, resource4.bytesDownloaded());
  }

  @ParameterizedTest
  @CsvSource({
    "5, true",
    "6, false"
  })
  void testRetry5xOK(int failures, boolean ok) throws Exception {
    String url = "http://url";
    Path dest = path.resolve("out");
    var resource = new Downloader.ResourceToDownload("resource", url, dest);
    Map<String, byte[]> resources = new ConcurrentHashMap<>();
    AtomicInteger tries = new AtomicInteger(0);
    String value = "abc";
    String truncatedValue = "ab";
    UnaryOperator<byte[]> overrideContent =
      bytes -> (tries.incrementAndGet() <= failures ? truncatedValue : value).getBytes(StandardCharsets.UTF_8);

    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    Downloader downloader = mockDownloader(resources, true, true, overrideContent);
    resources.put(url, bytes);
    var future = downloader.downloadIfNecessary(resource);
    if (ok) {
      future.get();
      assertEquals(value, Files.readString(dest));
      assertEquals(FileUtils.size(path), FileUtils.size(dest));
      assertEquals(value.length(), resource.bytesDownloaded());
      assertEquals(5, slept);
    } else {
      Throwable exception = ExceptionUtils.getRootCause(assertThrows(ExecutionException.class, future::get));
      assertInstanceOf(IOException.class, exception);
      assertFalse(Files.exists(dest));
      assertEquals(truncatedValue.length(), resource.bytesDownloaded());
      assertEquals(5, slept);
    }
  }

  @Test
  void testDownloadFailsIfTooBig() {
    var downloader = new Downloader(config, 2L) {

      @Override
      InputStream openStream(String url) {
        throw new AssertionError("Shouldn't get here");
      }

      @Override
      InputStream openStreamRange(String url, long start, long end) {
        throw new AssertionError("Shouldn't get here");
      }

      @Override
      ResourceMetadata httpHead(String url) {
        return new ResourceMetadata(Optional.empty(), url, OptionalLong.of(Long.MAX_VALUE), true);
      }
    };

    Path dest = path.resolve("out");
    String url = "http://url";

    var resource1 = new Downloader.ResourceToDownload("resource", url, dest);
    var exception = assertThrows(ExecutionException.class, () -> downloader.downloadIfNecessary(resource1).get());
    assertInstanceOf(IllegalStateException.class, exception.getCause());
    assertTrue(exception.getMessage().contains("--force"), exception.getMessage());
  }
}
