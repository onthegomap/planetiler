package com.onthegomap.planetiler.util;

import static com.onthegomap.planetiler.util.FileWatcher.normalize;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileWatcherTest {
  @TempDir
  static Path tempDir;
  Path a = normalize(tempDir.resolve("a"));
  Path b = normalize(tempDir.resolve("b"));
  Path c = normalize(tempDir.resolve("c"));
  long time = 1;

  private void touch(Path... paths) throws IOException {
    for (var path : paths) {
      Files.write(path, new byte[0]);
      Files.setLastModifiedTime(path, FileTime.fromMillis(time++));
    }
  }

  @Test
  void testWatch() throws IOException {
    touch(a);
    var watcher = FileWatcher.newWatcher(a, b);
    assertEquals(Set.of(), watcher.poll());
    assertEquals(Set.of(), watcher.poll());
    touch(a);
    assertEquals(Set.of(a), watcher.poll());
    touch(b);
    assertEquals(Set.of(b), watcher.poll());
    touch(a, b);
    assertEquals(Set.of(a, b), watcher.poll());
  }

  @Test
  void testRemoveWatch() throws IOException {
    touch(a, b);
    var watcher = FileWatcher.newWatcher(a, b);
    assertEquals(Set.of(), watcher.poll());
    watcher.unwatch(a);
    touch(a, b);
    assertEquals(Set.of(b), watcher.poll());
    watcher.unwatch(b);
    touch(a, b);
    assertEquals(Set.of(), watcher.poll());
  }

  @Test
  void testReturnWatched() throws IOException {
    touch(a);
    var watcher = FileWatcher.newWatcher(a, b);
    assertEquals(Set.of(), watcher.poll());
    assertEquals(Set.of(), watcher.poll());
    touch(a);
    assertEquals(Set.of(a), watcher.poll());

    watcher.setWatched(Set.of(b, c));
    touch(b);
    assertEquals(Set.of(b), watcher.poll());
    touch(a);
    assertEquals(Set.of(), watcher.poll());
    touch(a, b, c);
    assertEquals(Set.of(b, c), watcher.poll());

    watcher.setWatched(Set.of(a));
    touch(b);
    assertEquals(Set.of(), watcher.poll());
    touch(a);
    assertEquals(Set.of(a), watcher.poll());
    touch(a, b, c);
    assertEquals(Set.of(a), watcher.poll());

    watcher.setWatched(Set.of());
    touch(a, b, c);
    assertEquals(Set.of(a), watcher.poll());
    watcher.setWatched(null);
    touch(a, b, c);
    assertEquals(Set.of(a), watcher.poll());
  }
}
