package com.onthegomap.planetiler.util;

import static com.onthegomap.planetiler.util.Exceptions.throwFatalException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Watches a set of paths so that each time you call {@link #poll()} it returns the set of paths that have been modified
 * since the last call to {@link #poll()}.
 */
public class FileWatcher {
  private final Map<Path, Long> modificationTimes = new TreeMap<>();

  /** Returns the (normalized) paths modified since the last call to poll. */
  public Set<Path> poll() {
    Set<Path> result = new TreeSet<>();
    for (var path : List.copyOf(modificationTimes.keySet())) {
      if (watch(path)) {
        result.add(path);
      }
    }
    return result;
  }

  /** Adds {@code path} to the set of paths to check on each call to {@link #poll()}. */
  public boolean watch(Path path) {
    path = normalize(path);
    Long modifiedTime;
    try {
      modifiedTime = Files.getLastModifiedTime(path).toMillis();
    } catch (IOException e) {
      modifiedTime = null;
    }
    var last = modificationTimes.put(path, modifiedTime);
    return !Objects.equals(modifiedTime, last);
  }

  /** Removes {@code path} from the set of paths to check on each call to {@link #poll()}. */
  public void unwatch(Path path) {
    modificationTimes.remove(normalize(path));
  }

  /** Returns the canonical form of {@code path}. */
  static Path normalize(Path path) {
    return path.toAbsolutePath().normalize();
  }

  /** Returns a new file watcher watching a set of files for modifications. */
  public static FileWatcher newWatcher(Path... paths) {
    var watcher = new FileWatcher();
    for (var path : paths) {
      watcher.watch(path);
    }
    return watcher;
  }

  @FunctionalInterface
  public interface ConsumerThatThrows<T> {

    @SuppressWarnings("java:S112")
    void accept(T value) throws Exception;

    default void runAndWrapException(T value) {
      try {
        accept(value);
      } catch (Exception e) {
        throwFatalException(e);
      }
    }
  }

  public void pollForChanges(Duration delay, ConsumerThatThrows<Set<Path>> action) {
    while (!Thread.currentThread().isInterrupted()) {
      var changes = poll();
      if (!changes.isEmpty()) {
        action.runAndWrapException(changes);
      }
      try {
        Thread.sleep(delay.toMillis());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
