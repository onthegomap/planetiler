package com.onthegomap.planetiler.util;

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
import java.util.stream.Collectors;

/**
 * Watches a set of paths so that each time you call {@link #poll()} it returns the set of paths that have been modified
 * since the last call to {@link #poll()}.
 */
public class FileWatcher {
  private final Map<Path, Long> modificationTimes = new TreeMap<>();

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

  /** Returns true if we are currently watching {@code path} for changes. */
  public boolean watching(Path path) {
    return modificationTimes.containsKey(normalize(path));
  }

  /** Ensures we are only watching {@code paths} provided. */
  public void setWatched(Set<Path> paths) {
    if (paths == null || paths.isEmpty()) {
      return;
    }
    paths = paths.stream().map(FileWatcher::normalize).collect(Collectors.toSet());
    for (var toWatch : paths) {
      if (!watching(toWatch)) {
        watch(toWatch);
      }
    }
    for (var watching : Set.copyOf(modificationTimes.keySet())) {
      if (!paths.contains(watching)) {
        unwatch(watching);
      }
    }
  }

  /**
   * Blocks and invokes {@code action} every time one of the watched files changes, checking every {@code delay}
   * interval.
   */
  public void pollForChanges(Duration delay, FunctionThatThrows<Set<Path>, Set<Path>> action) {
    while (!Thread.currentThread().isInterrupted()) {
      var changes = poll();
      if (!changes.isEmpty()) {
        setWatched(action.runAndWrapException(changes));
      }
      try {
        Thread.sleep(delay.toMillis());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
