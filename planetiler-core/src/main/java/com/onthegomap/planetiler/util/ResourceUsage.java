package com.onthegomap.planetiler.util;

import com.onthegomap.planetiler.collection.Storage;
import com.onthegomap.planetiler.stats.ProcessInfo;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Check that estimated resource usage (memory, disk) will not exceed limit so we can fail fast before starting.
 * <p>
 * Each method mutates the instance and returns it for chaining. The {@code --force} option will bypass these checks.
 */
@ThreadSafe
public class ResourceUsage {

  /** System RAM - JVM -Xmx memory setting. */
  public static final LimitedResource OFF_HEAP_MEMORY = new Global(
    "free system memory",
    "run on a machine with more RAM or decrease JVM -Xmx setting",
    ProcessInfo::getSystemFreeMemoryBytes
  );
  /** Memory used by {@link java.nio.ByteBuffer#allocateDirect(int)}. */
  public static final LimitedResource DIRECT_MEMORY = new Global(
    "JVM direct memory",
    "increase JVM -XX:MaxDirectMemorySize setting",
    () -> OptionalLong.of(ProcessInfo.getMaxMemoryBytes())
  );
  /** Memory used JVM heap. */
  public static final LimitedResource HEAP = new Global(
    "JVM heap",
    "increase JVM -Xmx setting",
    () -> OptionalLong.of(ProcessInfo.getMaxMemoryBytes())
  );
  private static final Format FORMAT = Format.defaultInstance();
  private static final Logger LOGGER = LoggerFactory.getLogger(ResourceUsage.class);
  private final CopyOnWriteArrayList<Usage> usages = new CopyOnWriteArrayList<>();
  private final String description;

  /** Creates a new resource checker for a part of execution described by {@code description}. */
  public ResourceUsage(String description) {
    this.description = description;
  }

  /** Returns the total amount of disk requested so far. */
  public long diskUsage() {
    return usages.stream()
      .filter(d -> d.resource instanceof DiskUsage)
      .mapToLong(d -> d.amount)
      .sum();
  }

  /** Returns the total amount of disk requested so far for {@code types}. */
  public long get(LimitedResource... types) {
    var typeList = List.of(types);
    return usages.stream()
      .filter(d -> typeList.contains(d.resource))
      .mapToLong(d -> d.amount)
      .sum();
  }

  /** Requests {@code amount} bytes on the file system that contains {@code path}. */
  public ResourceUsage addDisk(Path path, long amount, String description) {
    return path == null ? this : add(new DiskUsage(path), amount, description);
  }

  /** Requests {@code amount} bytes of RAM in the JVM heap. */
  public ResourceUsage addMemory(long amount, String description) {
    return add(HEAP, amount, description);
  }

  /** Requests {@code amount} bytes of direct memory that will exist outside the JVM heap. */
  public ResourceUsage addDirectMemory(long amount, String description) {
    return add(DIRECT_MEMORY, amount, description)
      .add(OFF_HEAP_MEMORY, amount, description);
  }

  /** Requests {@code amount} bytes using {@code storage}. */
  public ResourceUsage add(Path path, Storage storage, long amount, String description) {
    return switch (storage) {
      case RAM -> addMemory(amount, description);
      case DIRECT -> addDirectMemory(amount, description);
      case MMAP -> addDisk(path, amount, description);
    };
  }

  /** Requests {@code amount} bytes of {@code resource}. */
  public ResourceUsage add(LimitedResource resource, long amount, String description) {
    return add(new Usage(resource, amount, description));
  }

  public ResourceUsage add(Usage usage) {
    if (usage.amount != 0) {
      usages.add(usage);
    }
    return this;
  }

  /** Adds all resource requests in {@code other} to this instance and returns it for changing. */
  public ResourceUsage addAll(ResourceUsage other) {
    usages.addAll(other.usages);
    return this;
  }

  /**
   * Add up all the resource requests and logs an error if any exceed the limit for that resource.
   *
   * @param force   If false, then throws an exception, otherwise just logs a warning
   * @param verbose If true then print each resource request even if it is under the limit
   */
  public void checkAgainstLimits(boolean force, boolean verbose) {
    List<String> issues = new ArrayList<>();
    var grouped = usages.stream().collect(Collectors.groupingBy(Usage::resource));
    for (var entry : grouped.entrySet()) {
      LimitedResource resource = entry.getKey();
      List<Usage> usages = entry.getValue();
      long requested = usages.stream().mapToLong(Usage::amount).sum();
      String requestedString = FORMAT.storage(requested);
      OptionalLong limitMaybe = resource.limit();
      if (limitMaybe.isEmpty()) {
        LOGGER
          .warn(requestedString + " requested but unable to get limit for " + resource.description() + ", may fail.");
      } else {
        long limit = limitMaybe.getAsLong();
        String limitString = FORMAT.storage(limit);
        String summary = requestedString + " " + resource.description() + " requested for " + description + ", " +
          limitString + " available";

        if (limit < requested) {
          LOGGER.warn("❌️ " + summary + (resource instanceof Fixable fixable ? " (" + fixable.howToFix() + ")" : ""));
          for (var usage : usages) {
            LOGGER.warn("   - " + FORMAT.storage(usage.amount) + " used for " + usage.description);
          }
        } else if (limit < requested * 1.1) {
          LOGGER.info("⚠️️ " + summary + (resource instanceof Fixable fixable ? " (" + fixable.howToFix() + ")" : ""));
          for (var usage : usages) {
            LOGGER.info("   - " + FORMAT.storage(usage.amount) + " used for " + usage.description);
          }
        } else if (verbose) {
          LOGGER.debug("✓ " + summary);
          for (var usage : usages) {
            LOGGER.debug(" - " + FORMAT.storage(usage.amount) + " used for " + usage.description);
          }
        }

        if (limit < requested) {
          issues.add(summary);
        }
      }
    }
    if (!force && !issues.isEmpty()) {
      throw new IllegalStateException("Insufficient resources for " + description +
        ", use the --force argument to continue anyway:\n" + String.join("\n", issues));
    }
  }

  /** A resource with instructions for increasing. */
  public interface Fixable {

    /** Instructions to increase a resource. */
    String howToFix();
  }

  /** A resource (like RAM or disk space) that has a fixed limit on this system. */
  public interface LimitedResource {

    /** The total amount of this resource available, or empty if unable to determine the limit. */
    OptionalLong limit();

    String description();
  }

  /** An amount of a resource that has been requested. */
  public record Usage(LimitedResource resource, long amount, String description) {}

  /** A shared global resource on this system. */
  public record Global(
    @Override String description,
    @Override String howToFix,
    @Override Supplier<OptionalLong> limitProvider
  ) implements LimitedResource, Fixable {

    @Override
    public OptionalLong limit() {
      return limitProvider.get();
    }
  }

  /** Limited disk space on {@code fileStore}. */
  public record DiskUsage(FileStore fileStore) implements LimitedResource {

    /** Finds the {@link FileStore} that {@code path} will exist on. */
    DiskUsage(Path path) {
      this(FileUtils.getFileStore(path));
    }

    @Override
    public OptionalLong limit() {
      try {
        return OptionalLong.of(fileStore.getUnallocatedSpace());
      } catch (IOException e) {
        return OptionalLong.empty();
      }
    }

    @Override
    public String description() {
      return "storage on " + fileStore.toString();
    }
  }
}
