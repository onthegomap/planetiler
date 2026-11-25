package com.onthegomap.planetiler.util;

import com.onthegomap.planetiler.collection.Storage;
import com.onthegomap.planetiler.config.ArgumentHelp;
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
import net.jcip.annotations.ThreadSafe;
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
  public static final LimitedResource HEAP = new Heap();
  private static final Format FORMAT = Format.defaultInstance();
  private static final Logger LOGGER = LoggerFactory.getLogger(ResourceUsage.class);
  private final CopyOnWriteArrayList<Usage> usages = new CopyOnWriteArrayList<>();
  private final String description;
  private final String howToFreeDisk;

  /** Creates a new resource checker for a part of execution described by {@code description}. */
  public ResourceUsage(String description) {
    this(description, null);
  }

  /**
   * Creates a new resource checker for a part of execution described by {@code description} (e.g. "reading the
   * sources"), where {@code howToFreeDisk} says what to do when a disk is too small for it.
   */
  public ResourceUsage(String description, String howToFreeDisk) {
    this.description = description;
    this.howToFreeDisk = howToFreeDisk;
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
   * Adds up the resource requests and fails if any exceeds what is available by more than a small margin, since the
   * estimates are rough and a run that might just fit should go ahead.
   *
   * @param force   If true, then logs the problems as warnings instead of failing
   * @param verbose If true then log each resource request at debug level even if it is under the limit
   * @throws InsufficientResourcesException if a resource will run out and {@code force} is false, with one line per
   *                                        resource
   */
  public void checkAgainstLimits(boolean force, boolean verbose) {
    List<String> issues = new ArrayList<>();
    var grouped = usages.stream().collect(Collectors.groupingBy(Usage::resource));
    for (var entry : grouped.entrySet()) {
      LimitedResource resource = entry.getKey();
      List<Usage> resourceUsages = entry.getValue();
      long requested = resourceUsages.stream().mapToLong(Usage::amount).sum();
      OptionalLong limitMaybe = resource.limit();
      if (limitMaybe.isEmpty()) {
        LOGGER.atDebug().setMessage("{} requested but unable to get limit for {}")
          .addArgument(() -> FORMAT.storage(requested))
          .addArgument(resource::description)
          .log();
        continue;
      }
      long limit = limitMaybe.getAsLong();
      boolean fails = requested > limit + resource.margin(requested);
      if (fails) {
        String howToFix = resource instanceof Fixable fixable ? fixable.howToFix() :
          resource instanceof DiskUsage ? howToFreeDisk : null;
        issues.add(resource.notEnough(description, requested, limit) + (howToFix == null ? "" : " " + howToFix));
      }
      if ((verbose || fails) && LOGGER.isDebugEnabled()) {
        LOGGER.debug("{} {} {} requested for {}, {} available", fails ? "❌️" : "✓", FORMAT.storage(requested),
          resource.description(), description, FORMAT.storage(limit));
        for (var usage : resourceUsages) {
          LOGGER.debug("   - {} used for {}", FORMAT.storage(usage.amount), usage.description);
        }
      }
    }
    if (issues.isEmpty()) {
      return;
    }
    if (!force) {
      throw new InsufficientResourcesException(String.join("\n", issues));
    }
    for (String issue : issues) {
      LOGGER.warn("{} Continuing anyway because of --force.", issue);
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

    /**
     * Returns how far {@code requested} may go over the limit before the check fails, since the estimates are rough:
     * 15% by default.
     */
    default long margin(long requested) {
      return requested * 15 / 100;
    }

    /** Returns the sentence saying that {@code usedFor} needs {@code requested} but only {@code limit} is there. */
    default String notEnough(String usedFor, long requested, long limit) {
      return "Not enough %s for %s: needs about %sB, %sB available.".formatted(description(), usedFor,
        FORMAT.storage(requested), FORMAT.storage(limit));
    }
  }

  /** The JVM heap, which the launcher's {@code --memory} or the {@code -Xmx} JVM option limits. */
  private record Heap() implements LimitedResource {

    @Override
    public OptionalLong limit() {
      return OptionalLong.of(ProcessInfo.getMaxMemoryBytes());
    }

    @Override
    public String description() {
      return "JVM heap";
    }

    @Override
    public long margin(long requested) {
      return requested / 4;
    }

    @Override
    public String notEnough(String usedFor, long requested, long limit) {
      // suggest some headroom over the estimate, since a heap that is just big enough spends most of its time in GC
      long suggested = requested * 3 / 2;
      return "Not enough memory for %s: needs about %sB, current limit is %sB. Try %s.".formatted(usedFor,
        FORMAT.storage(requested), FORMAT.storage(limit), ArgumentHelp.memoryLimitExample(suggested));
    }
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

    @Override
    public long margin(long requested) {
      // Output size estimates are often several times too high on extracts.
      return Math.max(requested * 15 / 100, 1L << 30);
    }

    @Override
    public String notEnough(String usedFor, long requested, long limit) {
      return "Not enough disk space on %s for %s: needs about %sB, %sB free.".formatted(fileStore, usedFor,
        FORMAT.storage(requested), FORMAT.storage(limit));
    }
  }
}
