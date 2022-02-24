package com.onthegomap.planetiler.stats;

import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.management.NotificationEmitter;
import javax.management.openmbean.CompositeData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A collection of utilities to gather runtime information about the JVM.
 */
public class ProcessInfo {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessInfo.class);

  // listen on GC events to track memory pool sizes after each GC
  private static final AtomicReference<Map<String, Long>> postGcMemoryUsage = new AtomicReference<>(Map.of());

  static {
    for (GarbageCollectorMXBean garbageCollectorMXBean : ManagementFactory.getGarbageCollectorMXBeans()) {
      if (garbageCollectorMXBean instanceof NotificationEmitter emitter) {
        emitter.addNotificationListener((notification, handback) -> {
          if (notification.getUserData() instanceof CompositeData compositeData) {
            var info = GarbageCollectionNotificationInfo.from(compositeData);
            GcInfo gcInfo = info.getGcInfo();
            postGcMemoryUsage.set(gcInfo.getMemoryUsageAfterGc().entrySet().stream()
              .map(e -> Map.entry(e.getKey(), e.getValue().getUsed()))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            );
          }
        }, null, null);
      }
    }

    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
    if (threadBean.isThreadContentionMonitoringSupported()) {
      ManagementFactory.getThreadMXBean().setThreadContentionMonitoringEnabled(true);
    } else {
      LOGGER.debug("Thread contention monitoring not supported, will not have access to waiting/blocking time stats.");
    }
  }

  /**
   * Returns the amount of CPU time this processed hase used according to {@link OperatingSystemMXBean}, or empty if the
   * JVM does not support this.
   */
  public static Optional<Duration> getProcessCpuTime() {
    Long result;
    Object obj = ManagementFactory.getOperatingSystemMXBean();
    try {
      result = callGetter(obj.getClass().getMethod("getProcessCpuTime"), obj, Long.class);
    } catch (NoSuchMethodException | InvocationTargetException e) {
      result = null;
    }
    return Optional
      .ofNullable(result)
      .map(Duration::ofNanos);
  }

  // reflection helper
  private static <T> T callGetter(Method method, Object obj, Class<T> resultClazz) throws InvocationTargetException {
    try {
      return resultClazz.cast(method.invoke(obj));
    } catch (IllegalAccessException e) {
      // Expected, the declaring class or interface might not be public.
    } catch (ClassCastException e) {
      return null;
    }

    // Iterate over all implemented/extended interfaces and attempt invoking the method with the
    // same name and parameters on each.
    for (Class<?> clazz : method.getDeclaringClass().getInterfaces()) {
      try {
        Method interfaceMethod = clazz.getMethod(method.getName(), method.getParameterTypes());
        T result = callGetter(interfaceMethod, obj, resultClazz);
        if (result != null) {
          return result;
        }
      } catch (NoSuchMethodException e) {
        // Expected, class might implement multiple, unrelated interfaces.
      }
    }

    return null;
  }

  /** Returns the {@code -Xmx} JVM property in bytes according to {@link Runtime#maxMemory()}. */
  public static long getMaxMemoryBytes() {
    return Runtime.getRuntime().maxMemory();
  }

  /** Processor usage statistics for a thread. */
  public record ThreadState(
    String name, Duration cpuTime, Duration userTime, Duration waiting, Duration blocking, long id
  ) {

    public ThreadState(ThreadMXBean threadMXBean, ThreadInfo thread) {
      this(
        thread.getThreadName(),
        Duration.ofNanos(threadMXBean.getThreadCpuTime(thread.getThreadId())),
        Duration.ofNanos(threadMXBean.getThreadUserTime(thread.getThreadId())),
        Duration.ofMillis(thread.getWaitedTime()),
        Duration.ofMillis(thread.getBlockedTime()),
        thread.getThreadId());
    }

    public static final ThreadState DEFAULT = new ThreadState("", Duration.ZERO, Duration.ZERO, Duration.ZERO,
      Duration.ZERO, -1);

    /** Adds up the timers in two {@code ThreadState} instances */
    public static ThreadState sum(ThreadState a, ThreadState b) {
      return new ThreadState("<multiple threads>",
        a.cpuTime.plus(b.cpuTime),
        a.userTime.plus(b.userTime),
        a.waiting.plus(b.waiting),
        a.blocking.plus(b.blocking),
        -1
      );
    }
  }

  /** Returns the amount of time this JVM has spent in any kind of garbage collection since startup. */
  public static Duration getGcTime() {
    long total = 0;
    for (final GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
      total += gc.getCollectionTime();
    }
    return Duration.ofMillis(total);
  }

  /** Returns a map from memory pool name to the size of that pool in bytes after the last garbage-collection. */
  public static Map<String, Long> getPostGcPoolSizes() {
    return postGcMemoryUsage.get();
  }

  /** Returns the total memory usage in bytes after last GC, or empty if no GC has occurred yet. */
  public static OptionalLong getMemoryUsageAfterLastGC() {
    var lastGcPoolSizes = postGcMemoryUsage.get();
    if (lastGcPoolSizes.isEmpty()) {
      return OptionalLong.empty();
    } else {
      return OptionalLong.of(lastGcPoolSizes.values().stream().mapToLong(l -> l).sum());
    }
  }

  /** Returns a map from thread ID to stats about that thread for every thread that has run, even completed ones. */
  public static Map<Long, ThreadState> getThreadStats() {
    Map<Long, ThreadState> threadState = new TreeMap<>();
    ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    for (ThreadInfo thread : threadMXBean.dumpAllThreads(false, false)) {
      threadState.put(thread.getThreadId(), new ThreadState(threadMXBean, thread));
    }
    return threadState;
  }

  public static ThreadState getCurrentThreadStats() {
    ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    ThreadInfo thread = threadMXBean.getThreadInfo(Thread.currentThread().getId());
    return new ThreadState(threadMXBean, thread);
  }
}
