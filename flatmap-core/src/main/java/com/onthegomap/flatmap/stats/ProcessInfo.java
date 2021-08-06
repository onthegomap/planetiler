package com.onthegomap.flatmap.stats;

import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
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

public class ProcessInfo {

  private static final AtomicReference<Map<String, Long>> postGcMemoryUsage = new AtomicReference<>(Map.of());

  // listen on GC events to track memory pool sizes after each GC
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
  }

  public static Optional<Duration> getProcessCpuTime() {
    return Optional
      .ofNullable(callGetter("getProcessCpuTime", ManagementFactory.getOperatingSystemMXBean(), Long.class))
      .map(Duration::ofNanos);
  }

  private static <T> T callGetter(String getterName, Object obj, Class<T> resultClazz) {
    try {
      return callGetter(obj.getClass().getMethod(getterName), obj, resultClazz);
    } catch (NoSuchMethodException | InvocationTargetException e) {
      return null;
    }
  }

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

  public static long getMaxMemoryBytes() {
    return Runtime.getRuntime().maxMemory();
  }


  public static record ThreadState(String name, long cpuTimeNanos, long userTimeNanos, long id) {

    public static final ThreadState DEFAULT = new ThreadState("", 0, 0, -1);

  }


  public static Duration getGcTime() {
    long total = 0;
    for (final GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
      total += gc.getCollectionTime();
    }
    return Duration.ofMillis(total);
  }

  public static Map<String, Long> getPostGcPoolSizes() {
    return postGcMemoryUsage.get();
  }

  public static OptionalLong getMemoryUsageAfterLastGC() {
    var lastGcPoolSizes = postGcMemoryUsage.get();
    if (lastGcPoolSizes.isEmpty()) {
      return OptionalLong.empty();
    } else {
      return OptionalLong.of(lastGcPoolSizes.values().stream().mapToLong(l -> l).sum());
    }
  }

  public static Map<Long, ThreadState> getThreadStats() {
    Map<Long, ThreadState> threadState = new TreeMap<>();
    ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    for (ThreadInfo thread : threadMXBean.dumpAllThreads(false, false)) {
      threadState.put(thread.getThreadId(),
        new ThreadState(
          thread.getThreadName(),
          threadMXBean.getThreadCpuTime(thread.getThreadId()),
          threadMXBean.getThreadUserTime(thread.getThreadId()),
          thread.getThreadId()
        ));
    }
    return threadState;
  }
}
