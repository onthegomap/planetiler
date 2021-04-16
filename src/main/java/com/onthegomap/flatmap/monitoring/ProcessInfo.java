package com.onthegomap.flatmap.monitoring;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;

public class ProcessInfo {

  public static Duration getProcessCpuTime() {
    try {
      return Duration.ofNanos(callLongGetter("getProcessCpuTime", ManagementFactory.getOperatingSystemMXBean()));
    } catch (NoSuchMethodException | InvocationTargetException e) {
      return Duration.ZERO;
    }
  }

  private static Long callLongGetter(String getterName, Object obj)
    throws NoSuchMethodException, InvocationTargetException {
    return callLongGetter(obj.getClass().getMethod(getterName), obj);
  }


  private static Long callLongGetter(Method method, Object obj) throws InvocationTargetException {
    try {
      return (Long) method.invoke(obj);
    } catch (IllegalAccessException e) {
      // Expected, the declaring class or interface might not be public.
    }

    // Iterate over all implemented/extended interfaces and attempt invoking the method with the
    // same name and parameters on each.
    for (Class<?> clazz : method.getDeclaringClass().getInterfaces()) {
      try {
        Method interfaceMethod = clazz.getMethod(method.getName(), method.getParameterTypes());
        Long result = callLongGetter(interfaceMethod, obj);
        if (result != null) {
          return result;
        }
      } catch (NoSuchMethodException e) {
        // Expected, class might implement multiple, unrelated interfaces.
      }
    }

    return null;
  }


  public static record ThreadState(String name, long cpuTimeNanos, long id) {

    public static final ThreadState DEFAULT = new ThreadState("", 0, -1);

  }


  public static Duration getGcTime() {
    long total = 0;
    for (final GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
      total += gc.getCollectionTime();
    }
    return Duration.ofMillis(total);
  }

  public static Map<Long, ThreadState> getThreadStats() {
    Map<Long, ThreadState> threadState = new TreeMap<>();
    ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    for (ThreadInfo thread : threadMXBean.dumpAllThreads(false, false)) {
      threadState.put(thread.getThreadId(),
        new ThreadState(thread.getThreadName(), threadMXBean.getThreadCpuTime(thread.getThreadId()),
          thread.getThreadId()));
    }
    return threadState;
  }
}
