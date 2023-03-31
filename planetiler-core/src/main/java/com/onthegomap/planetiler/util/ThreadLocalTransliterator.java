package com.onthegomap.planetiler.util;

import com.ibm.icu.text.Transliterator;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link com.ibm.icu.text.Transliterator} that does not share any static data with other thread local
 * transliterators.
 * <p>
 * By default, {@link com.ibm.icu.text.Transliterator} synchronizes on static data during transliteration, which results
 * in contention between threads when transliterating many strings in parallel. Separate instances of this class can be
 * used across different threads in order to transliterate without contention.
 */
public class ThreadLocalTransliterator {
  private static final Logger LOGGER = LoggerFactory.getLogger(ThreadLocalTransliterator.class);
  private static final AtomicBoolean loggedOnce = new AtomicBoolean(false);
  private final ClassLoader classLoader = DuplicateClassLoader.duplicateClassesWithPrefix("com.ibm.icu");

  /**
   * Returns a {@link com.ibm.icu.text.Transliterator} for {@code id} that does not share any data with transliterators
   * on other threads.
   */
  public TransliteratorInstance getInstance(String id) {
    try {
      Class<?> cls = classLoader.loadClass("com.ibm.icu.text.Transliterator");
      Method getInstance = cls.getMethod("getInstance", String.class);
      Object t = getInstance.invoke(null, id);
      Method transform = cls.getMethod("transliterate", String.class);
      return str -> {
        try {
          return (String) transform.invoke(t, str);
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new IllegalStateException(e);
        }
      };
    } catch (ClassNotFoundException | InvocationTargetException | NoSuchMethodException | IllegalAccessException |
      ExceptionInInitializerError e) {
      if (!loggedOnce.get() && loggedOnce.compareAndSet(false, true)) {
        LOGGER.warn("Could not get thread-local transliterator, falling back to slower shared instance: {}",
          e.toString());
      }
      Transliterator t = Transliterator.getInstance(id);
      return t::transliterate;
    }
  }

  @FunctionalInterface
  public interface TransliteratorInstance {
    String transliterate(String input);
  }
}
