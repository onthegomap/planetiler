package com.onthegomap.planetiler.util;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * A class loader that loads certain classes even if they are already loaded by a parent classloader.
 *
 * This makes it possible to get multiple copies of the same class, so for example you could invoke a method
 * synchronized on a static variable from different classes without contention.
 */
public class DuplicateClassLoader extends ClassLoader {

  private final Predicate<String> shouldDuplicate;

  private DuplicateClassLoader(Predicate<String> shouldDuplicate) {
    this.shouldDuplicate = shouldDuplicate;
  }

  /**
   * Returns a {@link ClassLoader} that loads every class with a name starting with {@code prefix} even if the parent
   * has already loaded it.
   */
  public static DuplicateClassLoader duplicateClassesWithPrefix(String prefix) {
    return new DuplicateClassLoader(name -> name.startsWith(prefix));
  }

  @Override
  public Class<?> loadClass(String name) throws ClassNotFoundException {
    if (shouldDuplicate.test(name)) {
      Class<?> c = findLoadedClass(name);
      if (c == null) {
        byte[] b = loadClassFromFile(name);
        return defineClass(name, b, 0, b.length);
      }
    }
    return super.loadClass(name);
  }

  private byte[] loadClassFromFile(String fileName) {
    String classFileName = fileName.replace('.', File.separatorChar) + ".class";
    try (var inputStream = getClass().getClassLoader().getResourceAsStream(classFileName)) {
      return Objects.requireNonNull(inputStream).readAllBytes();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
