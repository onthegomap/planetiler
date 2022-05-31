package com.onthegomap.planetiler.util;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;
import java.util.function.UnaryOperator;

public class ThreadLocalTransliterator {
  private final UnaryOperator<String> transliterator;

  public String transliterate(String input) {
    return transliterator.apply(input);
  }

  public ThreadLocalTransliterator() {
    var c = new Cloader();
    try {
      Class<?> cls = c.loadClass("com.ibm.icu.text.Transliterator");
      Method getInstance = cls.getMethod("getInstance", String.class);
      Object t = getInstance.invoke(null, "Any-Latin");
      Method transform = cls.getMethod("transform", String.class);
      transliterator = str -> {
        try {
          return (String) transform.invoke(t, str);
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new IllegalStateException(e);
        }
      };
    } catch (ClassNotFoundException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }

  private static class Cloader extends ClassLoader {
    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
      if (!name.startsWith("com.ibm.icu")) {
        Class<?> c = findLoadedClass(name);
        if (c == null) {
          byte[] b = loadClassFromFile(name);
          return defineClass(name, b, 0, b.length);
        }
      }
      return super.loadClass(name);
    }

    private byte[] loadClassFromFile(String fileName) {
      try {
        return Objects.requireNonNull(
          getClass().getClassLoader().getResourceAsStream(fileName.replace('.', File.separatorChar) + ".class"))
          .readAllBytes();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }
}
