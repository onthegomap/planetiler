package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;

class DuplicateClassLoaderTest {

  @Test
  void testDuplicateClassLoader() throws Exception {
    var cl1 = DuplicateClassLoader.duplicateClassesWithPrefix("com.onthegomap.planetiler.util");
    var cl2 = DuplicateClassLoader.duplicateClassesWithPrefix("com.onthegomap.planetiler.util");
    var tc1 = cl1.loadClass("com.onthegomap.planetiler.util.TestClass");
    var tc2 = cl2.loadClass("com.onthegomap.planetiler.util.TestClass");
    assertSame(tc1, cl1.loadClass("com.onthegomap.planetiler.util.TestClass"));
    assertNotSame(tc1, tc2);
    tc1.getConstructor().newInstance();
    tc2.getConstructor().newInstance();
  }
}
