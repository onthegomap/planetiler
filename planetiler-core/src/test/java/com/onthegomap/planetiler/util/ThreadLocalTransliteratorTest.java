package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class ThreadLocalTransliteratorTest {
  @Test
  void test() {
    var t1 = new ThreadLocalTransliterator();
    var t2 = new ThreadLocalTransliterator();
    assertEquals("rì běn", t1.transliterate("日本"));
    assertEquals("rì běn", t2.transliterate("日本"));
  }
}
