package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class CloseableIteratorTest {

  @Test
  void testFilter() {
    assertEquals(
      List.of(2, 4),
      CloseableIterator.of(Stream.of(1, 2, 3, 4, 5, 6))
        .filter(i -> i == 2 || i == 4)
        .stream()
        .toList()
    );
    assertEquals(
      List.of(),
      CloseableIterator.of(Stream.of(100, 99, 98))
        .filter(i -> i == 2 || i == 4)
        .stream()
        .toList()
    );
    assertEquals(
      List.of(),
      CloseableIterator.of(Stream.<Integer>of())
        .filter(i -> i == 2 || i == 4)
        .stream()
        .toList()
    );
  }

}
