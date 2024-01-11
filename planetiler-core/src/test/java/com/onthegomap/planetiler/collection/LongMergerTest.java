package com.onthegomap.planetiler.collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Supplier;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class LongMergerTest {
  record Item(long key, int secondary) implements HasLongSortKey, Comparable<Item> {
    @Override
    public int compareTo(Item o) {
      int cmp = Long.compare(key, o.key);
      if (cmp == 0) {
        cmp = Integer.compare(secondary, o.secondary);
      }
      return cmp;
    }

    long value() {
      return key + secondary;
    }
  }
  record ItemList(List<Item> items) {}

  private static ItemList list(boolean primaryKey, long... items) {
    return new ItemList(
      LongStream.of(items).mapToObj(i -> primaryKey ? new Item(i, 0) : new Item(0, (int) i)).toList());
  }

  private static List<Long> merge(ItemList... lists) {
    List<Long> list = new ArrayList<>();
    var iter = LongMerger.mergeIterators(Stream.of(lists)
      .map(d -> d.items.iterator())
      .toList(), Comparator.naturalOrder());
    iter.forEachRemaining(item -> list.add(item.value()));
    assertThrows(NoSuchElementException.class, iter::next);
    return list;
  }

  @Test
  void testMergeEmpty() {
    assertEquals(List.of(), merge());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testMergeSupplier(boolean primaryKey) {
    List<Long> list = new ArrayList<>();
    var iter = LongMerger.mergeSuppliers(Stream.of(new ItemList[]{list(primaryKey, 1, 2)})
      .map(d -> d.items.iterator())
      .<Supplier<Item>>map(d -> () -> {
        try {
          return d.next();
        } catch (NoSuchElementException e) {
          return null;
        }
      })
      .toList(), Comparator.naturalOrder());
    iter.forEachRemaining(item -> list.add(item.value()));
    assertThrows(NoSuchElementException.class, iter::next);
    assertEquals(List.of(1L, 2L), list);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testMerge1(boolean primaryKey) {
    assertEquals(List.of(), merge(list(primaryKey)));
    assertEquals(List.of(1L), merge(list(primaryKey, 1)));
    assertEquals(List.of(1L, 2L), merge(list(primaryKey, 1, 2)));
  }

  @ParameterizedTest
  @CsvSource(value = {
    ",,",
    "1,,1",
    "1,1,1 1",
    "1 2,,1 2",
    "1 2,2 3,1 2 2 3",
    "1,2,1 2",
    "1 2,3,1 2 3",
    "1 3,2,1 2 3",
  }, nullValues = {"null"})
  void testMerge2(String a, String b, String output) {
    for (boolean primaryKey : List.of(false, true)) {
      var listA = list(primaryKey, parse(a));
      var listB = list(primaryKey, parse(b));
      assertEquals(
        LongStream.of(parse(output)).boxed().toList(),
        merge(listA, listB),
        "primary=" + primaryKey
      );
      assertEquals(
        LongStream.of(parse(output)).boxed().toList(),
        merge(listB, listA),
        "primary=" + primaryKey
      );
    }
  }

  @ParameterizedTest
  @CsvSource(value = {
    ",,,",
    "1,,,1",
    "1,1,1,1 1 1",
    "1 2,,,1 2",
    "1 2,2 3,,1 2 2 3",
    "1,2,,1 2",
    "1,2,3,1 2 3",
    "1 2,3,4,1 2 3 4",
    "1 3,2,4,1 2 3 4",
  }, nullValues = {""})
  void testMerge3(String a, String b, String c, String output) {
    for (boolean primaryKey : List.of(false, true)) {
      var listA = list(primaryKey, parse(a));
      var listB = list(primaryKey, parse(b));
      var listC = list(primaryKey, parse(c));
      assertEquals(
        LongStream.of(parse(output)).boxed().toList(),
        merge(listA, listB, listC),
        "ABC primary=" + primaryKey
      );
      assertEquals(
        LongStream.of(parse(output)).boxed().toList(),
        merge(listA, listC, listB),
        "ACB primary=" + primaryKey
      );
      assertEquals(
        LongStream.of(parse(output)).boxed().toList(),
        merge(listB, listA, listC),
        "BAC primary=" + primaryKey
      );
      assertEquals(
        LongStream.of(parse(output)).boxed().toList(),
        merge(listB, listC, listA),
        "BCA primary=" + primaryKey
      );
      assertEquals(
        LongStream.of(parse(output)).boxed().toList(),
        merge(listC, listA, listB),
        "CAB primary=" + primaryKey
      );
      assertEquals(
        LongStream.of(parse(output)).boxed().toList(),
        merge(listC, listB, listA),
        "CBA primary=" + primaryKey
      );
    }
  }

  @ParameterizedTest
  @CsvSource(value = {
    ",,,,",
    "1,,,,1",
    "1,1,1,1,1 1 1 1",
    "1 2,,,,1 2",
    "1 2,3,,,1 2 3",
    "1 3,2,,,1 2 3",
    "1 3,2 4,,,1 2 3 4",
    "1 5,2 4,,,1 2 4 5",
    "1 2,2 3,,,1 2 2 3",
  }, nullValues = {""})
  void testMerge4(String a, String b, String c, String d, String output) {
    for (boolean primaryKey : List.of(false, true)) {
      var listA = list(primaryKey, parse(a));
      var listB = list(primaryKey, parse(b));
      var listC = list(primaryKey, parse(c));
      var listD = list(primaryKey, parse(d));

      assertEquals(
        LongStream.of(parse(output)).boxed().toList(),
        merge(listA, listB, listC, listD),
        "ABCD primary=" + primaryKey
      );
      assertEquals(
        LongStream.of(parse(output)).boxed().toList(),
        merge(listB, listA, listC, listD),
        "BACD primary=" + primaryKey
      );
      assertEquals(
        LongStream.of(parse(output)).boxed().toList(),
        merge(listB, listC, listA, listD),
        "BCAD primary=" + primaryKey
      );
      assertEquals(
        LongStream.of(parse(output)).boxed().toList(),
        merge(listB, listC, listD, listA),
        "BCDA primary=" + primaryKey
      );
    }
  }

  private static long[] parse(String in) {
    return in == null ? new long[0] : Stream.of(in.split("\\s+"))
      .map(String::strip)
      .filter(d -> !d.isBlank())
      .mapToLong(Long::parseLong)
      .toArray();
  }
}
