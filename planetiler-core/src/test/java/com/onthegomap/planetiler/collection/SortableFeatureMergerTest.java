package com.onthegomap.planetiler.collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Supplier;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class SortableFeatureMergerTest {
  record Item(long key) implements HasLongSortKey {}
  record ItemList(List<Item> items) {}

  private static ItemList list(long... items) {
    return new ItemList(LongStream.of(items).mapToObj(Item::new).toList());
  }

  private static List<Long> merge(ItemList... lists) {
    List<Long> list = new ArrayList<>();
    var iter = LongMerger.mergeIterators(Stream.of(lists)
      .map(d -> d.items.iterator())
      .toList());
    iter.forEachRemaining(item -> list.add(item.key));
    assertThrows(NoSuchElementException.class, iter::next);
    return list;
  }

  @Test
  void testMergeEmpty() {
    assertEquals(List.of(), merge());
  }

  @Test
  void testMergeSupplier() {
    List<Long> list = new ArrayList<>();
    var iter = LongMerger.mergeSuppliers(Stream.of(new ItemList[]{list(1, 2)})
      .map(d -> d.items.iterator())
      .<Supplier<Item>>map(d -> () -> {
        try {
          return d.next();
        } catch (NoSuchElementException e) {
          return null;
        }
      })
      .toList());
    iter.forEachRemaining(item -> list.add(item.key));
    assertThrows(NoSuchElementException.class, iter::next);
    assertEquals(List.of(1L, 2L), list);
  }

  @Test
  void testMerge1() {
    assertEquals(List.of(), merge(list()));
    assertEquals(List.of(1L), merge(list(1)));
    assertEquals(List.of(1L, 2L), merge(list(1, 2)));
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
    var listA = list(parse(a));
    var listB = list(parse(b));
    assertEquals(
      LongStream.of(parse(output)).boxed().toList(),
      merge(listA, listB)
    );
    assertEquals(
      LongStream.of(parse(output)).boxed().toList(),
      merge(listB, listA)
    );
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
    var listA = list(parse(a));
    var listB = list(parse(b));
    var listC = list(parse(c));
    assertEquals(
      LongStream.of(parse(output)).boxed().toList(),
      merge(listA, listB, listC),
      "ABC"
    );
    assertEquals(
      LongStream.of(parse(output)).boxed().toList(),
      merge(listA, listC, listB),
      "ACB"
    );
    assertEquals(
      LongStream.of(parse(output)).boxed().toList(),
      merge(listB, listA, listC),
      "BAC"
    );
    assertEquals(
      LongStream.of(parse(output)).boxed().toList(),
      merge(listB, listC, listA),
      "BCA"
    );
    assertEquals(
      LongStream.of(parse(output)).boxed().toList(),
      merge(listC, listA, listB),
      "CAB"
    );
    assertEquals(
      LongStream.of(parse(output)).boxed().toList(),
      merge(listC, listB, listA),
      "CBA"
    );
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
    var listA = list(parse(a));
    var listB = list(parse(b));
    var listC = list(parse(c));
    var listD = list(parse(d));

    assertEquals(
      LongStream.of(parse(output)).boxed().toList(),
      merge(listA, listB, listC, listD),
      "ABCD"
    );
    assertEquals(
      LongStream.of(parse(output)).boxed().toList(),
      merge(listB, listA, listC, listD),
      "BACD"
    );
    assertEquals(
      LongStream.of(parse(output)).boxed().toList(),
      merge(listB, listC, listA, listD),
      "BCAD"
    );
    assertEquals(
      LongStream.of(parse(output)).boxed().toList(),
      merge(listB, listC, listD, listA),
      "BCDA"
    );
  }

  private static long[] parse(String in) {
    return in == null ? new long[0] : Stream.of(in.split("\\s+"))
      .map(String::strip)
      .filter(d -> !d.isBlank())
      .mapToLong(Long::parseLong)
      .toArray();
  }
}
