package com.onthegomap.planetiler.osmmirror;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.stats.Stats;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LongLongSorterTest {
  @TempDir
  Path temp;
  LongLongSorter fixture;

  @BeforeEach
  void before() {
    fixture = new LongLongSorter.DiskBacked(temp, Stats.inMemory(), 2);
  }

  @Test
  void test1() {
    fixture.put(1, 1);
    assertEquals(
      List.of(
        new LongLongSorter.Result(1, 1)
      ),
      StreamSupport.stream(fixture.spliterator(), false).toList()
    );
  }

  @Test
  void test() {
    fixture.put(1, 2);
    fixture.put(1, 1);
    assertEquals(
      List.of(
        new LongLongSorter.Result(1, 1),
        new LongLongSorter.Result(1, 2)
      ),
      StreamSupport.stream(fixture.spliterator(), false).toList()
    );
  }

  @Test
  void test2() {
    fixture.put(1, 2);
    fixture.put(1, 3);
    fixture.put(1, 1);
    assertEquals(
      List.of(
        new LongLongSorter.Result(1, 1),
        new LongLongSorter.Result(1, 2),
        new LongLongSorter.Result(1, 3)
      ),
      StreamSupport.stream(fixture.spliterator(), false).toList()
    );
  }

  @Test
  void test3() {
    assertEquals(
      List.of(),
      StreamSupport.stream(fixture.spliterator(), false).toList()
    );
  }

  @Test
  void testBig() {
    List<LongLongSorter.Result> expected = new ArrayList<>();
    for (long a = 10; a >= 0; a--) {
      for (long b = 10; b >= 0; b--) {
        fixture.put(a, b);
      }
    }
    for (long a = 0; a <= 10; a++) {
      for (long b = 0; b <= 10; b++) {
        expected.add(new LongLongSorter.Result(a, b));
      }
    }
    assertEquals(
      expected,
      StreamSupport.stream(fixture.spliterator(), false).toList()
    );
  }
}
