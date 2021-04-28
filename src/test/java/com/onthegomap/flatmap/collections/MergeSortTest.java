package com.onthegomap.flatmap.collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.flatmap.monitoring.Stats;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class MergeSortTest {

  @TempDir
  Path tmpDir;

  private static MergeSort.Entry newEntry(int i) {
    return new MergeSort.Entry(i, new byte[]{(byte) i});
  }

  private MergeSort newSorter(int workers, int chunkSizeLimit) {
    return new MergeSort(tmpDir, workers, chunkSizeLimit, new Stats.InMemory());
  }

  @Test
  public void testEmpty() {
    MergeSort sorter = newSorter(1, 100);
    sorter.sort();
    assertEquals(List.of(), sorter.toList());
  }

  @Test
  public void testSingle() {
    MergeSort sorter = newSorter(1, 100);
    sorter.add(newEntry(1));
    sorter.sort();
    assertEquals(List.of(newEntry(1)), sorter.toList());
  }

  @Test
  public void testTwoItemsOneChunk() {
    MergeSort sorter = newSorter(1, 100);
    sorter.add(newEntry(2));
    sorter.add(newEntry(1));
    sorter.sort();
    assertEquals(List.of(newEntry(1), newEntry(2)), sorter.toList());
  }

  @Test
  public void testTwoItemsTwoChunks() {
    MergeSort sorter = newSorter(1, 0);
    sorter.add(newEntry(2));
    sorter.add(newEntry(1));
    sorter.sort();
    assertEquals(List.of(newEntry(1), newEntry(2)), sorter.toList());
  }

  @Test
  public void testTwoWorkers() {
    MergeSort sorter = newSorter(2, 0);
    sorter.add(newEntry(4));
    sorter.add(newEntry(3));
    sorter.add(newEntry(2));
    sorter.add(newEntry(1));
    sorter.sort();
    assertEquals(List.of(newEntry(1), newEntry(2), newEntry(3), newEntry(4)), sorter.toList());
  }

  @Test
  public void testManyItems() {
    List<MergeSort.Entry> sorted = new ArrayList<>();
    List<MergeSort.Entry> shuffled = new ArrayList<>();
    for (int i = 0; i < 10_000; i++) {
      shuffled.add(newEntry(i));
      sorted.add(newEntry(i));
    }
    Collections.shuffle(shuffled, new Random(0));
    MergeSort sorter = newSorter(2, 20_000);
    shuffled.forEach(sorter::add);
    sorter.sort();
    assertEquals(sorted, sorter.toList());
  }
}
