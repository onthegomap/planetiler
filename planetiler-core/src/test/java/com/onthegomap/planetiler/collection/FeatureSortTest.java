package com.onthegomap.planetiler.collection;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.stats.Stats;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FeatureSortTest {

  private final PlanetilerConfig config = PlanetilerConfig.defaults();

  @TempDir
  Path tmpDir;

  private SortableFeature newEntry(int i) {
    return new SortableFeature(Long.MIN_VALUE + i, new byte[]{(byte) i, (byte) (1 + i)});
  }


  private FeatureSort newSorter(int workers, int chunkSizeLimit, boolean gzip, boolean mmap) {
    return new ExternalMergeSort(tmpDir, workers, chunkSizeLimit, gzip, mmap, true, true, config,
      Stats.inMemory());
  }

  @Test
  void testEmpty() {
    var sorter = newSorter(1, 100, false, false);
    sorter.sort();
    assertEquals(List.of(), sorter.toList());
  }

  @Test
  void testSingle() {
    FeatureSort sorter = newSorter(1, 100, false, false);
    var writer = sorter.writerForThread();
    writer.accept(newEntry(1));
    sorter.sort();
    assertEquals(List.of(newEntry(1)), sorter.toList());
  }

  @Test
  void testTwoItemsOneChunk() {
    FeatureSort sorter = newSorter(1, 100, false, false);
    var writer = sorter.writerForThread();
    writer.accept(newEntry(2));
    writer.accept(newEntry(1));
    sorter.sort();
    assertEquals(List.of(newEntry(1), newEntry(2)), sorter.toList());
  }

  @Test
  void testTwoItemsTwoChunks() {
    FeatureSort sorter = newSorter(1, 0, false, false);
    var writer = sorter.writerForThread();
    writer.accept(newEntry(2));
    writer.accept(newEntry(1));
    sorter.sort();
    assertEquals(List.of(newEntry(1), newEntry(2)), sorter.toList());
  }

  @Test
  void testTwoWorkers() {
    FeatureSort sorter = newSorter(2, 0, false, false);
    var writer = sorter.writerForThread();
    writer.accept(newEntry(4));
    writer.accept(newEntry(3));
    writer.accept(newEntry(2));
    writer.accept(newEntry(1));
    sorter.sort();
    assertEquals(List.of(newEntry(1), newEntry(2), newEntry(3), newEntry(4)), sorter.toList());
  }

  @Test
  void testTwoWriters() {
    FeatureSort sorter = newSorter(2, 0, false, false);
    var writer1 = sorter.writerForThread();
    var writer2 = sorter.writerForThread();
    writer1.accept(newEntry(4));
    writer1.accept(newEntry(3));
    writer2.accept(newEntry(2));
    writer2.accept(newEntry(1));
    sorter.sort();
    assertEquals(List.of(newEntry(1), newEntry(2), newEntry(3), newEntry(4)), sorter.toList());
  }

  @Test
  void testMultipleWritersThatGetCombined() {
    FeatureSort sorter = newSorter(2, 2_000_000, false, false);
    var writer1 = sorter.writerForThread();
    var writer2 = sorter.writerForThread();
    var writer3 = sorter.writerForThread();
    writer1.accept(newEntry(4));
    writer1.accept(newEntry(3));
    writer2.accept(newEntry(2));
    writer2.accept(newEntry(1));
    writer3.accept(newEntry(5));
    writer3.accept(newEntry(6));
    sorter.sort();
    assertEquals(Stream.of(1, 2, 3, 4, 5, 6).map(this::newEntry).toList(),
      sorter.toList());
  }

  @ParameterizedTest
  @CsvSource({
    "false,false",
    "false,true",
    "true,false",
    "true,true",
  })
  void testManyItems(boolean gzip, boolean mmap) {
    List<SortableFeature> sorted = new ArrayList<>();
    List<SortableFeature> shuffled = new ArrayList<>();
    for (int i = 0; i < 10_000; i++) {
      shuffled.add(newEntry(i));
      sorted.add(newEntry(i));
    }
    Collections.shuffle(shuffled, new Random(0));
    FeatureSort sorter = newSorter(2, 20_000, gzip, mmap);
    var writer = sorter.writerForThread();
    shuffled.forEach(writer);
    sorter.sort();
    assertEquals(sorted, sorter.toList());
  }
}
