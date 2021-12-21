package com.onthegomap.planetiler.collection;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.stats.Stats;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class FeatureSortTest {

  private final PlanetilerConfig config = PlanetilerConfig.defaults();

  @TempDir
  Path tmpDir;

  private static SortableFeature newEntry(int i) {
    return new SortableFeature(Long.MIN_VALUE + i, new byte[]{(byte) i, (byte) (1 + i)});
  }

  private FeatureSort newSorter(int workers, int chunkSizeLimit, boolean gzip) {
    return new ExternalMergeSort(tmpDir, workers, chunkSizeLimit, gzip, config, Stats.inMemory());
  }

  @Test
  public void testEmpty() {
    FeatureSort sorter = newSorter(1, 100, false);
    sorter.sort();
    assertEquals(List.of(), sorter.toList());
  }

  @Test
  public void testSingle() {
    FeatureSort sorter = newSorter(1, 100, false);
    sorter.add(newEntry(1));
    sorter.sort();
    assertEquals(List.of(newEntry(1)), sorter.toList());
  }

  @Test
  public void testTwoItemsOneChunk() {
    FeatureSort sorter = newSorter(1, 100, false);
    sorter.add(newEntry(2));
    sorter.add(newEntry(1));
    sorter.sort();
    assertEquals(List.of(newEntry(1), newEntry(2)), sorter.toList());
  }

  @Test
  public void testTwoItemsTwoChunks() {
    FeatureSort sorter = newSorter(1, 0, false);
    sorter.add(newEntry(2));
    sorter.add(newEntry(1));
    sorter.sort();
    assertEquals(List.of(newEntry(1), newEntry(2)), sorter.toList());
  }

  @Test
  public void testTwoWorkers() {
    FeatureSort sorter = newSorter(2, 0, false);
    sorter.add(newEntry(4));
    sorter.add(newEntry(3));
    sorter.add(newEntry(2));
    sorter.add(newEntry(1));
    sorter.sort();
    assertEquals(List.of(newEntry(1), newEntry(2), newEntry(3), newEntry(4)), sorter.toList());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testManyItems(boolean gzip) {
    List<SortableFeature> sorted = new ArrayList<>();
    List<SortableFeature> shuffled = new ArrayList<>();
    for (int i = 0; i < 10_000; i++) {
      shuffled.add(newEntry(i));
      sorted.add(newEntry(i));
    }
    Collections.shuffle(shuffled, new Random(0));
    FeatureSort sorter = newSorter(2, 20_000, gzip);
    shuffled.forEach(sorter::add);
    sorter.sort();
    assertEquals(sorted, sorter.toList());
  }
}
