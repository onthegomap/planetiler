package com.onthegomap.flatmap.collections;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.onthegomap.flatmap.RenderedFeature;
import com.onthegomap.flatmap.monitoring.Stats.InMemory;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class MergeSortFeatureMapTest {

  @TempDir
  Path tmpDir;

  @Test
  public void testEmpty() {
    var features = new MergeSortFeatureMap(tmpDir, new InMemory());
    features.sort();
    assertFalse(features.getAll().hasNext());
  }

  @Test
  public void testThrowsWhenPreparedOutOfOrder() {
    var features = new MergeSortFeatureMap(tmpDir, new InMemory());
    features.accept(new RenderedFeature(1, new byte[]{}));
    assertThrows(IllegalStateException.class, features::getAll);
    features.sort();
    assertThrows(IllegalStateException.class, () -> features.accept(new RenderedFeature(1, new byte[]{})));
  }
}
