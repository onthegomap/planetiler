package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class LayerAttrStatsTest {

  final LayerAttrStats layerStats = new LayerAttrStats();

  @Test
  void testEmptyLayerStats() {
    assertEquals(List.of(), layerStats.getTileStats());
  }

  @Test
  void testEmptyLayerStatsOneLayer() {
    layerStats.accept("layer1", 3, "a", 1);
    layerStats.accept("layer1", 3, "b", "string");
    layerStats.accept("layer1", 3, "c", true);
    assertEquals(List.of(new LayerAttrStats.VectorLayer("layer1", Map.of(
      "a", LayerAttrStats.FieldType.NUMBER,
      "b", LayerAttrStats.FieldType.STRING,
      "c", LayerAttrStats.FieldType.BOOLEAN
    ), 3, 3)), layerStats.getTileStats());
  }

  @Test
  void testEmptyLayerStatsTwoLayers() {
    layerStats.handlerForThread().forZoom(3).forLayer("layer1");
    layerStats.accept("layer2", 4, "a", 1);
    layerStats.accept("layer2", 4, "b", true);
    layerStats.accept("layer2", 4, "c", true);
    layerStats.accept("layer2", 1, "a", 1);
    layerStats.accept("layer2", 1, "b", true);
    layerStats.accept("layer2", 1, "c", 1);
    assertEquals(List.of(new LayerAttrStats.VectorLayer("layer1", Map.of(
    ), 3, 3),
      new LayerAttrStats.VectorLayer("layer2", Map.of(
        "a", LayerAttrStats.FieldType.NUMBER,
        "b", LayerAttrStats.FieldType.BOOLEAN,
        "c", LayerAttrStats.FieldType.STRING
      ), 1, 4)), layerStats.getTileStats());
  }

  @Test
  void testMergeFromMultipleThreads() throws InterruptedException {
    layerStats.accept("layer1", 3, "a", true);
    Thread t1 = new Thread(() -> layerStats.accept("layer1", 3, "a", 1));
    t1.start();
    Thread t2 = new Thread(() -> layerStats.accept("layer1", 4, "a", true));
    t2.start();
    t1.join();
    t2.join();
    assertEquals(List.of(new LayerAttrStats.VectorLayer("layer1", Map.of(
      "a", LayerAttrStats.FieldType.STRING
    ), 3, 4)), layerStats.getTileStats());
  }
}
