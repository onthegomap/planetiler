package com.onthegomap.flatmap;

import static com.onthegomap.flatmap.TestUtils.newPoint;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.flatmap.read.ReaderFeature;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Test;

public class FeatureCollectorTest {

  private static void assertSameFeatures(List<FeatureCollector.Feature> expected,
    Iterable<FeatureCollector.Feature> actual) {
    List<FeatureCollector.Feature> actualList = StreamSupport.stream(actual.spliterator(), false).toList();
    assertEquals(expected, actualList);
  }

  @Test
  public void testEmpty() {
    var collector = FeatureCollector.from(new ReaderFeature(newPoint(0, 0), Map.of(
      "key", "val"
    )));
    assertSameFeatures(List.of(), collector);
  }


  @Test
  public void testPoint() {
    var collector = FeatureCollector.from(new ReaderFeature(newPoint(0, 0), Map.of(
      "key", "val"
    )));
    collector.point("layername");
    assertSameFeatures(List.of(

    ), collector);
  }
}
