package com.onthegomap.planetiler.expression;

import static com.onthegomap.planetiler.TestUtils.newPoint;

import com.onthegomap.planetiler.reader.SimpleFeature;
import com.onthegomap.planetiler.reader.SourceFeature;
import java.util.HashMap;
import java.util.Map;

public class ExpressionTestUtil {
  static SourceFeature featureWithTags(String... tags) {
    Map<String, Object> map = new HashMap<>();
    for (int i = 0; i < tags.length; i += 2) {
      map.put(tags[i], tags[i + 1]);
    }
    return SimpleFeature.create(newPoint(0, 0), map);
  }
}
