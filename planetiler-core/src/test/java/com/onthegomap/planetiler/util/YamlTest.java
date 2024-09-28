package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class YamlTest {
  @Test
  void testLoadYamlRaw() {
    assertEquals(Map.of("a", 1), YAML.load("""
      a: 1
      """, Object.class));
  }

  @Test
  void testLoadYamlToClass() {
    record Result(int a) {}
    assertEquals(new Result(1), YAML.load("""
      a: 1
      """, Result.class));
  }

  @Test
  void testLoadLargeYaml() {
    StringBuilder builder = new StringBuilder();
    List<Integer> expected = new ArrayList<>();
    for (int i = 0; i < 1_000_000; i++) {
      builder.append("\n- ").append(i);
      expected.add(i);
    }
    assertEquals(expected, YAML.load(builder.toString(), List.class));
  }
}
