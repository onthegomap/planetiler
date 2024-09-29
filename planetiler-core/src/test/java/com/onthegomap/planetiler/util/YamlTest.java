package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.onthegomap.planetiler.reader.FileFormatException;
import java.util.ArrayList;
import java.util.HashMap;
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
  void testLoadLargeYamlList() {
    StringBuilder builder = new StringBuilder();
    List<Integer> expected = new ArrayList<>();
    for (int i = 0; i < 500_000; i++) {
      builder.append("\n- ").append(i);
      expected.add(i);
    }
    assertEquals(expected, YAML.load(builder.toString(), List.class));
  }

  @Test
  void testLoadLargeYamlMap() {
    StringBuilder builder = new StringBuilder();
    Map<String, Integer> expected = new HashMap<>();
    for (int i = 0; i < 500_000; i++) {
      builder.append('\n').append(i).append(": ").append(i);
      expected.put(Integer.toString(i), i);
    }
    assertEquals(expected, YAML.load(builder.toString(), Map.class));
  }

  @Test
  void testMergeOperator() {
    assertSameYaml("""
      source: &label
        a: 1
      dest:
        <<: *label
        b: 2
      """, """
      source:
        a: 1
      dest:
        a: 1
        b: 2
      """);
  }

  @Test
  void testMergeOperatorNested() {
    assertSameYaml("""
      source: &label
        a: 1
      dest:
        l1:
          l2:
            l3:
              <<: *label
              b: 2
      """, """
      source:
        a: 1
      dest:
        l1:
          l2:
            l3:
              a: 1
              b: 2
      """);
  }

  @Test
  void testMergeOperatorOverride() {
    assertSameYaml("""
      source: &label
        a: 1
      dest:
        <<: *label
        a: 2
      """, """
      source:
        a: 1
      dest:
        a: 2
      """);
  }

  @Test
  void testMergeOperatorMultiple() {
    assertSameYaml("""
      source: &label1
        a: 1
        z: 1
      source2: &label2
        a: 2
        b: 3
      dest:
        <<: [*label1, *label2]
        b: 4
        c: 5
      """, """
      source:
        a: 1
        z: 1
      source2:
        a: 2
        b: 3
      dest:
        a: 1 # from label1 since it came first
        b: 4
        c: 5
        z: 1
      """);
  }

  @Test
  void testMergeNotAnchor() {
    assertSameYaml("""
      <<:
        a: 1
        b: 2
      b: 3
      c: 4
      """, """
      a: 1
      b: 3
      c: 4
      """);
  }

  @Test
  void testMergeOperatorSecond() {
    assertSameYaml("""
      source: &label
        a: 1
      dest:
        c: 3
        <<: *label
        b: 2
      """, """
      source:
        a: 1
      dest:
        a: 1
        b: 2
        c: 3
      """);
  }

  @Test
  void testMergeOperatorFromDraft1() {
    assertSameYaml("""
      - { x: 1, y: 2 }
      - { x: 0, y: 2 }
      - { r: 10 }
      - { r: 1 }
      - # Explicit keys
        x: 1
        y: 2
        r: 10
        label: center/big
      """, """
      - &CENTER { x: 1, y: 2 }
      - &LEFT { x: 0, y: 2 }
      - &BIG { r: 10 }
      - &SMALL { r: 1 }
      - # Merge one map
       << : *CENTER
       r: 10
       label: center/big
      """);
  }

  @Test
  void testMergeOperatorFromDraft2() {
    assertSameYaml("""
      - { x: 1, y: 2 }
      - { x: 0, y: 2 }
      - { r: 10 }
      - { r: 1 }
      - # Explicit keys
        x: 1
        y: 2
        r: 10
        label: center/big
      """, """
      - &CENTER { x: 1, y: 2 }
      - &LEFT { x: 0, y: 2 }
      - &BIG { r: 10 }
      - &SMALL { r: 1 }
      - # Merge multiple maps
        << : [ *CENTER, *BIG ]
        label: center/big
      """);
  }

  @Test
  void testMergeOperatorFromDraft3() {
    assertSameYaml("""
      - { x: 1, y: 2 }
      - { x: 0, y: 2 }
      - { r: 10 }
      - { r: 1 }
      - # Explicit keys
        x: 1
        y: 2
        r: 10
        label: center/big
      """, """
      - &CENTER { x: 1, y: 2 }
      - &LEFT { x: 0, y: 2 }
      - &BIG { r: 10 }
      - &SMALL { r: 1 }
      - # Override
        << : [ *BIG, *LEFT, *SMALL ]
        x: 1
        label: center/big
      """);
  }

  @Test
  void testAnchorAndAliasMap() {
    assertSameYaml("""
      source: &label
        a: 1
      dest: *label
      """, """
      source:
        a: 1
      dest:
        a: 1
      """);
  }

  @Test
  void testAnchorAndAliasList() {
    assertSameYaml("""
      source: &label
        - 1
      dest: *label
      """, """
      source: [1]
      dest: [1]
      """);
  }

  @Test
  void testAllowRefInMergeDoc() {
    assertSameYaml("""
      source: &label
        a: &label1
          c: 1
        b: *label1
        d:
          <<: *label1
      dest: *label
      """, """
      source: {a: {c: 1}, b: {c: 1}, d: {c: 1}}
      dest: {a: {c: 1}, b: {c: 1}, d: {c: 1}}
      """);
  }

  @Test
  void testFailsOnRecursiveRefs() {
    assertThrows(FileFormatException.class, () -> YAML.load("""
      source: &label
        - *label
      """, Object.class));
    assertThrows(FileFormatException.class, () -> YAML.load("""
      source: &label
        <<: *label
      """, Object.class));
    assertThrows(FileFormatException.class, () -> YAML.load("""
      source: &label
        a: *label
      """, Object.class));
  }

  private static void assertSameYaml(String a, String b) {
    assertEquals(
      YAML.load(b, Object.class),
      YAML.load(a, Object.class)
    );
  }
}
