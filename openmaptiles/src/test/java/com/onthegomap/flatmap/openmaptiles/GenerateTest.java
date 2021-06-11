package com.onthegomap.flatmap.openmaptiles;

import static com.onthegomap.flatmap.openmaptiles.Expression.and;
import static com.onthegomap.flatmap.openmaptiles.Expression.matchAny;
import static com.onthegomap.flatmap.openmaptiles.Expression.matchField;
import static com.onthegomap.flatmap.openmaptiles.Expression.or;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import org.junit.jupiter.api.Test;

public class GenerateTest {

  @Test
  public void testParseSimple() {
    FieldMapping parsed = Generate.generateFieldMapping(Generate.parseYaml("""
      output:
        key: value
        key2:
          - value2
          - '%value3%'
      """));
    assertEquals(new FieldMapping(Map.of(
      "output", or(
        matchAny("key", "value"),
        matchAny("key2", "value2", "%value3%")
      )
    )), parsed);
  }

  @Test
  public void testParseAnd() {
    FieldMapping parsed = Generate.generateFieldMapping(Generate.parseYaml("""
      output:
        __AND__:
          key1: val1
          key2: val2
      """));
    assertEquals(new FieldMapping(Map.of(
      "output", and(
        matchAny("key1", "val1"),
        matchAny("key2", "val2")
      )
    )), parsed);
  }

  @Test
  public void testParseAndWithOthers() {
    FieldMapping parsed = Generate.generateFieldMapping(Generate.parseYaml("""
      output:
        - key0: val0
        - __AND__:
            key1: val1
            key2: val2
      """));
    assertEquals(new FieldMapping(Map.of(
      "output", or(
        matchAny("key0", "val0"),
        and(
          matchAny("key1", "val1"),
          matchAny("key2", "val2")
        )
      )
    )), parsed);
  }

  @Test
  public void testParseAndContainingOthers() {
    FieldMapping parsed = Generate.generateFieldMapping(Generate.parseYaml("""
      output:
        __AND__:
          - key1: val1
          - __OR__:
              key2: val2
              key3: val3
      """));
    assertEquals(new FieldMapping(Map.of(
      "output", and(
        matchAny("key1", "val1"),
        or(
          matchAny("key2", "val2"),
          matchAny("key3", "val3")
        )
      )
    )), parsed);
  }

  @Test
  public void testParseContainsKey() {
    FieldMapping parsed = Generate.generateFieldMapping(Generate.parseYaml("""
      output:
        key1: val1
        key2:
      """));
    assertEquals(new FieldMapping(Map.of(
      "output", or(
        matchAny("key1", "val1"),
        matchField("key2")
      )
    )), parsed);
  }
}
