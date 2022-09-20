package com.onthegomap.planetiler.custommap.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.onthegomap.planetiler.expression.DataType;
import org.junit.jupiter.api.Test;

class DataTypeTest {
  @Test
  void testLong() {
    assertEquals(1L, DataType.from("long").convertFrom("1"));
    assertEquals(1L, DataType.from("long").convertFrom(1));
    assertNull(DataType.from("long").convertFrom("garbage"));
  }

  @Test
  void testInteger() {
    assertEquals(1, DataType.from("integer").convertFrom("1"));
    assertNull(DataType.from("integer").convertFrom("garbage"));
    assertEquals(1, DataType.from("integer").convertFrom("1.5"));
  }

  @Test
  void testDouble() {
    assertEquals(1.5, DataType.from("double").convertFrom("1.5"));
    assertEquals(1.0, DataType.from("double").convertFrom(1));
    assertNull(DataType.from("double").convertFrom("garbage"));
  }

  @Test
  void testString() {
    assertEquals("1.5", DataType.from("string").convertFrom("1.5"));
    assertEquals("1.5", DataType.from("string").convertFrom(1.5));
  }

  @Test
  void testRaw() {
    assertEquals("1.5", DataType.from("raw").convertFrom("1.5"));
    assertEquals(1.5, DataType.from("raw").convertFrom(1.5));
  }

  @Test
  void testBoolean() {
    assertEquals(true, DataType.from("boolean").convertFrom("1"));
    assertEquals(true, DataType.from("boolean").convertFrom("true"));
    assertEquals(true, DataType.from("boolean").convertFrom("yes"));
    assertEquals(true, DataType.from("boolean").convertFrom(1));
    assertEquals(false, DataType.from("boolean").convertFrom(0));
    assertEquals(false, DataType.from("boolean").convertFrom("false"));
    assertEquals(false, DataType.from("boolean").convertFrom("no"));
  }

  @Test
  void testDirection() {
    assertEquals(1, DataType.from("direction").convertFrom("1"));
    assertEquals(1, DataType.from("direction").convertFrom(1));
    assertEquals(1, DataType.from("direction").convertFrom("true"));
    assertEquals(1, DataType.from("direction").convertFrom("yes"));
    assertEquals(-1, DataType.from("direction").convertFrom(-1));
    assertEquals(-1, DataType.from("direction").convertFrom("-1"));
    assertEquals(0, DataType.from("direction").convertFrom(0));
    assertEquals(0, DataType.from("direction").convertFrom("no"));
    assertEquals(0, DataType.from("direction").convertFrom("false"));
  }
}
