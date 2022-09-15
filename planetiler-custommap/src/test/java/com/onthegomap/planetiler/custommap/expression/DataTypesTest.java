package com.onthegomap.planetiler.custommap.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.onthegomap.planetiler.expression.DataTypes;
import org.junit.jupiter.api.Test;

class DataTypesTest {
  @Test
  void testLong() {
    assertEquals(1L, DataTypes.from("long").convertFrom("1"));
    assertEquals(1L, DataTypes.from("long").convertFrom(1));
    assertNull(DataTypes.from("long").convertFrom("garbage"));
  }

  @Test
  void testInteger() {
    assertEquals(1, DataTypes.from("integer").convertFrom("1"));
    assertNull(DataTypes.from("integer").convertFrom("garbage"));
    assertEquals(1, DataTypes.from("integer").convertFrom("1.5"));
  }

  @Test
  void testDouble() {
    assertEquals(1.5, DataTypes.from("double").convertFrom("1.5"));
    assertEquals(1.0, DataTypes.from("double").convertFrom(1));
    assertNull(DataTypes.from("double").convertFrom("garbage"));
  }

  @Test
  void testString() {
    assertEquals("1.5", DataTypes.from("string").convertFrom("1.5"));
    assertEquals("1.5", DataTypes.from("string").convertFrom(1.5));
  }

  @Test
  void testRaw() {
    assertEquals("1.5", DataTypes.from("raw").convertFrom("1.5"));
    assertEquals(1.5, DataTypes.from("raw").convertFrom(1.5));
  }

  @Test
  void testBoolean() {
    assertEquals(true, DataTypes.from("boolean").convertFrom("1"));
    assertEquals(true, DataTypes.from("boolean").convertFrom("true"));
    assertEquals(true, DataTypes.from("boolean").convertFrom("yes"));
    assertEquals(true, DataTypes.from("boolean").convertFrom(1));
    assertEquals(false, DataTypes.from("boolean").convertFrom(0));
    assertEquals(false, DataTypes.from("boolean").convertFrom("false"));
    assertEquals(false, DataTypes.from("boolean").convertFrom("no"));
  }

  @Test
  void testDirection() {
    assertEquals(1, DataTypes.from("direction").convertFrom("1"));
    assertEquals(1, DataTypes.from("direction").convertFrom(1));
    assertEquals(1, DataTypes.from("direction").convertFrom("true"));
    assertEquals(1, DataTypes.from("direction").convertFrom("yes"));
    assertEquals(-1, DataTypes.from("direction").convertFrom(-1));
    assertEquals(-1, DataTypes.from("direction").convertFrom("-1"));
    assertEquals(0, DataTypes.from("direction").convertFrom(0));
    assertEquals(0, DataTypes.from("direction").convertFrom("no"));
    assertEquals(0, DataTypes.from("direction").convertFrom("false"));
  }
}
