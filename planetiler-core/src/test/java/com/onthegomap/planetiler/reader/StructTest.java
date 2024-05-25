package com.onthegomap.planetiler.reader;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class StructTest {
  @Test
  void testGet() {
    var struct = WithTags.from(Map.of(
      "a", Map.of("b", "c")
    ));
    var nested = struct.getStruct("a").get("b");
    assertEquals("c", nested.asString());
    assertEquals("c", nested.rawValue());
    assertFalse(nested.isNull());

    assertTrue(struct.getStruct("a").get("b").get("c").get("d").isNull());
  }

  @Test
  void testGetMultilevel() {
    var struct = WithTags.from(Map.of(
      "a", Map.of("b", "c")
    ));
    var nested = struct.getStruct("a", "b");
    assertEquals("c", nested.asString());
    assertEquals("c", nested.rawValue());
    assertFalse(nested.isNull());

    assertTrue(struct.getStruct("a", "b", "c", "d").isNull());
  }


  @Test
  void testGetDottedFromStruct() {
    assertEquals("c", Struct.of(Map.of(
      "a", Map.of("b", "c")
    )).get("a.b").asString());
    assertEquals("c", Struct.of(Map.of(
      "a.b", "c"
    )).get("a.b").asString());
    assertEquals("d", Struct.of(Map.of(
      "a", Map.of("b.c", "d")
    )).get("a.b.c").asString());
    assertNull(Struct.of(Map.of(
      "a", Map.of("b.c", "d")
    )).get("a.b.e").asString());
  }

  @Test
  void testGetDottedFromWithTags() {
    assertEquals("c", WithTags.from(Map.of(
      "a", Map.of("b", "c")
    )).getStruct("a.b").asString());
    assertEquals("c", WithTags.from(Map.of(
      "a", Map.of("b", "c")
    )).getTag("a.b"));
    assertTrue(WithTags.from(Map.of(
      "a", Map.of("b", "c")
    )).hasTag("a.b"));
  }


  @Test
  void testListQuery() {
    var struct = Struct.of(Map.of(
      "a", List.of(Map.of("b", "c"), Map.of("b", "d"))
    ));
    assertEquals("d", struct.get("a").flatMap(elem -> elem.get("b")).get(1).asString());
    assertEquals(Struct.of(List.of("c", "d")), struct.get("a[].b"));
  }

  @Test
  void testListGet() {
    var struct = Struct.of(List.of(1, 2, 3));
    assertEquals(1, struct.get(0).asInt());
    assertEquals(3, struct.get(2).asInt());
    assertTrue(struct.get(4).isNull());
    assertTrue(struct.get(-1).isNull());
  }

  @Test
  void testNullInput() {
    var struct = Struct.of(null);
    assertNull(struct.rawValue());
    assertTrue(struct.isNull());
    assertTrue(struct.get(0).isNull());
    assertTrue(struct.get("nested").isNull());
    assertTrue(struct.get("nested", "level2").isNull());
    assertEquals(Map.of(), struct.asMap());
    assertEquals(List.of(), struct.asList());
    assertEquals("null", struct.toString());
    assertEquals("null", struct.asJson());
    record Type() {}
    assertNull(struct.as(Type.class));

    assertEquals(1, struct.orElse(Struct.of(1)).rawValue());
  }

  private static void assertNotListOrMap(Struct struct) {
    assertTrue(struct.get(0).isNull());
    assertTrue(struct.get("nested").isNull());
    assertTrue(struct.get("nested", "level2").isNull());
    assertEquals(Map.of(), struct.asMap());
    assertEquals(List.of(struct), struct.asList());
  }

  @Test
  void testBooleanInput() {
    var struct = Struct.of(true);
    assertEquals(true, struct.rawValue());
    assertEquals(true, struct.asBoolean());
    assertFalse(struct.isNull());
    assertNotListOrMap(struct);
    assertEquals(true, struct.asList().get(0).asBoolean());
    assertEquals("true", struct.toString());
    assertEquals("true", struct.asJson());

    assertEquals(true, struct.orElse(Struct.of(1)).rawValue());
  }

  @Test
  void testIntInput() {
    var struct = Struct.of(1);
    assertEquals(1, struct.rawValue());
    assertEquals(1, struct.asInt());
    assertEquals(1L, struct.asLong());
    assertEquals(1d, struct.asDouble());
    assertFalse(struct.isNull());
    assertNotListOrMap(struct);
    assertEquals(1, struct.asList().get(0).asInt());
    assertEquals("1", struct.toString());
    assertEquals("1", struct.asJson());

    assertEquals(1, struct.orElse(Struct.of(2)).rawValue());
  }

  @Test
  void testLongInput() {
    var struct = Struct.of(1L);
    assertEquals(1L, struct.rawValue());
    assertEquals(1, struct.asInt());
    assertEquals(1L, struct.asLong());
    assertEquals(1d, struct.asDouble());
    assertEquals(Instant.ofEpochMilli(1), struct.asTimestamp());
    assertFalse(struct.isNull());
    assertNotListOrMap(struct);
    assertEquals(1, struct.asList().get(0).asInt());
    assertEquals("1", struct.toString());
    assertEquals("1", struct.asJson());

    assertEquals(1L, struct.orElse(Struct.of(2)).rawValue());
  }

  @Test
  void testFloatInput() {
    var struct = Struct.of(1.3f);
    assertEquals(1.3f, struct.rawValue());
    assertEquals(1, struct.asInt());
    assertEquals(1L, struct.asLong());
    assertEquals(1.3d, struct.asDouble(), 1e-2);
    assertFalse(struct.isNull());
    assertNotListOrMap(struct);
    assertEquals(1.3, struct.asList().get(0).asDouble(), 1e-2);
    assertEquals("1.3", struct.toString());
    assertEquals("1.3", struct.asJson());

    assertEquals(1.3f, struct.orElse(Struct.of(2)).rawValue());
  }

  @Test
  void testDoubleInput() {
    var struct = Struct.of(1.3d);
    assertEquals(1.3d, struct.rawValue());
    assertEquals(1, struct.asInt());
    assertEquals(1L, struct.asLong());
    assertEquals(1.3d, struct.asDouble());
    assertFalse(struct.isNull());
    assertNotListOrMap(struct);
    assertEquals(1.3, struct.asList().get(0).asDouble());
    assertEquals("1.3", struct.toString());
    assertEquals("1.3", struct.asJson());
    assertEquals(1.3d, struct.orElse(Struct.of(2)).rawValue());
  }

  @Test
  void testNumbersConvertToTimestamps() {
    assertEquals(Instant.ofEpochSecond(1, Duration.ofMillis(1).toNanos() / 2), Struct.of(1000.5).asTimestamp());
    assertEquals(Instant.ofEpochMilli(1500), Struct.of(1500L).asTimestamp());
  }

  @Test
  void testInstantInput() {
    var struct = Struct.of(Instant.ofEpochSecond(60));
    assertFalse(struct.isNull());
    assertNotListOrMap(struct);

    assertEquals(Instant.ofEpochSecond(60), struct.rawValue());
    assertEquals(60_000, struct.asInt());
    assertEquals(60_000L, struct.asLong());
    assertEquals(60_000d, struct.asDouble());
    assertEquals(Instant.ofEpochSecond(60), struct.asTimestamp());
    assertEquals(60_000d, struct.asList().get(0).asDouble());
    assertEquals("1970-01-01T00:01:00Z", struct.toString());
    assertEquals("\"1970-01-01T00:01:00Z\"", struct.asJson());

    assertEquals(Instant.ofEpochSecond(60), struct.orElse(Struct.of(2)).rawValue());
  }

  @Test
  void testLocalTimeInput() {
    var struct = Struct.of(LocalTime.of(1, 2));
    assertFalse(struct.isNull());
    assertNotListOrMap(struct);

    assertEquals(LocalTime.of(1, 2), struct.rawValue());
    assertEquals((int) Duration.ofHours(1).plusMinutes(2).toMillis(), struct.asInt());
    assertEquals(Duration.ofHours(1).plusMinutes(2).toMillis(), struct.asLong());
    assertEquals((double) Duration.ofHours(1).plusMinutes(2).toMillis(), struct.asDouble());
    assertEquals("01:02:00", struct.toString());
    assertEquals("\"01:02:00\"", struct.asJson());
  }

  @Test
  void testLocalDateInput() {
    var struct = Struct.of(LocalDate.of(1, 2, 3));
    assertFalse(struct.isNull());
    assertNotListOrMap(struct);

    assertEquals(LocalDate.of(1, 2, 3), struct.rawValue());
    assertEquals(-719129, struct.asInt());
    assertEquals(-719129L, struct.asLong());
    assertEquals(-719129d, struct.asDouble());
    assertEquals("0001-02-03", struct.toString());
    assertEquals("\"0001-02-03\"", struct.asJson());
  }

  @Test
  void testUUIDInput() {
    var struct = Struct.of(new UUID(1, 2));
    assertFalse(struct.isNull());
    assertNotListOrMap(struct);

    assertEquals(new UUID(1, 2), struct.rawValue());
    assertEquals("00000000-0000-0001-0000-000000000002", struct.asString());
    assertEquals("00000000-0000-0001-0000-000000000002", struct.toString());
    assertEquals("\"00000000-0000-0001-0000-000000000002\"", struct.asJson());
  }

  @Test
  void testStringInput() {
    var struct = Struct.of("abc");
    assertFalse(struct.isNull());
    assertNotListOrMap(struct);
    assertEquals("abc", struct.asString());
    assertEquals("\"abc\"", struct.asJson());
    assertNull(struct.asInt());
    assertNull(struct.asLong());
    assertNull(struct.asDouble());
    assertEquals(true, struct.asBoolean());
    assertArrayEquals("abc".getBytes(StandardCharsets.UTF_8), struct.asBytes());
  }

  @Test
  void testStringToNumber() {
    var struct = Struct.of("1.5");
    assertEquals(1, struct.asInt());
    assertEquals(1L, struct.asLong());
    assertEquals(1.5, struct.asDouble(), 1e-2);
    assertEquals(true, struct.asBoolean());
    assertEquals("\"1.5\"", struct.asJson());
  }

  @Test
  void testStringToBoolean() {
    assertFalse(Struct.of("false").asBoolean());
    assertTrue(Struct.of("true").asBoolean());
    assertTrue(Struct.of("yes").asBoolean());
    assertFalse(Struct.of("0").asBoolean());
    assertTrue(Struct.of("1").asBoolean());
    assertFalse(Struct.of("no").asBoolean());
  }

  @Test
  void testStringToInstant() {
    assertEquals(Instant.ofEpochSecond(100), Struct.of(Instant.ofEpochSecond(100).toString()).asTimestamp());
    assertEquals(Instant.ofEpochSecond(100), Struct.of("100000").asTimestamp());
  }

  @Test
  void testJsonStringToStruct() {
    record Inner(int b) {}
    record Outer(List<Inner> a) {}
    var struct = Struct.of("""
      {"a":[{"b":1}]}
      """);
    assertEquals(1, struct.get("a", 0, "b").asInt());
    assertEquals(new Outer(List.of(new Inner(1))), struct.as(Outer.class));
  }

  @Test
  void testJsonListToStruct() {
    var struct = Struct.of("""
      [1,2,3]
      """);
    assertEquals(1, struct.get(0).asInt());
    assertEquals(2, struct.get(1).asInt());
  }

  @Test
  void testBinaryInput() {
    var struct = Struct.of(new byte[]{1, 2});
    assertArrayEquals(new byte[]{1, 2}, struct.asBytes());
  }

  @Test
  void testAsMapper() {
    var struct = WithTags.from(Map.of(
      "a", Map.of("b", "c")
    ));
    record Inner(String b) {}
    record Outer(Inner a) {}
    assertEquals(new Outer(new Inner("c")), struct.as(Outer.class));
    assertEquals(new Inner("c"), struct.getStruct("a").as(Inner.class));
  }

  @Test
  void testAsJson() {
    var struct = WithTags.from(Map.of(
      "a", Map.of("b", "c")
    ));
    assertEquals("""
      {"a":{"b":"c"}}
      """.strip(), struct.asJson());
    assertEquals("""
      {"b":"c"}
      """.strip(), struct.getStruct("a").asJson());
    assertEquals("""
      "c"
      """.strip(), struct.getStruct("a").get("b").asJson());
  }
}
