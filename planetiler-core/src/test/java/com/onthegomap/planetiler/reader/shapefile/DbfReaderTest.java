package com.onthegomap.planetiler.reader.shapefile;

import static org.junit.jupiter.api.Assertions.*;

import com.onthegomap.planetiler.reader.shapefile.TestShapefileWriter.Field;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.time.temporal.JulianFields;
import java.util.Date;
import java.util.List;
import org.junit.jupiter.api.Test;

class DbfReaderTest {
  private static final List<Field> FIELDS = List.of(
    new Field("TEXT", 'C', 6, 0),
    new Field("SMALL", 'N', 3, 0),
    new Field("BIG", 'N', 12, 0),
    new Field("DECIMAL", 'N', 6, 2),
    new Field("FLOAT", 'F', 8, 3),
    new Field("FLAG", 'L', 1, 0),
    new Field("DAY", 'D', 8, 0),
    new Field("EMPTY", 'C', 0, 0), // a field without bytes in the records never has a value, so it's left out
    new Field("STAMP", '@', 8, 0),
    new Field("DATETIME", 'T', 8, 0),
    new Field("INT", 'I', 4, 0),
    new Field("AUTO", '+', 4, 0),
    new Field("DOUBLE", 'O', 8, 0),
    new Field("BINARY", 'B', 8, 0)
  );

  private static byte[] dbf(String... records) {
    return dbf(FIELDS, records);
  }

  private static byte[] dbf(List<Field> fields, String... records) {
    // ISO-8859-1 keeps the bytes of binary fields as they are
    return TestShapefileWriter.dbf(fields, StandardCharsets.ISO_8859_1, List.of(records));
  }

  private static String littleEndian(int... values) {
    var buffer = ByteBuffer.allocate(4 * values.length).order(ByteOrder.LITTLE_ENDIAN);
    for (int value : values) {
      buffer.putInt(value);
    }
    return new String(buffer.array(), StandardCharsets.ISO_8859_1);
  }

  private static String littleEndian(double value) {
    byte[] bytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(value).array();
    return new String(bytes, StandardCharsets.ISO_8859_1);
  }

  @Test
  void testReadsFieldTypes() throws IOException {
    var time = Instant.parse("2024-02-29T12:34:56.789Z").atOffset(ZoneOffset.UTC);
    // Julian day number, then milliseconds since midnight
    String stamp = littleEndian((int) time.getLong(JulianFields.JULIAN_DAY), time.get(ChronoField.MILLI_OF_DAY));
    byte[] file = dbf(
      // deletion flag, then TEXT(6) SMALL(3) BIG(12) DECIMAL(6) FLOAT(8) FLAG(1) DAY(8) as text, then
      // STAMP(8) DATETIME(8) INT(4) AUTO(4) DOUBLE(8) BINARY(8) as binary
      " ab\tc\0\0" + "12 123456789012  1.25   3.500T20240229" +
        stamp + stamp + littleEndian(-7, 8) + littleEndian(1.5) + littleEndian(1, 2),
      "*      ***            ******        ?00000000" +
        " ".repeat(8) + littleEndian(0, 0) + littleEndian(0, 0) + littleEndian(0.0) + " ".repeat(8)
    );
    try (var reader = new DbfReader(new ByteArrayInputStream(file), StandardCharsets.UTF_8)) {
      assertEquals(List.of("TEXT", "SMALL", "BIG", "DECIMAL", "FLOAT", "FLAG", "DAY", "STAMP", "DATETIME", "INT",
        "AUTO", "DOUBLE", "BINARY"), reader.fieldNames());
      var first = reader.next();
      assertFalse(first.deleted());
      var day = Date.from(LocalDate.of(2024, 2, 29).atStartOfDay(ZoneId.systemDefault()).toInstant());
      var timestamp = Date.from(time.toInstant());
      // null bytes after text are padding, and B is ambiguous (memo block in dBase, double in FoxPro)
      assertArrayEquals(new Object[]{"ab\tc", 12, 123456789012L, 1.25, 3.5, true, day, timestamp, timestamp, -7, 8, 1.5, null},
        first.values());
      var second = reader.next();
      assertTrue(second.deleted());
      // blank text is empty, blank or overflowed (****) numbers and unknown logicals and dates have no value, and so
      // do blank and all-zero timestamps
      assertArrayEquals(new Object[]{"", null, null, null, null, null, null, null, null, 0, 0, 0.0, null},
        second.values());
      assertNull(reader.next());
    }
  }

  @Test
  void testTextStartingWithNullByteHasNoValue() throws IOException {
    byte[] file = dbf(List.of(new Field("TEXT", 'C', 3, 0)), " \0xy");
    try (var reader = new DbfReader(new ByteArrayInputStream(file), StandardCharsets.UTF_8)) {
      assertNull(reader.next().values()[0]);
    }
  }

  @Test
  void testRepeatedFieldNamesGetNumbers() throws IOException {
    byte[] file = dbf(List.of(
      new Field("LONG_NAME_", 'C', 1, 0),
      new Field("OTHER", 'C', 1, 0),
      new Field("LONG_NAME_", 'C', 1, 0),
      new Field("LONG_NAME_", 'C', 1, 0)
    ), " abcd");
    try (var reader = new DbfReader(new ByteArrayInputStream(file), StandardCharsets.UTF_8)) {
      assertEquals(List.of("LONG_NAME_", "OTHER", "LONG_NAME_1", "LONG_NAME_2"), reader.fieldNames());
      assertArrayEquals(new Object[]{"a", "b", "c", "d"}, reader.next().values());
    }
    file = dbf(List.of(
      new Field("NAME", 'C', 1, 0),
      new Field("NAME", 'C', 1, 0),
      new Field("NAME1", 'C', 1, 0)
    ), " abc");
    try (var reader = new DbfReader(new ByteArrayInputStream(file), StandardCharsets.UTF_8)) {
      assertEquals(List.of("NAME", "NAME1", "NAME11"), reader.fieldNames());
    }
  }

  @Test
  void testFieldNamesUseTheCharset() throws IOException {
    byte[] file =
      TestShapefileWriter.dbf(List.of(new Field("straße", 'C', 1, 0)), StandardCharsets.UTF_8, List.of(" x"));
    try (var reader = new DbfReader(new ByteArrayInputStream(file), StandardCharsets.UTF_8)) {
      assertEquals(List.of("straße"), reader.fieldNames());
    }
  }

  @Test
  void testInvalidDatesHaveNoValue() throws IOException {
    // GeoTools rolled invalid dates like February 30th over to the next valid date
    byte[] file = dbf(List.of(new Field("DAY", 'D', 8, 0)), " 20240230", " 20241301", " 2024ab01");
    try (var reader = new DbfReader(new ByteArrayInputStream(file), StandardCharsets.UTF_8)) {
      for (int i = 0; i < 3; i++) {
        assertNull(reader.next().values()[0]);
      }
    }
  }

  @Test
  void testNumbersThatDontFitTheFieldTypeReadAsDoubles() throws IOException {
    byte[] file = dbf(List.of(new Field("SMALL", 'N', 4, 0), new Field("BIG", 'N', 20, 0)),
      " 1.5 12345678901234567890",
      " 1e2 not a number        "
    );
    try (var reader = new DbfReader(new ByteArrayInputStream(file), StandardCharsets.UTF_8)) {
      assertArrayEquals(new Object[]{1.5, 1.234567890123456789e19}, reader.next().values());
      assertArrayEquals(new Object[]{100.0, null}, reader.next().values());
    }
  }
}
