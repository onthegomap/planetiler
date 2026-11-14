package com.onthegomap.planetiler.reader.shapefile;

import static org.junit.jupiter.api.Assertions.*;

import com.onthegomap.planetiler.reader.shapefile.TestShapefileWriter.Field;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneId;
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
    new Field("DAY", 'D', 8, 0)
  );

  private static byte[] dbf(String... records) {
    return dbf(FIELDS, records);
  }

  private static byte[] dbf(List<Field> fields, String... records) {
    return TestShapefileWriter.dbf(fields, StandardCharsets.UTF_8, List.of(records));
  }

  @Test
  void testReadsFieldTypes() throws IOException {
    byte[] file = dbf(
      // deletion flag, then TEXT(6) SMALL(3) BIG(12) DECIMAL(6) FLOAT(8) FLAG(1) DAY(8)
      " ab\tc  12 123456789012  1.25   3.500T20240229",
      "*      ***            ******        ?00000000"
    );
    try (var reader = new DbfReader(new ByteArrayInputStream(file), StandardCharsets.UTF_8)) {
      assertEquals(List.of("TEXT", "SMALL", "BIG", "DECIMAL", "FLOAT", "FLAG", "DAY"), reader.fieldNames());
      var first = reader.next();
      assertFalse(first.deleted());
      var day = Date.from(LocalDate.of(2024, 2, 29).atStartOfDay(ZoneId.systemDefault()).toInstant());
      assertArrayEquals(new Object[]{"ab\tc", 12, 123456789012L, 1.25, 3.5, true, day}, first.values());
      var second = reader.next();
      assertTrue(second.deleted());
      // blank text is empty, blank or overflowed (****) numbers and unknown logicals and dates have no value
      assertArrayEquals(new Object[]{"", null, null, null, null, null, null}, second.values());
      assertNull(reader.next());
    }
  }

  @Test
  void testTextFilledWithNullBytesHasNoValue() throws IOException {
    byte[] file = dbf(" x       1           1     1       1F20000101");
    // the TEXT field starts after the deletion flag
    for (int i = 1; i <= 6; i++) {
      file[32 + 32 * FIELDS.size() + 1 + i] = 0;
    }
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
  }

  @Test
  void testFieldNamesUseTheCharset() throws IOException {
    byte[] file = dbf(List.of(new Field("straße", 'C', 1, 0)), " x");
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
