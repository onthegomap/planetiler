package com.onthegomap.planetiler.reader.parquet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.config.Bounds;
import com.onthegomap.planetiler.util.Glob;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.locationtech.jts.geom.Envelope;

class ParquetInputFileTest {

  static List<Path> bostons() {
    return Glob.of(TestUtils.pathToResource("parquet")).resolve("boston*.parquet").find();
  }

  @ParameterizedTest
  @MethodSource("bostons")
  void testReadBoston(Path path) {
    for (int i = 0; i < 3; i++) {
      Set<Object> ids = new HashSet<>();
      for (var block : new ParquetInputFile("parquet", "layer", path)
        .get()) {
        for (var item : block) {
          ids.add(item.getString("id"));
        }
      }
      assertEquals(3, ids.size(), "iter " + i);
    }
  }

  @ParameterizedTest
  @MethodSource("bostons")
  void testReadBostonWithBboxFilterCovering(Path path) {
    Set<Object> ids = new HashSet<>();
    for (var block : new ParquetInputFile("parquet", "layer", path, null,
      new Bounds(new Envelope(-71.0747653629, -71.0741656634, 42.3560968301, 42.3564346282)), null, null)
      .get()) {
      for (var item : block) {
        ids.add(item.getString("id"));
      }
    }
    assertEquals(3, ids.size());
  }

  @ParameterizedTest
  @MethodSource("bostons")
  void testReadBostonWithBboxFilterCoveringAndOtherFilter(Path path) {
    Set<Object> ids = new HashSet<>();
    for (var block : new ParquetInputFile("parquet", "layer", path, FilterApi.gt(FilterApi.doubleColumn("height"), 3d),
      new Bounds(new Envelope(-71.0747653629, -71.0741656634, 42.3560968301, 42.3564346282)), null, null)
      .get()) {
      for (var item : block) {
        ids.add(item.getString("id"));
      }
    }
    assertEquals(1, ids.size());
  }

  @ParameterizedTest
  @MethodSource("bostons")
  void testReadBostonWithBboxFilterNotCovering(Path path) {
    Set<Object> ids = new HashSet<>();
    for (var block : new ParquetInputFile("parquet", "layer", path, null,
      new Bounds(new Envelope(-72.0747653629, -72.0741656634, 42.3560968301, 42.3564346282)), null, null)
      .get()) {
      for (var item : block) {
        ids.add(item.getString("id"));
      }
    }
    assertEquals(0, ids.size());
  }

  @TestFactory
  List<DynamicTest> testReadAllDataTypes() {

    /*
    ┌──────────────────────┬────────────────────────────────┬─────────┬─────────┬─────────┬─────────┐
    │     column_name      │          column_type           │  null   │   key   │ default │  extra  │
    │       varchar        │            varchar             │ varchar │ varchar │ varchar │ varchar │
    ├──────────────────────┼────────────────────────────────┼─────────┼─────────┼─────────┼─────────┤
    │ geometry             │ BLOB                           │ YES     │         │         │         │ ST_AsWKB(ST_Point(1, 2))
    │ bigint               │ BIGINT                         │ YES     │         │         │         │ 9223372036854775807
    │ blob                 │ BLOB                           │ YES     │         │         │         │ '1011'
    │ boolean              │ BOOLEAN                        │ YES     │         │         │         │ true
    │ date                 │ DATE                           │ YES     │         │         │         │ '2000-01-01'
    │ decimal              │ DECIMAL(18,3)                  │ YES     │         │         │         │ 123456.789
    │ double               │ DOUBLE                         │ YES     │         │         │         │ 123456.789
    │ hugeint              │ HUGEINT                        │ YES     │         │         │         │ 92233720368547758079223372036854775807
    │ integer              │ INTEGER                        │ YES     │         │         │         │ 123
    │ interval             │ INTERVAL                       │ YES     │         │         │         │ INTERVAL 1 MONTH + INTERVAL 1 DAY + INTERVAL 1 SECOND
    │ real                 │ FLOAT                          │ YES     │         │         │         │ 123.456
    │ smallint             │ SMALLINT                       │ YES     │         │         │         │ 1234
    │ time                 │ TIME                           │ YES     │         │         │         │ 2000-01-01 05:30:10.123
    │ timestamp_with_tim…  │ TIMESTAMP WITH TIME ZONE       │ YES     │         │         │         │ 2000-01-01 05:30:10.123 EST
    │ timestamp            │ TIMESTAMP                      │ YES     │         │         │         │ 2000-01-01 05:30:10.123456 EST
    │ tinyint              │ TINYINT                        │ YES     │         │         │         │ 123
    │ ubigint              │ UBIGINT                        │ YES     │         │         │         │ 9223372036854775807
    │ uhugeint             │ UHUGEINT                       │ YES     │         │         │         │ 92233720368547758079223372036854775807
    │ uinteger             │ UINTEGER                       │ YES     │         │         │         │ 123
    │ usmallint            │ USMALLINT                      │ YES     │         │         │         │ 123
    │ utinyint             │ UTINYINT                       │ YES     │         │         │         │ 123
    │ uuid                 │ UUID                           │ YES     │         │         │         │ 606362d9-012a-4949-b91a-1ab439951671
    │ varchar              │ VARCHAR                        │ YES     │         │         │         │ "string"
    │ list                 │ INTEGER[]                      │ YES     │         │         │         │ [1,2,3,4]
    │ map                  │ MAP(INTEGER, VARCHAR)          │ YES     │         │         │         │ map([1,2,3],['one','two','three'])
    │ array                │ INTEGER[3]                     │ YES     │         │         │         │ [1,2,3]
    │ struct               │ STRUCT(i INTEGER, j VARCHAR)   │ YES     │         │         │         │ {'i': 42, 'j': 'a'}
    │ complex              │ MAP(VARCHAR, STRUCT(i INTEGE…  │ YES     │         │         │         │ [MAP(['a', 'b'], [[], [{'i': 43, 'j': 'a'}, {'i': 43, 'j': 'b'}]])];
    ├──────────────────────┴────────────────────────────────┴─────────┴─────────┴─────────┴─────────┤
    │ 29 rows                                                                             6 columns │
    └───────────────────────────────────────────────────────────────────────────────────────────────┘

     */
    Map<String, Object> map = null;
    int i = 0;
    for (var block : new ParquetInputFile("parquet", "layer",
      TestUtils.pathToResource("parquet").resolve("all_data_types.parquet"))
      .get()) {
      for (var item : block) {
        map = item.tags();
        assertEquals(0, i++);
      }
    }
    assertNotNull(map);
    return List.of(
      testEquals(map, "bigint", 9223372036854775807L),
      test(map, "blob", v -> assertArrayEquals("1011".getBytes(), (byte[]) v)),
      testEquals(map, "boolean", true),
      testEquals(map, "date", LocalDate.of(2000, 1, 1)),
      testEquals(map, "decimal", 123456.789),
      testEquals(map, "double", 123456.789),
      testEquals(map, "hugeint", 92233720368547758079223372036854775807.0),
      testEquals(map, "integer", 123),
      testEquals(map, "interval", Interval.of(1, 2, 3_000)),
      test(map, "real", v -> assertEquals(123.456, (double) v, 1e-3)),
      testEquals(map, "smallint", 1234),
      testEquals(map, "time", LocalTime.parse("05:30:10.123")),
      testEquals(map, "timestamp_with_timezone", Instant.parse("2000-01-01T10:30:10.123Z")),
      testEquals(map, "timestamp", Instant.parse("2000-01-01T10:30:10.123Z")),
      testEquals(map, "tinyint", 123),
      testEquals(map, "ubigint", 9223372036854775807L),
      testEquals(map, "uhugeint", 92233720368547758079223372036854775807.0),
      testEquals(map, "uinteger", 123),
      testEquals(map, "usmallint", 123),
      testEquals(map, "utinyint", 123),
      testEquals(map, "uuid", UUID.fromString("606362d9-012a-4949-b91a-1ab439951671")),
      testEquals(map, "varchar", "string"),
      testEquals(map, "list_of_items", List.of(1, 2, 3, 4)),
      testEquals(map, "map", Map.of(1, "one", 2, "two", 3, "three")),
      testEquals(map, "array", List.of(1, 2, 3)),
      testEquals(map, "struct", Map.of("i", 42, "j", "a")),
      testEquals(map, "complex", List.of(Map.of(
        "a", List.of(),
        "b", List.of(
          Map.of("i", 43, "j", "a"),
          Map.of("i", 43, "j", "b")
        )
      )))
    );
  }

  private static DynamicTest testEquals(Map<String, Object> map, String key, Object expected) {
    return test(map, key, v -> assertEquals(expected, map.get(key)));
  }

  private static DynamicTest test(Map<String, Object> map, String key, Consumer<Object> test) {
    return dynamicTest(key, () -> test.accept(map.get(key)));
  }
}
