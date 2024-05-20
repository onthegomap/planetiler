package com.onthegomap.planetiler.reader.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Lists;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Period;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ParquetConverterTest {
  @Test
  void testIntPrimitive() {
    testPrimitive(
      PrimitiveType.PrimitiveTypeName.INT32,
      converter -> converter.addInt(1),
      1
    );
  }

  @ParameterizedTest
  @CsvSource({
    "32, true, 100, 100",
    "32, true, 2147483647, 2147483647",
    "32, true, -2147483648, -2147483648",
    "32, false, 100, 100",
    "16, true, 100, 100",
    "8, true, 256, 256",
  })
  void testIntPrimitiveWithAnnotation(int bitWidth, boolean isSigned, int input, int expected) {
    testAnnotatedPrimitive(
      PrimitiveType.PrimitiveTypeName.INT32,
      LogicalTypeAnnotation.intType(bitWidth, isSigned),
      converter -> converter.addInt(input),
      expected
    );
  }

  @Test
  void testLongPrimitive() {
    testPrimitive(
      PrimitiveType.PrimitiveTypeName.INT64,
      converter -> converter.addLong(1),
      1L
    );
  }

  @ParameterizedTest
  @CsvSource({
    "64, true, 9223372036854775807, 9223372036854775807",
    "64, false, 9223372036854775807, 9223372036854775807",
    "64, true, -9223372036854775808, -9223372036854775808",
    "64, true, 1, 1",
  })
  void testLongPrimitiveWithAnnotation(int bitWidth, boolean isSigned, long input, long expected) {
    testAnnotatedPrimitive(
      PrimitiveType.PrimitiveTypeName.INT64,
      LogicalTypeAnnotation.intType(bitWidth, isSigned),
      converter -> converter.addLong(input),
      expected
    );
  }

  @ParameterizedTest
  @CsvSource({
    "0, 1, 10, 10",
    "1, 9, 10, 1",
    "2, 9, 10, 0.1",
  })
  void testIntDecimal(int scale, int precision, int value, double expected) {
    testAnnotatedPrimitive(
      PrimitiveType.PrimitiveTypeName.INT32,
      LogicalTypeAnnotation.decimalType(scale, precision),
      converter -> converter.addInt(value),
      expected
    );
  }

  @ParameterizedTest
  @CsvSource({
    "0, 1, 10, 10",
    "1, 18, 10, 1",
    "2, 18, 10, 0.1",
  })
  void testLongDecimal(int scale, int precision, long value, double expected) {
    testAnnotatedPrimitive(
      PrimitiveType.PrimitiveTypeName.INT64,
      LogicalTypeAnnotation.decimalType(scale, precision),
      converter -> converter.addLong(value),
      expected
    );
  }

  @Test
  void testBooleanPrimitive() {
    testPrimitive(
      PrimitiveType.PrimitiveTypeName.BOOLEAN,
      converter -> converter.addBoolean(true),
      true
    );
  }

  @Test
  void testFloatPrimitive() {
    testPrimitive(
      PrimitiveType.PrimitiveTypeName.FLOAT,
      converter -> converter.addFloat(1f),
      1.0
    );
  }

  @Test
  void testDoublePrimitive() {
    testPrimitive(
      PrimitiveType.PrimitiveTypeName.DOUBLE,
      converter -> converter.addDouble(1.5),
      1.5
    );
  }

  @Test
  void testInt96Timestamp() {
    testPrimitive(
      PrimitiveType.PrimitiveTypeName.INT96,
      converter -> converter.addBinary(Binary.fromConstantByteArray(new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})),
      Instant.parse("-4713-11-24T00:00:00Z")
    );
  }

  @Test
  void testDate() {
    testAnnotatedPrimitive(
      PrimitiveType.PrimitiveTypeName.INT32,
      LogicalTypeAnnotation.dateType(),
      converter -> converter.addInt(2),
      LocalDate.of(1970, 1, 3)
    );
  }

  @Test
  void testTime() {
    testAnnotatedPrimitive(
      PrimitiveType.PrimitiveTypeName.INT32,
      LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MILLIS),
      converter -> converter.addInt(61_000),
      LocalTime.of(0, 1, 1)
    );
    testAnnotatedPrimitive(
      PrimitiveType.PrimitiveTypeName.INT64,
      LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MICROS),
      converter -> converter.addLong(61_000_000),
      LocalTime.of(0, 1, 1)
    );
    testAnnotatedPrimitive(
      PrimitiveType.PrimitiveTypeName.INT64,
      LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.NANOS),
      converter -> converter.addLong(61_000_000_000L),
      LocalTime.of(0, 1, 1)
    );
  }

  @ParameterizedTest
  @CsvSource({
    "true, MILLIS, 61000, 1970-01-01T00:01:01Z",
    "true, MICROS, 61000000, 1970-01-01T00:01:01Z",
    "true, NANOS, 61000000000, 1970-01-01T00:01:01Z",
  })
  void testTimestamp(boolean utc, LogicalTypeAnnotation.TimeUnit unit, long input, String output) {
    testAnnotatedPrimitive(
      PrimitiveType.PrimitiveTypeName.INT64,
      LogicalTypeAnnotation.timestampType(utc, unit),
      converter -> converter.addLong(input),
      Instant.parse(output)
    );
  }

  @Test
  void testString() {
    testAnnotatedPrimitive(
      PrimitiveType.PrimitiveTypeName.BINARY,
      LogicalTypeAnnotation.stringType(),
      converter -> converter.addBinary(Binary.fromString("abcdef")),
      "abcdef"
    );
  }

  @Test
  void testEnum() {
    testAnnotatedPrimitive(
      PrimitiveType.PrimitiveTypeName.BINARY,
      LogicalTypeAnnotation.enumType(),
      converter -> converter.addBinary(Binary.fromString("value")),
      "value"
    );
  }

  @Test
  void testJson() {
    testAnnotatedPrimitive(
      PrimitiveType.PrimitiveTypeName.BINARY,
      LogicalTypeAnnotation.jsonType(),
      converter -> converter.addBinary(Binary.fromString("[1,2,3]")),
      "[1,2,3]"
    );
  }

  @Test
  void testUUID() {
    testAnnotatedPrimitive(
      PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
      LogicalTypeAnnotation.uuidType(),
      16,
      converter -> converter
        .addBinary(Binary.fromConstantByteArray(
          new byte[]{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, (byte) 0x88, (byte) 0x99, (byte) 0xaa, (byte) 0xbb,
            (byte) 0xcc, (byte) 0xdd, (byte) 0xee, (byte) 0xff})),
      UUID.fromString("00112233-4455-6677-8899-aabbccddeeff")
    );
  }

  @Test
  void testInterval() {
    testAnnotatedPrimitive(
      PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
      LogicalTypeAnnotation.IntervalLogicalTypeAnnotation.getInstance(),
      12,
      converter -> converter.addBinary(Binary.fromConstantByteBuffer(ByteBuffer.allocate(12)
        .order(ByteOrder.LITTLE_ENDIAN)
        .putInt(1)
        .putInt(2)
        .putInt(3)
        .flip())),
      new Interval(
        Period.ofMonths(1).plusDays(2),
        Duration.ofMillis(3)
      )
    );
  }

  @Test
  void testOptionalMissing() {
    var materializer = new ParquetRecordConverter(Types.buildMessage()
      .optional(PrimitiveType.PrimitiveTypeName.INT32).named("value")
      .named("message"));
    var rootConverter = materializer.getRootConverter();
    rootConverter.start();
    rootConverter.end();
    assertEquals(Map.of(), materializer.getCurrentRecord());
  }

  @Test
  void testListFromSimpleRepeatedElement() {
    var materializer = new ParquetRecordConverter(Types.buildMessage()
      .repeated(PrimitiveType.PrimitiveTypeName.INT32).named("value")
      .named("message"));


    var rootConverter = materializer.getRootConverter();
    rootConverter.start();
    rootConverter.end();
    assertEquals(Map.of(), materializer.getCurrentRecord());

    rootConverter.start();
    rootConverter.getConverter(0).asPrimitiveConverter().addInt(1);
    rootConverter.end();
    assertEquals(Map.of("value", List.of(1)), materializer.getCurrentRecord());

    rootConverter.start();
    rootConverter.getConverter(0).asPrimitiveConverter().addInt(1);
    rootConverter.getConverter(0).asPrimitiveConverter().addInt(2);
    rootConverter.end();
    assertEquals(Map.of("value", List.of(1, 2)), materializer.getCurrentRecord());
  }

  @Test
  void testListFromListElementStructs() {
    var materializer = new ParquetRecordConverter(Types.buildMessage()
      .requiredList().optionalElement(PrimitiveType.PrimitiveTypeName.INT32).named("value")
      .named("message"));

    var root = materializer.getRootConverter();
    var value = root.getConverter(0).asGroupConverter();
    var list = value.getConverter(0).asGroupConverter();
    var element = list.getConverter(0).asPrimitiveConverter();
    root.start();
    value.start();
    value.end();
    root.end();
    assertEquals(Map.of("value", List.of()), materializer.getCurrentRecord());

    root.start();
    value.start();
    list.start();
    element.addInt(1);
    list.end();
    list.start();
    list.end();
    list.start();
    element.addInt(3);
    list.end();
    value.end();
    root.end();
    assertEquals(Map.of("value", Lists.newArrayList(1, null, 3)), materializer.getCurrentRecord());
  }

  @Test
  void testListRepeatedAtTopAndBottomLevel() {
    var materializer = new ParquetRecordConverter(Types.buildMessage()
      .list(Type.Repetition.REPEATED).element(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REPEATED)
      .named("value")
      .named("message"));

    var root = materializer.getRootConverter();
    var value = root.getConverter(0).asGroupConverter();
    var list = value.getConverter(0).asGroupConverter();
    var element = list.getConverter(0).asPrimitiveConverter();
    root.start();
    value.start();
    value.end();
    value.start();
    list.start();
    element.addInt(1);
    element.addInt(2);
    list.end();
    list.start();
    element.addInt(3);
    list.end();
    value.end();
    root.end();
    assertEquals(Map.of("value", List.of(List.of(), List.of(List.of(1, 2), List.of(3)))),
      materializer.getCurrentRecord());
  }

  @Test
  void testNestedList() {
    var materializer = new ParquetRecordConverter(Types.buildMessage()
      .optionalList()
      .optionalListElement()
      .optionalElement(PrimitiveType.PrimitiveTypeName.INT32)
      .named("value")
      .named("root"));

    //message root {
    //  optional group value (LIST) {
    //    repeated group list {
    //      optional group element (LIST) {
    //        repeated group list {
    //          optional int32 element;
    //        }
    //      }
    //    }
    //  }
    //}

    var root = materializer.getRootConverter();
    var value = root.getConverter(0).asGroupConverter();
    var outerList = value.getConverter(0).asGroupConverter();
    var outerElement = outerList.getConverter(0).asGroupConverter();
    var innerList = outerElement.getConverter(0).asGroupConverter();
    var innerElement = innerList.getConverter(0).asPrimitiveConverter();
    root.start();
    root.end();
    assertEquals(Map.of(), materializer.getCurrentRecord());

    root.start();
    value.start();
    value.end();
    root.end();
    assertEquals(Map.of("value", List.of()), materializer.getCurrentRecord());

    root.start();
    value.start();
    outerList.start();
    outerList.end();

    outerList.start();
    outerElement.start();

    innerList.start();
    innerElement.addInt(1);
    innerList.end();

    innerList.start();
    innerList.end();

    innerList.start();
    innerElement.addInt(2);
    innerList.end();

    outerElement.end();
    outerList.end();
    value.end();
    root.end();

    assertEquals(Map.of(
      "value", Lists.newArrayList(null, Lists.newArrayList(1, null, 2))
    ), materializer.getCurrentRecord());
  }

  @Test
  void testMapConverter() {
    var materializer = new ParquetRecordConverter(Types.buildMessage()
      .optionalMap()
      .key(PrimitiveType.PrimitiveTypeName.INT32)
      .optionalValue(PrimitiveType.PrimitiveTypeName.INT64)
      .named("value")
      .named("root"));

    //message root {
    //  optional group value (MAP) {
    //    repeated group key_value {
    //      required int32 key;
    //      optional int64 value;
    //    }
    //  }
    //}

    var root = materializer.getRootConverter();
    var map = root.getConverter(0).asGroupConverter();
    var keyValue = map.getConverter(0).asGroupConverter();
    var key = keyValue.getConverter(0).asPrimitiveConverter();
    var value = keyValue.getConverter(1).asPrimitiveConverter();

    root.start();
    root.end();
    assertEquals(Map.of(), materializer.getCurrentRecord());

    root.start();
    map.start();
    map.end();
    root.end();
    assertEquals(Map.of("value", Map.of()), materializer.getCurrentRecord());

    root.start();
    map.start();
    keyValue.start();
    key.addInt(1);
    keyValue.end();
    map.end();
    root.end();
    assertEquals(Map.of("value", Map.of()), materializer.getCurrentRecord());

    root.start();
    map.start();
    keyValue.start();
    key.addInt(1);
    value.addLong(2);
    keyValue.end();
    map.end();
    root.end();
    assertEquals(Map.of("value", Map.of(1, 2L)), materializer.getCurrentRecord());

    root.start();
    map.start();
    keyValue.start();
    key.addInt(1);
    value.addLong(2);
    keyValue.end();
    keyValue.start();
    key.addInt(3);
    value.addLong(4);
    keyValue.end();
    map.end();
    root.end();
    assertEquals(Map.of("value", Map.of(1, 2L, 3, 4L)), materializer.getCurrentRecord());
  }

  @Test
  void testRepeatedMap() {
    var materializer = new ParquetRecordConverter(Types.buildMessage()
      .map(Type.Repetition.REPEATED)
      .key(PrimitiveType.PrimitiveTypeName.INT32)
      .optionalValue(PrimitiveType.PrimitiveTypeName.INT64)
      .named("value")
      .named("root"));

    var root = materializer.getRootConverter();
    var map = root.getConverter(0).asGroupConverter();
    var keyValue = map.getConverter(0).asGroupConverter();
    var key = keyValue.getConverter(0).asPrimitiveConverter();

    root.start();
    map.start();
    keyValue.start();
    key.addInt(1);
    keyValue.end();
    map.end();
    root.end();
    assertEquals(Map.of("value", List.of(Map.of())), materializer.getCurrentRecord());
  }

  private void testPrimitive(PrimitiveType.PrimitiveTypeName type, Consumer<PrimitiveConverter> consumer,
    Object expected) {
    var materializer = new ParquetRecordConverter(Types.buildMessage()
      .required(type).named("value")
      .named("message"));
    var rootConverter = materializer.getRootConverter();
    rootConverter.start();
    consumer.accept(rootConverter.getConverter(0).asPrimitiveConverter());
    rootConverter.end();
    assertEquals(Map.of("value", expected), materializer.getCurrentRecord());
  }

  private void testAnnotatedPrimitive(PrimitiveType.PrimitiveTypeName type, LogicalTypeAnnotation annotation,
    Consumer<PrimitiveConverter> consumer, Object expected) {
    testAnnotatedPrimitive(type, annotation, 0, consumer, expected);
  }

  private void testAnnotatedPrimitive(PrimitiveType.PrimitiveTypeName type, LogicalTypeAnnotation annotation,
    int length, Consumer<PrimitiveConverter> consumer, Object expected) {
    var materializer = new ParquetRecordConverter(Types.buildMessage()
      .required(type).as(annotation).length(length).named("value")
      .named("message"));
    var rootConverter = materializer.getRootConverter();
    rootConverter.start();
    consumer.accept(rootConverter.getConverter(0).asPrimitiveConverter());
    rootConverter.end();
    assertEquals(Map.of("value", expected), materializer.getCurrentRecord());
  }
}
