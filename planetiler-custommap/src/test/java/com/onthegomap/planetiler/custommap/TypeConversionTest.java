package com.onthegomap.planetiler.custommap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class TypeConversionTest {
  record Case(Object in, Class<?> clazz, Object out) {
    Case {
      if (out != null) {
        assertInstanceOf(clazz, out);
      }
    }
  }

  private static Stream<Case> testTo(Class<?> clazz, Object out, Object... in) {
    return Stream.of(in).map(i -> new Case(i, clazz, out));
  }

  private static Stream<Case> testConvertTo(Object out, Object... in) {
    return testTo(out.getClass(), out, in);
  }

  static List<Case> cases() {
    return Stream.of(
      testConvertTo(1, "1", 1L, 1.1),
      testConvertTo(1L, "1", 1L, 1.1),
      testConvertTo(1d, "1", "1.0", "1e0", 1L, 1d),
      testConvertTo(1.1, "1.1", 1.1),
      testConvertTo("1", "1", 1, 1L, 1d),
      testConvertTo("1.1", "1.1", 1.1d, 1.1f),
      testConvertTo("1000", 1000, 1000d),
      testConvertTo("NaN", Double.NaN),
      testConvertTo(true, 1, 1L, 1d, "true", "TRUE"),
      testConvertTo(false, 0, 0L, 0d, "false", "FALSE", "no"),
      testConvertTo("true", true),
      testConvertTo("false", false),
      testTo(String.class, null, (String) null),
      testTo(Integer.class, null, (String) null)
    ).flatMap(d -> d).toList();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void testConversion(Case testCase) {
    Object out = TypeConversion.convert(testCase.in, testCase.clazz);
    assertEquals(testCase.out, out);
  }
}
