package com.onthegomap.planetiler.custommap;

import static com.onthegomap.planetiler.custommap.expression.ConfigExpression.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.custommap.expression.ConfigExpression;
import com.onthegomap.planetiler.expression.DataType;
import com.onthegomap.planetiler.expression.Expression;
import com.onthegomap.planetiler.expression.MultiExpression;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ConfigExpressionParserTest {
  private static final TagValueProducer TVP = new TagValueProducer(Map.of());
  private static final ConfigExpression.Signature<Contexts.ProcessFeature, Object> FEATURE_SIGNATURE =
    signature(Contexts.ProcessFeature.DESCRIPTION, Object.class);

  private static <O> void assertParse(String yaml, ConfigExpression<?, ?> parsed, Class<O> clazz) {
    Object expression = YAML.load(yaml, Object.class);
    var actual = ConfigExpressionParser.parse(expression, TVP, FEATURE_SIGNATURE.in(), clazz);
    assertEquals(
      parsed.simplify(),
      actual.simplify()
    );
  }

  @Test
  void testEmpty() {
    assertParse("", constOf(null), String.class);
  }

  @ParameterizedTest
  @ValueSource(strings = {"1", "'1'"})
  void testConst(String input) {
    assertParse(input, constOf(1), Integer.class);
    assertParse(input, constOf(1L), Long.class);
    assertParse(input, constOf(1d), Double.class);
  }

  @Test
  void testVar() {
    assertParse("${feature.id}", variable(FEATURE_SIGNATURE.withOutput(Integer.class), "feature.id"), Integer.class);
    assertParse("${feature.id}", variable(FEATURE_SIGNATURE.withOutput(Long.class), "feature.id"), Long.class);
    assertParse("${feature.id}", variable(FEATURE_SIGNATURE.withOutput(Double.class), "feature.id"), Double.class);
  }

  @Test
  void testStaticExpression() {
    assertParse("${1+2}", constOf(3), Integer.class);
    assertParse("${1+2}", constOf(3L), Long.class);
  }

  @Test
  void testDynamicExpression() {
    assertParse("${feature.tags.a}", script(FEATURE_SIGNATURE, "feature.tags.a"), Object.class);
  }

  @Test
  void testCoalesceStatic() {
    assertParse("""
      coalesce:
      - 1
      - 2
      """, constOf(1), Integer.class);
  }

  @Test
  void testCoalesceDynamic() {
    assertParse("""
      coalesce:
      - ${feature.tags.get('a')}
      - ${feature.tags.get('b')}
      """, coalesce(List.of(
      script(FEATURE_SIGNATURE, "feature.tags.get('a')"),
      script(FEATURE_SIGNATURE, "feature.tags.get('b')")
    )), Object.class);
  }

  @Test
  void testMatch() {
    assertParse("""
      match:
      - if:
          natural: water
        value: 1
      - if:
          natural: lake
        value: 2
      - else: 3
      """, match(FEATURE_SIGNATURE.withOutput(Integer.class), MultiExpression.of(List.of(
      MultiExpression.entry(constOf(1), Expression.matchAny("natural", "water")),
      MultiExpression.entry(constOf(2), Expression.matchAny("natural", "lake"))
    )), constOf(3)), Integer.class);
  }

  @Test
  void testMatchMap() {
    assertParse("""
      match:
        1:
          natural: water
        2:
          natural: lake
        3: default_value
      """, match(FEATURE_SIGNATURE.withOutput(Integer.class), MultiExpression.of(List.of(
      MultiExpression.entry(constOf(1), Expression.matchAny("natural", "water")),
      MultiExpression.entry(constOf(2), Expression.matchAny("natural", "lake"))
    )), constOf(3)), Integer.class);
  }

  @Test
  void testOverrides() {
    assertParse("""
      default_value: 3
      overrides:
      - if: {natural: water}
        value: 1
      - if: {natural: lake}
        value: 2
      """, match(FEATURE_SIGNATURE.withOutput(Integer.class), MultiExpression.of(List.of(
      MultiExpression.entry(constOf(1), Expression.matchAny("natural", "water")),
      MultiExpression.entry(constOf(2), Expression.matchAny("natural", "lake"))
    )), constOf(3)), Integer.class);
  }

  @Test
  void testCast() {
    assertParse("""
      tag_value: abc
      type: integer
      """,
      cast(
        FEATURE_SIGNATURE.withOutput(Integer.class),
        getTag(FEATURE_SIGNATURE.withOutput(Object.class), constOf("abc")),
        DataType.GET_INT
      ),
      Integer.class
    );
  }

  @Test
  void testCoalesceWithType() {
    assertParse("""
      type: double
      coalesce:
      - '1'
      - '2'
      """, constOf(1d), Double.class);
  }

  @Test
  void testCastAValue() {
    assertParse("""
      type: double
      value: '${feature.tags.a}'
      """,
      cast(
        FEATURE_SIGNATURE.withOutput(Double.class),
        script(FEATURE_SIGNATURE.withOutput(Object.class), "feature.tags.a"),
        DataType.GET_DOUBLE
      ),
      Double.class);
  }
}
