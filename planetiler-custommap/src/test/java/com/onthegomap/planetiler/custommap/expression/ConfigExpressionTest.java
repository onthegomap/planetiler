package com.onthegomap.planetiler.custommap.expression;

import static com.onthegomap.planetiler.TestUtils.newPoint;
import static com.onthegomap.planetiler.custommap.TestContexts.FEATURE_POST_MATCH;
import static com.onthegomap.planetiler.custommap.TestContexts.PROCESS_FEATURE;
import static com.onthegomap.planetiler.custommap.TestContexts.ROOT_CONTEXT;
import static com.onthegomap.planetiler.custommap.expression.ConfigExpression.*;
import static com.onthegomap.planetiler.expression.Expression.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.Lists;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.custommap.BooleanExpressionParser;
import com.onthegomap.planetiler.custommap.Contexts;
import com.onthegomap.planetiler.custommap.TagValueProducer;
import com.onthegomap.planetiler.custommap.TestContexts;
import com.onthegomap.planetiler.expression.DataType;
import com.onthegomap.planetiler.expression.MultiExpression;
import com.onthegomap.planetiler.reader.SimpleFeature;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ConfigExpressionTest {
  private static final ConfigExpression.Signature<Contexts.Root, Integer> ROOT = signature(ROOT_CONTEXT, Integer.class);
  private static final ConfigExpression.Signature<Contexts.ProcessFeature, Integer> FEATURE_SIGNATURE =
    signature(PROCESS_FEATURE, Integer.class);

  @Test
  void testConst() {
    assertEquals(1, constOf(1).apply(ScriptContext.empty()));
  }

  @Test
  void testVariable() {
    var feature = SimpleFeature.create(newPoint(0, 0), Map.of("a", "b", "c", 1), "source", "source_layer", 99);
    var context = TestContexts.ROOT.createProcessFeatureContext(feature, new TagValueProducer(Map.of()));
    // simple match
    assertEquals("source", variable(FEATURE_SIGNATURE.withOutput(String.class), "feature.source").apply(context));
    assertEquals("source_layer",
      variable(FEATURE_SIGNATURE.withOutput(String.class), "feature.source_layer").apply(context));
    assertEquals(99L, variable(FEATURE_SIGNATURE.withOutput(Long.class), "feature.id").apply(context));
    assertEquals(99, variable(FEATURE_SIGNATURE.withOutput(Integer.class), "feature.id").apply(context));
    assertEquals(99d, variable(FEATURE_SIGNATURE.withOutput(Double.class), "feature.id").apply(context));
    assertThrows(ParseException.class, () -> variable(FEATURE_SIGNATURE, "missing"));
  }

  @Test
  void testCoalesce() {
    assertNull(coalesce(List.of()).apply(TestContexts.ROOT));
    assertNull(coalesce(
      List.of(
        constOf(null)
      )).apply(TestContexts.ROOT));
    assertEquals(2, coalesce(
      List.of(
        constOf(null),
        constOf(2)
      )).apply(TestContexts.ROOT));
    assertEquals(1, coalesce(
      List.of(
        constOf(1),
        constOf(2)
      )).apply(TestContexts.ROOT));
  }

  @Test
  void testDynamic() {
    assertEquals(1, script(ROOT, "5 - 4").apply(TestContexts.ROOT));
  }

  @Test
  void testMatch() {
    var feature = SimpleFeature.create(newPoint(0, 0), Map.of("a", "b", "c", 1));
    var context = TestContexts.ROOT.createProcessFeatureContext(feature, new TagValueProducer(Map.of()));
    // simple match
    assertEquals(2, match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
      MultiExpression.entry(constOf(1),
        BooleanExpressionScript.script("feature.tags.has('a', 'c')", FEATURE_SIGNATURE.in())),
      MultiExpression.entry(constOf(2),
        BooleanExpressionScript.script("feature.tags.has('a', 'b')", FEATURE_SIGNATURE.in()))
    ))).apply(context));

    // dynamic fallback
    assertEquals(5, match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
      MultiExpression.entry(constOf(1),
        BooleanExpressionScript.script("feature.tags.has('a', 'c')", FEATURE_SIGNATURE.in())),
      MultiExpression.entry(constOf(2),
        BooleanExpressionScript.script("feature.tags.has('a', 'd')", FEATURE_SIGNATURE.in()))
    )), ConfigExpression.script(FEATURE_SIGNATURE, "feature.tags.c + 4")).apply(context));

    // no fallback
    assertNull(match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
      MultiExpression.entry(constOf(1),
        BooleanExpressionScript.script("feature.tags.has('a', 'd')", FEATURE_SIGNATURE.in())),
      MultiExpression.entry(constOf(2),
        BooleanExpressionScript.script("feature.tags.has('a', 'e')", FEATURE_SIGNATURE.in()))
    ))).apply(context));

    // dynamic value
    assertEquals(2, match(
      FEATURE_SIGNATURE,
      MultiExpression.of(List.of(
        MultiExpression.entry(script(FEATURE_SIGNATURE, "1 + size(feature.tags.a)"),
          matchAny("a", "b")),
        MultiExpression.entry(constOf(1), matchAny("a", "c"))
      ))
    ).apply(context));
  }

  @Test
  void testSimplifyCelFunction() {
    assertEquals(
      constOf(3),

      script(FEATURE_SIGNATURE, "1+2").simplify()
    );
  }

  @Test
  void testSimplifyCelFunctionThatJustAccessesVar() {
    assertEquals(
      variable(FEATURE_SIGNATURE, "feature.id"),

      script(FEATURE_SIGNATURE, "feature.id").simplify()
    );
    assertEquals(
      script(FEATURE_SIGNATURE, "feature.tags.a"),

      script(FEATURE_SIGNATURE, "feature.tags.a").simplify()
    );
  }

  @Test
  void testCantSimplifyDynamicLeftExpression() {
    assertEquals(
      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(constOf(1),
          matchAnyTyped(null, script(FEATURE_SIGNATURE.withOutput(Object.class), "feature.id + 1"), List.of(1, 2)
          ))))),

      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(constOf(1),
          BooleanExpressionParser.parse(Map.of("${feature.id + 1}", List.of(1, 2)), null, FEATURE_SIGNATURE.in())
        )))).simplify()
    );
  }

  @ParameterizedTest
  @CsvSource({
    "feature.id",
    "feature.osm_user_name",
    "feature.osm_type",
  })
  void testSimplifySimpleLeftExpression(String varname) {
    assertEquals(
      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(constOf(1),
          matchAnyTyped(null, variable(FEATURE_SIGNATURE.withOutput(Object.class), varname), List.of(1, 2)
          ))))),

      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(constOf(1),
          BooleanExpressionParser.parse(Map.of("${" + varname + "}", List.of(1, 2)), null, FEATURE_SIGNATURE.in())
        )))).simplify()
    );
  }

  @Test
  void testSimplifyExpressionToMatchSource() {
    assertEquals(
      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(constOf(1),
          or(matchSource("source1"), matchSource("source2"))
        )))),

      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(constOf(1),
          BooleanExpressionParser.parse(Map.of("${feature.source}", List.of("source1", "source2")), null,
            FEATURE_SIGNATURE.in())
        )))).simplify()
    );
  }

  @Test
  void testSimplifyExpressionToMatchSourceLayer() {
    assertEquals(
      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(constOf(1),
          or(matchSourceLayer("layer1"), matchSourceLayer("layer2"))
        )))),

      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(constOf(1),
          BooleanExpressionParser.parse(Map.of("${feature.source_layer}", List.of("layer1", "layer2")), null,
            FEATURE_SIGNATURE.in())
        )))).simplify()
    );
  }

  @Test
  void testDontSimplifyOtherSourceLayerCases() {
    testDontSimplifyOtherSourceLayerCases("__any__", List.of());
    testDontSimplifyOtherSourceLayerCases("", List.of(""));
    testDontSimplifyOtherSourceLayerCases("prefix%", List.of("prefix%"));
  }

  private static void testDontSimplifyOtherSourceLayerCases(String in, List<?> out) {
    assertEquals(
      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(constOf(1),
          matchAnyTyped(null, variable(FEATURE_SIGNATURE.withOutput(Object.class), "feature.source_layer"),
            out
          ))))),

      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(constOf(1),
          BooleanExpressionParser.parse(Map.of("${feature.source_layer}", Lists.newArrayList(in)), null,
            FEATURE_SIGNATURE.in())
        )))).simplify()
    );
  }

  @Test
  void testSimplifyCoalesce() {
    assertEquals(
      constOf(null),
      coalesce(List.of()).simplify()
    );
    assertEquals(
      constOf(null),
      coalesce(List.of(constOf(null))).simplify()
    );
    assertEquals(
      constOf(1),
      coalesce(List.of(constOf(1))).simplify()
    );
    assertEquals(
      constOf(1),
      coalesce(List.of(constOf(1), constOf(2))).simplify()
    );
    assertEquals(
      constOf(1),
      coalesce(List.of(constOf(1), constOf(1))).simplify()
    );
    assertEquals(
      constOf(1),
      coalesce(List.of(constOf(1), constOf(2), constOf(1))).simplify()
    );
  }

  @Test
  void testSimplifyMatchAllFalse() {
    assertEquals(
      constOf(null),

      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(constOf(1),
          BooleanExpressionScript.script("1 > 2", FEATURE_SIGNATURE.in())),
        MultiExpression.entry(constOf(2),
          BooleanExpressionScript.script("1 > 3", FEATURE_SIGNATURE.in()))
      ))).simplify()
    );
  }

  @Test
  void testSimplifyMatchAllFalseWithFallback() {
    assertEquals(
      constOf(3),

      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(constOf(1),
          BooleanExpressionScript.script("1 > 2", FEATURE_SIGNATURE.in())),
        MultiExpression.entry(constOf(2),
          BooleanExpressionScript.script("1 > 3", FEATURE_SIGNATURE.in()))
      )), script(FEATURE_SIGNATURE, "1+2")).simplify()
    );
  }

  @Test
  void testSimplifyRemoveCasesAfterTrueAndReplaceFallback() {
    assertEquals(
      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(constOf(0),
          BooleanExpressionScript.script("feature.tags.has('a', 'b')", FEATURE_SIGNATURE.in()))
      )), constOf(1)),

      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(constOf(0),
          BooleanExpressionScript.script("feature.tags.has('a', 'b')", FEATURE_SIGNATURE.in())),
        MultiExpression.entry(constOf(1),
          BooleanExpressionScript.script("1 < 2", FEATURE_SIGNATURE.in())),
        MultiExpression.entry(constOf(2),
          BooleanExpressionScript.script("feature.tags.has('c', 'd')", FEATURE_SIGNATURE.in()))
      )), constOf(2)).simplify()
    );
  }

  @Test
  void testSimplifyRemoveFalseCases() {
    assertEquals(
      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(constOf(2),
          BooleanExpressionScript.script("feature.tags.has('a', 'b')", FEATURE_SIGNATURE.in()))
      )), script(FEATURE_SIGNATURE, "size(feature.tags.a)")),

      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(constOf(1),
          BooleanExpressionScript.script("1 > 2", FEATURE_SIGNATURE.in())),
        MultiExpression.entry(constOf(2),
          BooleanExpressionScript.script("feature.tags.has('a', 'b')", FEATURE_SIGNATURE.in()))
      )), script(FEATURE_SIGNATURE, "size(feature.tags.a)")).simplify()
    );
  }

  @Test
  void testSimplifyMatchCondition() {
    assertEquals(
      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(constOf(2), matchAny("a", "b")))
      ), script(FEATURE_SIGNATURE, "size(feature.tags.a)")),

      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(constOf(2), or(or(matchAny("a", "b"))))
      )), script(FEATURE_SIGNATURE, "size(feature.tags.a)")).simplify()
    );
  }

  @Test
  void testSimplifyMatchResultFunction() {
    assertEquals(
      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(constOf(2), matchAny("a", "b")))
      ), script(FEATURE_SIGNATURE, "size(feature.tags.a)")),

      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(coalesce(List.of(constOf(2))), matchAny("a", "b"))
      )), script(FEATURE_SIGNATURE, "size(feature.tags.a)")).simplify()
    );
  }

  @Test
  void testSimplifyFallbackFunction() {
    assertEquals(
      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(constOf(2), matchAny("a", "b")))
      ), constOf(3)),

      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(constOf(2), matchAny("a", "b"))
      )), script(FEATURE_SIGNATURE, "1+2")).simplify()
    );
  }

  @Test
  void testSimplifyFirstTrue() {
    assertEquals(
      constOf(1),

      match(FEATURE_SIGNATURE, MultiExpression.of(List.of(
        MultiExpression.entry(constOf(1),
          BooleanExpressionScript.script("1 < 2", FEATURE_SIGNATURE.in())),
        MultiExpression.entry(constOf(2),
          BooleanExpressionScript.script("1 > 3", FEATURE_SIGNATURE.in()))
      ))).simplify()
    );
  }

  @Test
  void testGetTag() {
    var feature = SimpleFeature.create(newPoint(0, 0), Map.of("abc", "123"), "source", "source_layer", 99);
    assertEquals(
      "123",
      getTag(FEATURE_SIGNATURE.withOutput(Object.class), constOf("abc")).apply(
        TestContexts.ROOT.createProcessFeatureContext(feature, new TagValueProducer(Map.of())))
    );

    assertEquals(
      123,
      getTag(FEATURE_SIGNATURE.withOutput(Object.class), constOf("abc"))
        .apply(TestContexts.ROOT.createProcessFeatureContext(feature, new TagValueProducer(Map.of("abc", "integer"))))
    );

    assertEquals(
      123,
      getTag(signature(FEATURE_POST_MATCH, Object.class), constOf("abc"))
        .apply(TestContexts.ROOT.createProcessFeatureContext(feature, new TagValueProducer(Map.of("abc", "integer")))
          .createPostMatchContext(List.of()))
    );

    assertNull(getTag(signature(ROOT_CONTEXT, Object.class), constOf("abc"))
      .apply(TestContexts.ROOT));
  }

  @Test
  void testCastGetTag() {
    var feature = SimpleFeature.create(newPoint(0, 0), Map.of("abc", "123"), "source", "source_layer", 99);
    var context = TestContexts.ROOT.createProcessFeatureContext(feature, new TagValueProducer(Map.of()));
    var expression = cast(
      FEATURE_SIGNATURE.withOutput(Integer.class),
      getTag(FEATURE_SIGNATURE.withOutput(Object.class), constOf("abc")),
      DataType.GET_INT
    );
    assertEquals(123, expression.apply(context));

    assertEquals(123d, cast(
      FEATURE_SIGNATURE.withOutput(Double.class),
      getTag(FEATURE_SIGNATURE.withOutput(Object.class), constOf("abc")),
      DataType.GET_INT
    ).apply(context));
  }

  @Test
  void testCast() {
    var expression = cast(
      ROOT.withOutput(Integer.class),
      constOf("123"),
      DataType.GET_INT
    );
    assertEquals(123, expression.apply(TestContexts.ROOT));
  }

  @Test
  void testSimplifyCast() {
    assertEquals(constOf(123),
      cast(
        ROOT.withOutput(Integer.class),
        constOf("123"),
        DataType.GET_INT
      ).simplify()
    );
  }

  @Test
  void testSimplifyGetArg() {
    var args = Arguments.of(Map.of("arg", "12"));
    var root = Contexts.buildRootContext(args, Map.of());
    var context = signature(root.description(), Integer.class);
    assertEquals(constOf(12),
      getArg(
        context,
        constOf("arg")
      ).simplify()
    );

    assertEquals(constOf(12),
      script(
        context,
        "args.arg"
      ).simplify()
    );

    assertEquals(constOf(12),
      script(
        context,
        "args['arg']"
      ).simplify()
    );
  }
}
