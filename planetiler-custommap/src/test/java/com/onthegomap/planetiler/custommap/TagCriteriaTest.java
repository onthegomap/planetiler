package com.onthegomap.planetiler.custommap;

import static com.onthegomap.planetiler.expression.Expression.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.expression.Expression;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TagCriteriaTest {
  private static final TagValueProducer TVP = new TagValueProducer(Map.of());

  private static void assertParse(String yaml, Expression parsed) {
    Object expression = YAML.load(yaml, Object.class);
    var actual = TagCriteria.matcher(expression, TVP);
    assertEquals(
      parsed.simplify().generateJavaCode(),
      actual.simplify().generateJavaCode()
    );
    assertEquals(
      parsed.simplify(),
      actual.simplify()
    );
  }

  @Test
  void testEmpty() {
    assertParse("""
      """,
      Expression.FALSE);
  }

  @Test
  void testSingleValue() {
    assertParse("""
      a: b
      """,
      matchAny("a", "b")
    );
  }

  @Test
  void testMultivalue() {
    assertParse("""
      a:
      - b
      - c
      """,
      or(matchAny("a", "b", "c"))
    );
  }

  @Test
  void testMultiKey() {
    assertParse("""
      a: b
      c: [d, e]
      """,
      or(
        matchAny("a", "b"),
        matchAny("c", "d", "e")
      )
    );
  }

  @Test
  void testAnyValue() {
    assertParse("""
      a: __any__
      """,
      matchField("a")
    );
    assertParse("""
      a: __ANY__
      """,
      matchField("a")
    );
    assertParse("""
      a: [b, __any__]
      """,
      matchField("a")
    );
  }

  @Test
  void testEscapeAny() {
    assertParse("""
      a: \\__any__
      b: [\\__any__]
      """,
      or(
        matchAny("a", "__any__"),
        matchAny("b", "__any__")
      )
    );
    assertParse("""
      a: \\__ANY__
      """,
      matchAny("a", "__ANY__")
    );
    assertParse("""
      a: \\\\__any__
      """,
      matchAny("a", "\\__any__")
    );
    assertParse("""
      a: \\\\__ANY__
      """,
      matchAny("a", "\\__ANY__")
    );
  }

  @Test
  void testMatchAnything() {
    assertParse("__any__", Expression.TRUE);
  }

  @Test
  void testAnyWrapper() {
    assertParse("""
      __any__:
        a: b
        c: d
      """,
      or(
        matchAny("a", "b"),
        matchAny("c", "d")
      )
    );
  }

  @Test
  void testAllWrapper() {
    assertParse("""
      __all__:
        a: b
        c: d
      """,
      and(
        matchAny("a", "b"),
        matchAny("c", "d")
      )
    );
  }

  @Test
  void testNestedNot() {
    assertParse("""
      __all__:
        a: b
        __not__:
          c: d
      """,
      and(
        matchAny("a", "b"),
        not(
          matchAny("c", "d")
        )
      )
    );
  }

  @Test
  void testNestedAnd() {
    assertParse("""
      a: b
      __ALL__:
        c: d
        e: f
      """,
      or(
        matchAny("a", "b"),
        and(
          matchAny("c", "d"),
          matchAny("e", "f")
        )
      )
    );
  }

  @Test
  void testNestedAndOrNot() {
    assertParse("""
      a: b
      __ALL__:
        c: d
        __NOT__:
          e: f
          g: h
      """,
      or(
        matchAny("a", "b"),
        and(
          matchAny("c", "d"),
          not(or(
            matchAny("e", "f"),
            matchAny("g", "h")
          ))
        )
      )
    );
  }

  @Test
  void testActualAnyAllUnescaped() {
    assertParse("""
      a: b
      __any__: d
      __all__: d
      """,
      or(
        matchAny("a", "b"),
        matchAny("__any__", "d"),
        matchAny("__all__", "d")
      )
    );
  }

  @Test
  void testActualAnyAll() {
    assertParse("""
      a: b
      \\__any__: d
      \\__all__: d
      """,
      or(
        matchAny("a", "b"),
        matchAny("__any__", "d"),
        matchAny("__all__", "d")
      )
    );
  }

  @Test
  void testActualAnyAllList() {
    assertParse("""
      a: b
      \\__any__: [d1, d2]
      \\__all__: [e1, e2]
      """,
      or(
        matchAny("a", "b"),
        matchAny("__any__", "d1", "d2"),
        matchAny("__all__", "e1", "e2")
      )
    );
  }

  @Test
  void testList() {
    assertParse("""
      __all__:
        - a: b
        - __any__:
          - c: d
          - e: f
        - __any__:
          - g: h
          - j: i
      """,
      and(
        matchAny("a", "b"),
        or(
          matchAny("c", "d"),
          matchAny("e", "f")
        ),
        or(
          matchAny("g", "h"),
          matchAny("j", "i")
        )
      )
    );
  }
}
