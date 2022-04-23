package com.onthegomap.planetiler.expression;

import static com.onthegomap.planetiler.TestUtils.newLineString;
import static com.onthegomap.planetiler.TestUtils.newPoint;
import static com.onthegomap.planetiler.TestUtils.rectangle;
import static com.onthegomap.planetiler.expression.Expression.*;
import static com.onthegomap.planetiler.expression.MultiExpression.entry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.reader.SimpleFeature;
import com.onthegomap.planetiler.reader.SourceFeature;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class MultiExpressionTest {

  private static SourceFeature featureWithTags(String... tags) {
    Map<String, Object> map = new HashMap<>();
    for (int i = 0; i < tags.length; i += 2) {
      map.put(tags[i], tags[i + 1]);
    }
    return SimpleFeature.create(newPoint(0, 0), map);
  }

  @Test
  public void testEmpty() {
    var index = MultiExpression.<String>of(List.of()).index();
    assertSameElements(List.of(), index.getMatches(featureWithTags()));
    assertSameElements(List.of(), index.getMatches(featureWithTags("key", "value")));
  }

  @Test
  public void testSingleElement() {
    var index = MultiExpression.of(List.of(
      entry("a", matchAny("key", "value"))
    )).index();
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key", "value")));
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key", "value", "otherkey", "othervalue")));
    assertSameElements(List.of(), index.getMatches(featureWithTags("key2", "value", "key3", "value")));
    assertSameElements(List.of(), index.getMatches(featureWithTags("key2", "value")));
    assertSameElements(List.of(), index.getMatches(featureWithTags("key", "no")));
    assertSameElements(List.of(), index.getMatches(featureWithTags()));
  }

  @Test
  public void testSingleElementBooleanTrue() {
    var index = MultiExpression.of(List.of(
      entry("a", matchAnyTyped("key", BOOLEAN_DATATYPE, true))
    )).index();
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key", "true")));
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key", "yes")));
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key", "1")));
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key", "true", "otherkey", "othervalue")));
    assertSameElements(List.of(), index.getMatches(featureWithTags("key2", "true", "key3", "value")));
    assertSameElements(List.of(), index.getMatches(featureWithTags("key2", "value")));
    assertSameElements(List.of(), index.getMatches(featureWithTags("key", "false")));
    assertSameElements(List.of(), index.getMatches(featureWithTags()));
  }

  @Test
  public void testSingleElementBooleanFalse() {
    var index = MultiExpression.of(List.of(
      entry("a", matchAnyTyped("key", BOOLEAN_DATATYPE, false))
    )).index();
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key", "false")));
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key", "no")));
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key", "0")));
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key", "false", "otherkey", "othervalue")));
    assertSameElements(List.of(), index.getMatches(featureWithTags("key2", "false", "key3", "value")));
    assertSameElements(List.of(), index.getMatches(featureWithTags("key2", "value")));
    assertSameElements(List.of(), index.getMatches(featureWithTags("key", "true")));
    assertSameElements(List.of(), index.getMatches(featureWithTags()));
  }

  @Test
  public void testSingleElementLong() {
    var index = MultiExpression.of(List.of(
      entry("a", matchAnyTyped("key", LONG_DATATYPE, 42))
    )).index();
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key", "42")));
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key", "42", "otherkey", "othervalue")));
    assertSameElements(List.of(), index.getMatches(featureWithTags("key2", "42", "key3", "value")));
    assertSameElements(List.of(), index.getMatches(featureWithTags("key2", "value")));
    assertSameElements(List.of(), index.getMatches(featureWithTags("key", "99")));
    assertSameElements(List.of(), index.getMatches(featureWithTags()));
  }

  @Test
  public void testSingleElementDirection() {
    var index = MultiExpression.of(List.of(
      entry("a", matchAnyTyped("key", DIRECTION_DATATYPE, 1))
    )).index();
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key", "yes")));
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key", "1", "otherkey", "othervalue")));
    assertSameElements(List.of(), index.getMatches(featureWithTags("key2", "1", "key3", "value")));
    assertSameElements(List.of(), index.getMatches(featureWithTags("key2", "value")));
    assertSameElements(List.of(), index.getMatches(featureWithTags("key", "99")));
    assertSameElements(List.of(), index.getMatches(featureWithTags()));
  }

  @Test
  public void testBlankStringTreatedAsNotMatch() {
    var index = MultiExpression.of(List.of(
      entry("a", matchAny("key", "value", ""))
    )).index();
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key", "value")));
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key", "")));
    assertSameElements(List.of("a"), index.getMatches(featureWithTags()));
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("otherkey", "othervalue")));
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key2", "value", "key3", "value")));
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key2", "value")));
    assertSameElements(List.of(), index.getMatches(featureWithTags("key", "no")));
  }

  @Test
  public void testSingleMatchField() {
    var index = MultiExpression.of(List.of(
      entry("a", matchField("key"))
    )).index();
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key", "value")));
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key", "")));
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key", "value2", "otherkey", "othervalue")));
    assertSameElements(List.of(), index.getMatches(featureWithTags("key2", "value", "key3", "value")));
    assertSameElements(List.of(), index.getMatches(featureWithTags("key2", "value")));
    assertSameElements(List.of(), index.getMatches(featureWithTags("key2", "no")));
    assertSameElements(List.of(), index.getMatches(featureWithTags()));
  }

  @Test
  public void testWildcard() {
    var index = MultiExpression.of(List.of(
      entry("a", matchAny("key", "%value%"))
    )).index();
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key", "value")));
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key", "value1")));
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key", "1value")));
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key", "1value1")));
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key", "1value1", "otherkey", "othervalue")));
    assertSameElements(List.of(), index.getMatches(featureWithTags("key2", "value", "key3", "value")));
    assertSameElements(List.of(), index.getMatches(featureWithTags("key", "no")));
    assertSameElements(List.of(), index.getMatches(featureWithTags("key2", "value")));
    assertSameElements(List.of(), index.getMatches(featureWithTags()));
  }

  @Test
  public void testMultipleWildcardsMixedWithExacts() {
    var index = MultiExpression.of(List.of(
      entry("a", matchAny("key", "%value%", "other"))
    )).index();
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key", "1value1")));
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key", "other")));
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key", "1value1", "otherkey", "othervalue")));
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key", "other", "otherkey", "othervalue")));
    assertSameElements(List.of(), index.getMatches(featureWithTags("key2", "value", "key3", "value")));
    assertSameElements(List.of(), index.getMatches(featureWithTags("key", "no")));
    assertSameElements(List.of(), index.getMatches(featureWithTags("key2", "value")));
    assertSameElements(List.of(), index.getMatches(featureWithTags()));
  }

  @Test
  public void testAnd() {
    var index = MultiExpression.of(List.of(
      entry("a", and(
        matchAny("key1", "val1"),
        matchAny("key2", "val2")
      ))
    )).index();
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key1", "val1", "key2", "val2")));
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key1", "val1", "key2", "val2", "key3", "val3")));
    assertSameElements(List.of(), index.getMatches(featureWithTags("key1", "no", "key2", "val2")));
    assertSameElements(List.of(), index.getMatches(featureWithTags("key1", "val1", "key2", "no")));
    assertSameElements(List.of(), index.getMatches(featureWithTags("key1", "val1")));
    assertSameElements(List.of(), index.getMatches(featureWithTags("key2", "val2")));
    assertSameElements(List.of(), index.getMatches(featureWithTags()));
  }

  @Test
  public void testOr() {
    var index = MultiExpression.of(List.of(
      entry("a", or(
        matchAny("key1", "val1"),
        matchAny("key2", "val2")
      ))
    )).index();
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key1", "val1", "key2", "val2")));
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key1", "val1", "key2", "val2", "key3", "val3")));
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key1", "no", "key2", "val2")));
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key1", "val1", "key2", "no")));
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key1", "val1")));
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key2", "val2")));
    assertSameElements(List.of(), index.getMatches(featureWithTags("key1", "no", "key2", "no")));
    assertSameElements(List.of(), index.getMatches(featureWithTags()));
  }

  @Test
  public void testNot() {
    var index = MultiExpression.of(List.of(
      entry("a", and(
        matchAny("key1", "val1"),
        not(
          matchAny("key2", "val2")
        )
      ))
    )).index();
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key1", "val1")));
    assertSameElements(List.of(), index.getMatches(featureWithTags("key1", "val1", "key2", "val2")));
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key1", "val1", "key2", "val3")));
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key1", "val1", "key3", "val2")));
    assertSameElements(List.of(), index.getMatches(featureWithTags()));
  }

  @Test
  public void testMatchesMultiple() {
    var index = MultiExpression.of(List.of(
      entry("a", or(
        matchAny("key1", "val1"),
        matchAny("key2", "val2")
      )),
      entry("b", or(
        matchAny("key2", "val2"),
        matchAny("key3", "val3")
      ))
    )).index();
    assertSameElements(List.of("a"), index.getMatches(featureWithTags("key1", "val1")));
    assertSameElements(List.of("a", "b"), index.getMatches(featureWithTags("key2", "val2")));
    assertSameElements(List.of("b"), index.getMatches(featureWithTags("key3", "val3")));
    assertSameElements(List.of("a", "b"), index.getMatches(featureWithTags("key2", "val2", "key3", "val3")));
    assertSameElements(List.of("a", "b"), index.getMatches(featureWithTags("key1", "val1", "key3", "val3")));
    assertSameElements(List.of(), index.getMatches(featureWithTags()));
  }

  @Test
  public void testTracksMatchingKey() {
    var index = MultiExpression.of(List.of(
      entry("a", or(
        matchAny("key1", "val1"),
        matchAny("key2", "val2")
      )),
      entry("b", or(
        matchAny("key2", "val2"),
        matchAny("key3", "val3")
      ))
    )).index();
    assertSameElements(List.of(new MultiExpression.Match<>(
      "a", List.of("key1")
    )), index.getMatchesWithTriggers(featureWithTags("key1", "val1")));
    assertSameElements(List.of(new MultiExpression.Match<>(
      "a", List.of("key2")
    ), new MultiExpression.Match<>(
      "b", List.of("key2")
    )), index.getMatchesWithTriggers(featureWithTags("key2", "val2")));
    assertSameElements(List.of(new MultiExpression.Match<>(
      "b", List.of("key3")
    )), index.getMatchesWithTriggers(featureWithTags("key3", "val3")));
  }

  @Test
  public void testTracksMatchingKeyFromCorrectPath() {
    var index = MultiExpression.of(List.of(
      entry("a", or(
        and(
          matchAny("key3", "val3"),
          matchAny("key2", "val2")
        ),
        and(
          matchAny("key1", "val1"),
          matchAny("key3", "val3")
        )
      ))
    )).index();
    assertSameElements(List.of(new MultiExpression.Match<>(
      "a", List.of("key1", "key3")
    )), index.getMatchesWithTriggers(featureWithTags("key1", "val1", "key3", "val3")));
  }

  @Test
  public void testMatchDifferentTypes() {
    Expression polygonExpression = and(matchType("polygon"), matchField("field"));
    Expression linestringExpression = and(matchType("linestring"), matchField("field"));
    Expression pointExpression = and(matchType("point"), matchField("field"));
    Map<String, Object> map = Map.of("field", "value");
    SourceFeature point = SimpleFeature.create(newPoint(0, 0), map);
    SourceFeature linestring = SimpleFeature.create(newLineString(0, 0, 1, 1), map);
    SourceFeature polygon = SimpleFeature.create(rectangle(0, 1), map);
    var index = MultiExpression.of(List.of(
      entry("polygon", polygonExpression),
      entry("linestring", linestringExpression),
      entry("point", pointExpression)
    )).index();
    assertTrue(pointExpression.evaluate(point, new ArrayList<>()));
    assertTrue(linestringExpression.evaluate(linestring, new ArrayList<>()));
    assertTrue(polygonExpression.evaluate(polygon, new ArrayList<>()));
    assertEquals("point", index.getOrElse(point, null));
    assertEquals("linestring", index.getOrElse(linestring, null));
    assertEquals("polygon", index.getOrElse(polygon, null));
  }

  private static <T> void assertSameElements(List<T> a, List<T> b) {
    assertEquals(
      a.stream().sorted(Comparator.comparing(Object::toString)).toList(),
      b.stream().sorted(Comparator.comparing(Object::toString)).toList()
    );
  }
}
