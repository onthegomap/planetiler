package com.onthegomap.flatmap.openmaptiles;

import static com.onthegomap.flatmap.openmaptiles.Expression.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class MultiExpressionTest {

  @Test
  public void testEmpty() {
    var index = MultiExpression.<String>of(Map.of()).index();
    assertSameElements(List.of(), index.getMatches(Map.of()));
    assertSameElements(List.of(), index.getMatches(Map.of("key", "value")));
  }

  @Test
  public void testSingleElement() {
    var index = MultiExpression.of(Map.of(
      "a", matchAny("key", "value")
    )).index();
    assertSameElements(List.of("a"), index.getMatches(Map.of("key", "value")));
    assertSameElements(List.of("a"), index.getMatches(Map.of("key", "value", "otherkey", "othervalue")));
    assertSameElements(List.of(), index.getMatches(Map.of("key2", "value", "key3", "value")));
    assertSameElements(List.of(), index.getMatches(Map.of("key2", "value")));
    assertSameElements(List.of(), index.getMatches(Map.of("key", "no")));
    assertSameElements(List.of(), index.getMatches(Map.of()));
  }

  @Test
  public void testBlankStringTreatedAsNotMatch() {
    var index = MultiExpression.of(Map.of(
      "a", matchAny("key", "value", "")
    )).index();
    assertSameElements(List.of("a"), index.getMatches(Map.of("key", "value")));
    assertSameElements(List.of("a"), index.getMatches(Map.of("key", "")));
    assertSameElements(List.of("a"), index.getMatches(Map.of()));
    assertSameElements(List.of("a"), index.getMatches(Map.of("otherkey", "othervalue")));
    assertSameElements(List.of("a"), index.getMatches(Map.of("key2", "value", "key3", "value")));
    assertSameElements(List.of("a"), index.getMatches(Map.of("key2", "value")));
    assertSameElements(List.of(), index.getMatches(Map.of("key", "no")));
  }

  @Test
  public void testSingleMatchField() {
    var index = MultiExpression.of(Map.of(
      "a", matchField("key")
    )).index();
    assertSameElements(List.of("a"), index.getMatches(Map.of("key", "value")));
    assertSameElements(List.of("a"), index.getMatches(Map.of("key", "")));
    assertSameElements(List.of("a"), index.getMatches(Map.of("key", "value2", "otherkey", "othervalue")));
    assertSameElements(List.of(), index.getMatches(Map.of("key2", "value", "key3", "value")));
    assertSameElements(List.of(), index.getMatches(Map.of("key2", "value")));
    assertSameElements(List.of(), index.getMatches(Map.of("key2", "no")));
    assertSameElements(List.of(), index.getMatches(Map.of()));
  }

  @Test
  public void testWildcard() {
    var index = MultiExpression.of(Map.of(
      "a", matchAny("key", "%value%")
    )).index();
    assertSameElements(List.of("a"), index.getMatches(Map.of("key", "value")));
    assertSameElements(List.of("a"), index.getMatches(Map.of("key", "value1")));
    assertSameElements(List.of("a"), index.getMatches(Map.of("key", "1value")));
    assertSameElements(List.of("a"), index.getMatches(Map.of("key", "1value1")));
    assertSameElements(List.of("a"), index.getMatches(Map.of("key", "1value1", "otherkey", "othervalue")));
    assertSameElements(List.of(), index.getMatches(Map.of("key2", "value", "key3", "value")));
    assertSameElements(List.of(), index.getMatches(Map.of("key", "no")));
    assertSameElements(List.of(), index.getMatches(Map.of("key2", "value")));
    assertSameElements(List.of(), index.getMatches(Map.of()));
  }

  @Test
  public void testMultipleWildcardsMixedWithExacts() {
    var index = MultiExpression.of(Map.of(
      "a", matchAny("key", "%value%", "other")
    )).index();
    assertSameElements(List.of("a"), index.getMatches(Map.of("key", "1value1")));
    assertSameElements(List.of("a"), index.getMatches(Map.of("key", "other")));
    assertSameElements(List.of("a"), index.getMatches(Map.of("key", "1value1", "otherkey", "othervalue")));
    assertSameElements(List.of("a"), index.getMatches(Map.of("key", "other", "otherkey", "othervalue")));
    assertSameElements(List.of(), index.getMatches(Map.of("key2", "value", "key3", "value")));
    assertSameElements(List.of(), index.getMatches(Map.of("key", "no")));
    assertSameElements(List.of(), index.getMatches(Map.of("key2", "value")));
    assertSameElements(List.of(), index.getMatches(Map.of()));
  }

  @Test
  public void testAnd() {
    var index = MultiExpression.of(Map.of(
      "a", and(
        matchAny("key1", "val1"),
        matchAny("key2", "val2")
      )
    )).index();
    assertSameElements(List.of("a"), index.getMatches(Map.of("key1", "val1", "key2", "val2")));
    assertSameElements(List.of("a"), index.getMatches(Map.of("key1", "val1", "key2", "val2", "key3", "val3")));
    assertSameElements(List.of(), index.getMatches(Map.of("key1", "no", "key2", "val2")));
    assertSameElements(List.of(), index.getMatches(Map.of("key1", "val1", "key2", "no")));
    assertSameElements(List.of(), index.getMatches(Map.of("key1", "val1")));
    assertSameElements(List.of(), index.getMatches(Map.of("key2", "val2")));
    assertSameElements(List.of(), index.getMatches(Map.of()));
  }

  @Test
  public void testOr() {
    var index = MultiExpression.of(Map.of(
      "a", or(
        matchAny("key1", "val1"),
        matchAny("key2", "val2")
      )
    )).index();
    assertSameElements(List.of("a"), index.getMatches(Map.of("key1", "val1", "key2", "val2")));
    assertSameElements(List.of("a"), index.getMatches(Map.of("key1", "val1", "key2", "val2", "key3", "val3")));
    assertSameElements(List.of("a"), index.getMatches(Map.of("key1", "no", "key2", "val2")));
    assertSameElements(List.of("a"), index.getMatches(Map.of("key1", "val1", "key2", "no")));
    assertSameElements(List.of("a"), index.getMatches(Map.of("key1", "val1")));
    assertSameElements(List.of("a"), index.getMatches(Map.of("key2", "val2")));
    assertSameElements(List.of(), index.getMatches(Map.of("key1", "no", "key2", "no")));
    assertSameElements(List.of(), index.getMatches(Map.of()));
  }

  @Test
  public void testNot() {
    var index = MultiExpression.of(Map.of(
      "a", and(
        matchAny("key1", "val1"),
        not(
          matchAny("key2", "val2")
        )
      )
    )).index();
    assertSameElements(List.of("a"), index.getMatches(Map.of("key1", "val1")));
    assertSameElements(List.of(), index.getMatches(Map.of("key1", "val1", "key2", "val2")));
    assertSameElements(List.of("a"), index.getMatches(Map.of("key1", "val1", "key2", "val3")));
    assertSameElements(List.of("a"), index.getMatches(Map.of("key1", "val1", "key3", "val2")));
    assertSameElements(List.of(), index.getMatches(Map.of()));
  }

  @Test
  public void testMatchesMultiple() {
    var index = MultiExpression.of(Map.of(
      "a", or(
        matchAny("key1", "val1"),
        matchAny("key2", "val2")
      ),
      "b", or(
        matchAny("key2", "val2"),
        matchAny("key3", "val3")
      )
    )).index();
    assertSameElements(List.of("a"), index.getMatches(Map.of("key1", "val1")));
    assertSameElements(List.of("a", "b"), index.getMatches(Map.of("key2", "val2")));
    assertSameElements(List.of("b"), index.getMatches(Map.of("key3", "val3")));
    assertSameElements(List.of("a", "b"), index.getMatches(Map.of("key2", "val2", "key3", "val3")));
    assertSameElements(List.of("a", "b"), index.getMatches(Map.of("key1", "val1", "key3", "val3")));
    assertSameElements(List.of(), index.getMatches(Map.of()));
  }

  @Test
  public void testTracksMatchingKey() {
    var index = MultiExpression.of(Map.of(
      "a", or(
        matchAny("key1", "val1"),
        matchAny("key2", "val2")
      ),
      "b", or(
        matchAny("key2", "val2"),
        matchAny("key3", "val3")
      )
    )).index();
    assertSameElements(List.of(new MultiExpression.MultiExpressionIndex.MatchWithTriggers<>(
      "a", List.of("key1")
    )), index.getMatchesWithTriggers(Map.of("key1", "val1")));
    assertSameElements(List.of(new MultiExpression.MultiExpressionIndex.MatchWithTriggers<>(
      "a", List.of("key2")
    ), new MultiExpression.MultiExpressionIndex.MatchWithTriggers<>(
      "b", List.of("key2")
    )), index.getMatchesWithTriggers(Map.of("key2", "val2")));
    assertSameElements(List.of(new MultiExpression.MultiExpressionIndex.MatchWithTriggers<>(
      "b", List.of("key3")
    )), index.getMatchesWithTriggers(Map.of("key3", "val3")));
  }

  @Test
  public void testTracksMatchingKeyFromCorrectPath() {
    var index = MultiExpression.of(Map.of(
      "a", or(
        and(
          matchAny("key3", "val3"),
          matchAny("key2", "val2")
        ),
        and(
          matchAny("key1", "val1"),
          matchAny("key3", "val3")
        )
      )
    )).index();
    assertSameElements(List.of(new MultiExpression.MultiExpressionIndex.MatchWithTriggers<>(
      "a", List.of("key1", "key3")
    )), index.getMatchesWithTriggers(Map.of("key1", "val1", "key3", "val3")));
  }

  private static <T> void assertSameElements(List<T> a, List<T> b) {
    assertEquals(
      a.stream().sorted(Comparator.comparing(Object::toString)).toList(),
      b.stream().sorted(Comparator.comparing(Object::toString)).toList()
    );
  }
}
