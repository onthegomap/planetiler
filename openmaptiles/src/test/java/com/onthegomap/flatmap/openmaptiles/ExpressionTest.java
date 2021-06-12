package com.onthegomap.flatmap.openmaptiles;

import static com.onthegomap.flatmap.openmaptiles.Expression.and;
import static com.onthegomap.flatmap.openmaptiles.Expression.matchAny;
import static com.onthegomap.flatmap.openmaptiles.Expression.not;
import static com.onthegomap.flatmap.openmaptiles.Expression.or;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class ExpressionTest {

  public static final Expression.MatchAny matchAB = matchAny("a", "b");
  public static final Expression.MatchAny matchCD = matchAny("c", "d");
  public static final Expression.MatchAny matchBC = matchAny("b", "c");

  @Test
  public void testSimplify() {
    assertEquals(matchAB, matchAB.simplify());
  }

  @Test
  public void testSimplifyAdjacentOrs() {
    assertEquals(or(matchAB, matchCD),
      or(or(matchAB), or(matchCD)).simplify()
    );
  }

  @Test
  public void testSimplifyOrWithOneChild() {
    assertEquals(matchAB, or(matchAB).simplify());
  }

  @Test
  public void testSimplifyOAndWithOneChild() {
    assertEquals(matchAB, and(matchAB).simplify());
  }

  @Test
  public void testSimplifyDeeplyNested() {
    assertEquals(matchAB, or(or(and(and(matchAB)))).simplify());
  }

  @Test
  public void testSimplifyDeeplyNested2() {
    assertEquals(or(matchAB, matchBC),
      or(or(and(and(matchAB)), matchBC)).simplify());
  }

  @Test
  public void testSimplifyDeeplyNested3() {
    assertEquals(or(and(matchAB, matchCD), matchBC),
      or(or(and(and(matchAB), matchCD), matchBC)).simplify());
  }

  @Test
  public void testNotNot() {
    assertEquals(matchAB, not(not(matchAB)).simplify());
  }

  @Test
  public void testDemorgans() {
    assertEquals(or(not(matchAB), not(matchBC)), not(and(matchAB, matchBC)).simplify());
    assertEquals(and(not(matchAB), not(matchBC)), not(or(matchAB, matchBC)).simplify());
  }
}
