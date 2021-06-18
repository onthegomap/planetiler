package com.onthegomap.flatmap.openmaptiles;

import static com.onthegomap.flatmap.openmaptiles.Expression.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;
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

  @Test
  public void testSimplifyFalse() {
    assertEquals(FALSE, and(FALSE).simplify());
    assertEquals(FALSE, and(FALSE, matchAB).simplify());
    assertEquals(FALSE, or(FALSE).simplify());
  }

  @Test
  public void testSimplifyTrue() {
    assertEquals(TRUE, and(TRUE).simplify());
    assertEquals(matchAB, and(TRUE, matchAB).simplify());
    assertEquals(TRUE, or(TRUE, matchAB).simplify());
  }

  @Test
  public void testReplace() {
    assertEquals(
      or(not(matchCD), matchCD, and(matchCD, matchBC)),
      or(not(matchAB), matchAB, and(matchAB, matchBC))
        .replace(matchAB, matchCD));
  }

  @Test
  public void testReplacePredicate() {
    assertEquals(
      or(not(matchCD), matchCD, and(matchCD, matchCD)),
      or(not(matchCD), matchCD, and(matchCD, matchCD))
        .replace(e -> Set.of(matchAB, matchBC).contains(e), matchCD));
  }
}
