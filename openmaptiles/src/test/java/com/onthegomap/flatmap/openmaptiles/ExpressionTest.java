package com.onthegomap.flatmap.openmaptiles;

import static com.onthegomap.flatmap.openmaptiles.Expression.and;
import static com.onthegomap.flatmap.openmaptiles.Expression.matchAny;
import static com.onthegomap.flatmap.openmaptiles.Expression.or;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class ExpressionTest {

  @Test
  public void testSimplify() {
    assertEquals(matchAny("a", "b"), matchAny("a", "b").simplify());
  }

  @Test
  public void testSimplifyAdjacentOrs() {
    assertEquals(or(matchAny("a", "b"), matchAny("c", "d")),
      or(or(matchAny("a", "b")), or(matchAny("c", "d"))).simplify()
    );
  }

  @Test
  public void testSimplifyOrWithOneChild() {
    assertEquals(matchAny("a", "b"), or(matchAny("a", "b")).simplify());
  }

  @Test
  public void testSimplifyOAndWithOneChild() {
    assertEquals(matchAny("a", "b"), and(matchAny("a", "b")).simplify());
  }

  @Test
  public void testSimplifyDeeplyNested() {
    assertEquals(matchAny("a", "b"), or(or(and(and(matchAny("a", "b"))))).simplify());
  }

  @Test
  public void testSimplifyDeeplyNested2() {
    assertEquals(or(matchAny("a", "b"), matchAny("b", "c")),
      or(or(and(and(matchAny("a", "b"))), matchAny("b", "c"))).simplify());
  }

  @Test
  public void testSimplifyDeeplyNested3() {
    assertEquals(or(and(matchAny("a", "b"), matchAny("c", "d")), matchAny("b", "c")),
      or(or(and(and(matchAny("a", "b")), matchAny("c", "d")), matchAny("b", "c"))).simplify());
  }
}
