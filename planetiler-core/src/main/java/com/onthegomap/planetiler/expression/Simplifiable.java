package com.onthegomap.planetiler.expression;

import java.util.HashSet;
import java.util.Set;

/**
 * An expression that can be simplified to an equivalent, but cheaper to evaluate expression.
 * <p>
 * Implementers should only override {@link #simplifyOnce()} which applies all the rules that can be used to simplify
 * this expression, and {@link #simplify()} will take care of applying it repeatedly until the output settles to a fixed
 * point.
 * <p>
 * Implementers must also ensure {@code equals} and {@code hashCode} reflect equivalence between expressions so that
 * {@link #simplify()} can know when to stop.
 */
public interface Simplifiable<T extends Simplifiable<T>> {

  /**
   * Returns a copy of this expression, with all simplification rules applied once.
   * <p>
   * {@link #simplify()} will take care of applying it repeatedly until the output settles.
   */
  default T simplifyOnce() {
    return self();
  }

  default T self() {
    @SuppressWarnings("unchecked") T self = (T) this;
    return self;
  }

  /**
   * Returns an equivalent, simplified copy of this expression but does not modify {@code this} by repeatedly running
   * {@link #simplifyOnce()}.
   */
  default T simplify() {
    // iteratively simplify the expression until we reach a fixed point and start seeing
    // an expression that's already been seen before
    T simplified = self();
    Set<T> seen = new HashSet<>();
    seen.add(simplified);
    while (true) {
      simplified = simplified.simplifyOnce();
      if (seen.contains(simplified)) {
        return simplified;
      }
      if (seen.size() > 1000) {
        throw new IllegalStateException("Infinite loop while simplifying " + this);
      }
      seen.add(simplified);
    }
  }
}
