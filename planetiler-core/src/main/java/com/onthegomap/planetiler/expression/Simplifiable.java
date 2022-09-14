package com.onthegomap.planetiler.expression;

import java.util.HashSet;
import java.util.Set;

public interface Simplifiable<T extends Simplifiable<T>> {

  default T simplifyOnce() {
    return self();
  }

  default T self() {
    @SuppressWarnings("unchecked") T self = (T) this;
    return self;
  }

  /** Returns an equivalent, simplified copy of this expression but does not modify {@code this}. */
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
