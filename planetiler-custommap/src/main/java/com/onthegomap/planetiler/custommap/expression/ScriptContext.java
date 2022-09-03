package com.onthegomap.planetiler.custommap.expression;

import com.google.common.base.Function;

/**
 * The runtime environment of an executing CEL expression that returns variables by their name.
 */
public interface ScriptContext extends Function<String, Object> {
  static ScriptContext empty() {
    return key -> null;
  }
}
