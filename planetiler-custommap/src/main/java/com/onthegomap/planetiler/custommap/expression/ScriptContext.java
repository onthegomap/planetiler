package com.onthegomap.planetiler.custommap.expression;

import com.google.common.base.Function;
import com.onthegomap.planetiler.custommap.TagValueProducer;
import com.onthegomap.planetiler.reader.WithTags;
import java.util.Map;

/**
 * The runtime environment of an executing expression script that returns variables by their name.
 */
public interface ScriptContext extends Function<String, Object>, WithTags {
  static ScriptContext empty() {
    return key -> null;
  }

  @Override
  default Map<String, Object> tags() {
    // TODO remove this when MultiExpression can take any object
    return Map.of();
  }

  default TagValueProducer tagValueProducer() {
    return TagValueProducer.EMPTY;
  }
}
