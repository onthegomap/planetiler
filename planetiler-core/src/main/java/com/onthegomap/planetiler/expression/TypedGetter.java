package com.onthegomap.planetiler.expression;

import com.onthegomap.planetiler.reader.WithTags;

@FunctionalInterface
public interface TypedGetter {
  Object apply(WithTags withTags, String tag);
}
