package com.onthegomap.planetiler.expression;

import com.onthegomap.planetiler.reader.WithTags;
import com.onthegomap.planetiler.util.Parse;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;

public enum ValueGetter implements BiFunction<WithTags, String, Object> {
  GET_STRING("string", WithTags::getString, s -> s),
  GET_BOOLEAN("boolean", WithTags::getBoolean, Parse::bool),
  GET_DIRECTION("direction", WithTags::getDirection, Parse::direction),
  GET_LONG("long", WithTags::getLong, Parse::parseLong),
  GET_TAG("get", WithTags::getTag, s -> s);

  private final BiFunction<WithTags, String, Object> getter;
  private final String id;
  private final UnaryOperator<Object> parser;

  ValueGetter(String id, BiFunction<WithTags, String, Object> getter, UnaryOperator<Object> parser) {
    this.id = id;
    this.getter = getter;
    this.parser = parser;
  }

  @Override
  public Object apply(WithTags withTags, String string) {
    return this.getter.apply(withTags, string);
  }

  public static ValueGetter from(String id) {
    for (var value : values()) {
      if (value.id.equals(id)) {
        return value;
      }
    }
    return GET_TAG;
  }

  public UnaryOperator<Object> parser() {
    return parser;
  }
}
