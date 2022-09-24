package com.onthegomap.planetiler.expression;

import com.onthegomap.planetiler.reader.WithTags;
import com.onthegomap.planetiler.util.Parse;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;

/**
 * Destination data types for an attribute that link the type to functions that can parse the value from an input object
 */
public enum DataType implements BiFunction<WithTags, String, Object> {
  GET_STRING("string", WithTags::getString, Objects::toString),
  GET_BOOLEAN("boolean", WithTags::getBoolean, Parse::bool),
  GET_DIRECTION("direction", WithTags::getDirection, Parse::direction),
  GET_LONG("long", WithTags::getLong, Parse::parseLongOrNull),
  GET_INT("integer", Parse::parseIntOrNull),
  GET_DOUBLE("double", Parse::parseDoubleOrNull),
  GET_TAG("get", WithTags::getTag, s -> s);

  private final BiFunction<WithTags, String, Object> getter;
  private final String id;
  private final UnaryOperator<Object> parser;

  DataType(String id, BiFunction<WithTags, String, Object> getter, UnaryOperator<Object> parser) {
    this.id = id;
    this.getter = getter;
    this.parser = parser;
  }

  DataType(String id, UnaryOperator<Object> parser) {
    this(id, (d, k) -> parser.apply(d.getTag(k)), parser);
  }

  @Override
  public Object apply(WithTags withTags, String string) {
    return this.getter.apply(withTags, string);
  }

  public Object convertFrom(Object value) {
    return this.parser.apply(value);
  }

  /** Returns the data type associated with {@code id}, or {@link #GET_TAG} as a fallback. */
  public static DataType from(String id) {
    for (var value : values()) {
      if (value.id.equals(id)) {
        return value;
      }
    }
    return GET_TAG;
  }

  public String id() {
    return id;
  }

  public UnaryOperator<Object> parser() {
    return parser;
  }
}
