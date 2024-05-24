package com.onthegomap.planetiler.reader;

import com.onthegomap.planetiler.util.Parse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * Wrapper for a value that could either be a primitive, list, or map of nested primitives.
 * <p>
 * The APIs are meant to be forgiving, so if you access field a.b.c but "a" is missing from the top-level struct it will
 * just return {@link #NULL} instead of throwing an exception.
 * <p>
 * Values are also coerced to other datatypes when possible, for example:
 * <ul>
 * <li>calling {@link #asLong()} or {@link #asDouble()} on a string will attempt to parse that string to a number
 * <li>calling {@link #get(Object)} on a string will attempt to parse it as JSON and traverse nested data
 * <li>calling {@link #asLong()} on an {@link Instant} will return milliseconds since epoch
 * <li>calling {@link #asLong()} on an {@link LocalDate} will return epoch day
 * <li>calling {@link #asLong()} on an {@link LocalTime} will return millisecond of the day
 * <li>calling {@link #asString()} on anything will return a string representation of it
 * <li>calling {@link #asJson()} will return a json representation of the underlying value
 * </ul>
 */
public interface Struct {
  /** Returns a new struct that wraps a primitive java value, or nested {@link List} or {@link Map} of values. */
  static Struct of(Object o) {
    return switch (o) {
      case null -> NULL;
      case Struct struct -> struct;
      case Number n -> new Numeric(n);
      case Boolean b -> new BooleanStruct(b);
      case String s -> new StringStruct(s);
      case byte[] b -> new BinaryStruct(b);
      case Instant i -> new InstantStruct(i);
      case LocalTime i -> new LocalTimeStruct(i);
      case LocalDate i -> new LocalDateStruct(i);
      case UUID uuid -> new PrimitiveStruct<>(uuid);
      case Map<?, ?> map -> {
        Map<Object, Struct> result = LinkedHashMap.newLinkedHashMap(map.size());
        for (var e : map.entrySet()) {
          var v = of(e.getValue());
          if (!v.isNull()) {
            result.put(e.getKey(), v);
          }
        }
        yield new MapStruct(result);
      }
      case Collection<?> collection -> {
        List<Struct> result = new ArrayList<>(collection.size());
        for (var d : collection) {
          result.add(of(d));
        }
        yield new ListStruct(result);
      }
      default -> throw new IllegalArgumentException("Unable to convert " + o + " (" + o.getClass() + ")");
    };
  }

  /**
   * Returns the nested field of a map struct, an element of an array if {@code key} is numeric, or {@link #NULL} when
   * called on a primitive value.
   */
  default Struct get(Object key) {
    return NULL;
  }

  /** Shortcut for calling {@link #get(Object)} multiple times to query a value several layers deep. */
  default Struct get(Object first, Object... others) {
    Struct struct = first instanceof Number n ? get(n.intValue()) : get(first.toString());
    for (Object other : others) {
      struct = other instanceof Number n ? struct.get(n.intValue()) : struct.get(other.toString());
      if (struct.isNull()) {
        return Struct.NULL;
      }
    }
    return struct;
  }

  /** When this is map, returns a map from key to value struct, otherwise an empty map. */
  default Map<Object, Struct> asMap() {
    return Map.of();
  }

  /** Returns this struct, or {@code fallback} when {@link #NULL} */
  default Struct orElse(Object fallback) {
    return this;
  }

  /** A missing or empty value. */
  Struct NULL = new Struct() {
    @Override
    public Object rawValue() {
      return null;
    }

    @Override
    public List<Struct> asList() {
      return List.of();
    }

    @Override
    public String asString() {
      return null;
    }

    @Override
    public Struct orElse(Object fallback) {
      return of(fallback);
    }

    @Override
    public String toString() {
      return "null";
    }

    @Override
    public String asJson() {
      return "null";
    }

    @Override
    public boolean isNull() {
      return true;
    }

    @Override
    public boolean equals(Object obj) {
      return obj == NULL;
    }

    @Override
    public int hashCode() {
      return 0;
    }
  };

  /** Returns the nth element of a list, or {@link #NULL} when not a list or {@code index} is out of bounds. */
  default Struct get(int index) {
    return NULL;
  }

  /**
   * Returns the list of nested structs in a list, a list of this single element when this is a primitive, or empty list
   * when {@link #NULL}.
   */
  default List<Struct> asList() {
    return List.of(this);
  }

  /**
   * Returns the {@link Number#intValue()} for numeric values, millisecond value for time types, or attempts to parse as
   * a number when this is a string.
   */
  default Integer asInt() {
    return null;
  }

  /**
   * Returns the {@link Number#longValue()} for numeric values, millisecond value for time types, or attempts to parse
   * as a number when this is a string.
   */
  default Long asLong() {
    return null;
  }

  /**
   * Returns the {@link Number#doubleValue()} ()} for numeric values, millisecond value for time types, or attempts to
   * parse as a double when this is a string.
   */
  default Double asDouble() {
    return null;
  }

  /**
   * Returns boolean value of this element, or true for "1", "true", "yes" and false for "0", "false", "no".
   */
  default Boolean asBoolean() {
    return null;
  }

  /** Returns a string representation of this value (use {@link #asJson()} for json string). */
  default String asString() {
    return rawValue() == null ? null : rawValue().toString();
  }


  /**
   * Returns an {@link Instant} parsed from milliseconds since epoch, or a string with an ISO-8601 encoded time string.
   */
  default Instant asTimestamp() {
    return null;
  }

  /** Returns a byte array value or bytes from a UTF8-encoded string. */
  default byte[] asBytes() {
    return null;
  }

  default boolean isNull() {
    return false;
  }

  /** Returns true if this is a map with nested key/value pairs, false for lists or primitives. */
  default boolean isStruct() {
    return false;
  }

  /** Returns the raw primitive, {@link List} or {@link Map} value, with all nested {@link Struct Structs} unwrapped. */
  Object rawValue();

  /**
   * Attempts to marshal this value into a typed java class or record using
   * <a href="https://github.com/FasterXML/jackson-databind">jackson-databind</a>.
   * <p>
   * For example:
   * {@snippet :
   * record Point(double x, double y) {}
   * var point = Struct.of(Map.of("x", 1.5, "y", 2)).as(Point.class);
   * System.out.println(point); // "Point[x=1.5, y=2.0]"
   * }
   */
  default <T> T as(Class<T> clazz) {
    return JsonConversion.convertValue(rawValue(), clazz);
  }

  /** Returns a JSON string representation of the raw value wrapped by this struct. */
  default String asJson() {
    return JsonConversion.writeValueAsString(rawValue());
  }

  /**
   * Returns a new list where each element of this list has been expanded to the list of elements returned by
   * {@code mapper}.
   * <p>
   * Individual items are treated as a list containing just that item.
   */
  default Struct flatMap(UnaryOperator<Struct> mapper) {
    var list = asList().stream()
      .flatMap(item -> mapper.apply(item).asList().stream())
      .map(Struct::of)
      .toList();
    return list.isEmpty() ? NULL : new ListStruct(list);
  }

  class PrimitiveStruct<T> implements Struct {

    final T value;
    private String asJson;

    PrimitiveStruct(T value) {
      this.value = value;
    }


    @Override
    public final Object rawValue() {
      return value;
    }

    @Override
    public String asJson() {
      if (this.asJson == null) {
        this.asJson = Struct.super.asJson();
      }
      return asJson;
    }

    @Override
    public String asString() {
      return value.toString();
    }

    @Override
    public String toString() {
      return asString();
    }

    @Override
    public boolean equals(Object o) {
      return this == o || (o instanceof PrimitiveStruct<?> that && value.equals(that.value));
    }

    @Override
    public int hashCode() {
      return value.hashCode();
    }
  }

  class Numeric extends PrimitiveStruct<Number> {

    Numeric(Number value) {
      super(value);
    }

    @Override
    public Integer asInt() {
      return value.intValue();
    }

    @Override
    public Long asLong() {
      return value.longValue();
    }

    @Override
    public Double asDouble() {
      return value.doubleValue();
    }

    @Override
    public Instant asTimestamp() {
      var raw = Instant.ofEpochMilli(value.longValue());
      if (value instanceof Float || value instanceof Double) {
        double doubleValue = value.doubleValue();
        raw = raw.plusNanos((long) ((doubleValue - Math.floor(doubleValue)) * Duration.ofMillis(1).toNanos()));
      }
      return raw;
    }
  }

  class BooleanStruct extends PrimitiveStruct<Boolean> {

    BooleanStruct(boolean value) {
      super(value);
    }

    @Override
    public Boolean asBoolean() {
      return value == Boolean.TRUE;
    }
  }

  class StringStruct extends PrimitiveStruct<String> {
    private Struct struct = null;


    StringStruct(String value) {
      super(value);
    }

    @Override
    public String asString() {
      return value;
    }

    @Override
    public Integer asInt() {
      return Parse.parseIntOrNull(value);
    }

    @Override
    public Long asLong() {
      return Parse.parseLongOrNull(value);
    }

    @Override
    public Double asDouble() {
      return Parse.parseDoubleOrNull(value);
    }

    @Override
    public Boolean asBoolean() {
      return Parse.bool(value);
    }

    @Override
    public Instant asTimestamp() {
      try {
        return Instant.parse(value);
      } catch (DateTimeParseException e) {
        Long value = asLong();
        if (value != null) {
          return Instant.ofEpochMilli(value);
        }
        return null;
      }
    }

    @Override
    public Struct get(Object key) {
      return parseJson().get(key);
    }

    @Override
    public Struct get(int index) {
      return parseJson().get(index);
    }

    @Override
    public Map<Object, Struct> asMap() {
      return parseJson().asMap();
    }

    private Struct parseJson() {
      return struct != null ? struct : (struct = of(JsonConversion.readValue(value, Object.class)));
    }

    @Override
    public byte[] asBytes() {
      return value.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public <T> T as(Class<T> clazz) {
      return JsonConversion.readValue(value, clazz);
    }
  }

  class BinaryStruct extends PrimitiveStruct<byte[]> {

    BinaryStruct(byte[] value) {
      super(value);
    }

    @Override
    public String asString() {
      return new String(value, StandardCharsets.UTF_8);
    }

    @Override
    public byte[] asBytes() {
      return value;
    }
  }

  class InstantStruct extends PrimitiveStruct<Instant> {

    InstantStruct(Instant value) {
      super(value);
    }

    @Override
    public Instant asTimestamp() {
      return value;
    }

    @Override
    public Integer asInt() {
      return Math.toIntExact(value.toEpochMilli());
    }

    @Override
    public Long asLong() {
      return value.toEpochMilli();
    }

    @Override
    public Double asDouble() {
      return (double) value.toEpochMilli();
    }
  }

  class LocalTimeStruct extends PrimitiveStruct<LocalTime> {

    LocalTimeStruct(LocalTime value) {
      super(value);
    }

    @Override
    public Integer asInt() {
      return Math.toIntExact(Duration.ofNanos(value.toNanoOfDay()).toMillis());
    }

    @Override
    public Long asLong() {
      return Duration.ofNanos(value.toNanoOfDay()).toMillis();
    }

    @Override
    public Double asDouble() {
      return value.toNanoOfDay() * 1d / Duration.ofMillis(1).toNanos();
    }

    @Override
    public String asString() {
      return DateTimeFormatter.ISO_LOCAL_TIME.format(value);
    }
  }

  class LocalDateStruct extends PrimitiveStruct<LocalDate> {

    LocalDateStruct(LocalDate value) {
      super(value);
    }

    @Override
    public Integer asInt() {
      return Math.toIntExact(value.toEpochDay());
    }

    @Override
    public Long asLong() {
      return value.toEpochDay();
    }

    @Override
    public Double asDouble() {
      return (double) value.toEpochDay();
    }

    @Override
    public String asString() {
      return DateTimeFormatter.ISO_LOCAL_DATE.format(value);
    }
  }

  class MapStruct extends PrimitiveStruct<Map<Object, Struct>> {

    MapStruct(Map<Object, Struct> value) {
      super(value);
    }

    @Override
    public Struct get(Object key) {
      var result = value.get(key);
      if (result != null) {
        return result;
      } else if (key instanceof String s) {
        if (s.contains(".")) {
          String[] parts = s.split("\\.", 2);
          if (parts.length == 2) {
            String firstPart = parts[0];
            return firstPart.endsWith("[]") ?
              get(firstPart.substring(0, firstPart.length() - 2)).flatMap(child -> child.get(parts[1])) :
              get(firstPart, parts[1]);
          }
        }
      }
      return NULL;
    }

    @Override
    public boolean isStruct() {
      return true;
    }

    @Override
    public String asString() {
      return super.asJson();
    }

    @Override
    public Map<Object, Struct> asMap() {
      return value.entrySet().stream()
        .map(e -> Map.entry(e.getKey(), e.getValue()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
  }

  class ListStruct extends PrimitiveStruct<List<Struct>> {
    ListStruct(List<Struct> value) {
      super(value);
    }

    @Override
    public List<Struct> asList() {
      return value;
    }

    @Override
    public Struct get(int index) {
      return index < value.size() && index >= 0 ? value.get(index) : NULL;
    }

    @Override
    public Struct get(Object key) {
      return key instanceof Number n ? get(n.intValue()) : NULL;
    }
  }


}
