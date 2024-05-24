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
import java.util.stream.Collectors;

public interface Struct {
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
  };

  default Struct orElse(Object fallback) {
    return this;
  }

  default Struct get(String key) {
    return NULL;
  }

  default Struct get(Object first, Object... others) {
    Struct struct = first instanceof Number n ? get(n.intValue()) : get(first.toString());
    for (Object other : others) {
      struct = other instanceof Number n ? struct.get(n.intValue()) : struct.get(other.toString());
      if (struct.isNull()) {
        return NULL;
      }
    }
    return struct;
  }

  default Map<Object, Struct> asMap() {
    return Map.of();
  }

  default Struct get(int index) {
    return NULL;
  }

  default List<Struct> asList() {
    return List.of(this);
  }

  default Integer asInt() {
    return null;
  }

  default Long asLong() {
    return null;
  }

  default Boolean asBoolean() {
    return null;
  }

  default String asString() {
    return rawValue() == null ? null : rawValue().toString();
  }

  default Double asDouble() {
    return null;
  }

  default Instant asTimestamp() {
    return null;
  }

  default byte[] asBytes() {
    return null;
  }

  default boolean isNull() {
    return false;
  }

  default boolean isStruct() {
    return false;
  }

  Object rawValue();

  default <T> T as(Class<T> clazz) {
    return JsonConversion.convertValue(rawValue(), clazz);
  }

  default String asJson() {
    return JsonConversion.writeValueAsString(rawValue());
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
    public Struct get(String key) {
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
    public Struct get(String key) {
      return value.getOrDefault(key, NULL);
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
  }
}
