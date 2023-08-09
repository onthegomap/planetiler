package com.onthegomap.planetiler.overture;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public interface Struct {
  ObjectMapper mapper = new ObjectMapper()
    .registerModules(new SimpleModule().addSerializer(Struct.class, new StructSerializer()));
  Struct NULL = new Struct() {
    @Override
    public boolean isNull() {
      return true;
    }

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
  };

  static Struct of(Object value) {
    if (value == null) {
      return NULL;
    } else if (value instanceof GenericRecord r) {
      Map<String, Struct> result = new HashMap<>(r.getSchema().getFields().size());
      for (var f : r.getSchema().getFields()) {
        var v = of(r.get(f.name()));
        if (!v.isNull()) {
          result.put(f.name(), v);
        }
      }
      return new MapStruct(result);
    } else if (value instanceof Map<?, ?> map) {
      Map<String, Struct> result = new HashMap<>(map.size());
      for (var e : map.entrySet()) {
        var v = of(e.getValue());
        if (!v.isNull()) {
          result.put(e.getKey().toString(), v);
        }
      }
      return new MapStruct(result);
    } else if (value instanceof Collection<?> collection) {
      List<Struct> result = new ArrayList<>(collection.size());
      for (var d : collection) {
        if (d instanceof GenericRecord r && r.hasField("array_element")) {
          result.add(of(r.get("array_element")));
        } else if (d instanceof Map<?, ?> m && m.containsKey("array_element")) {
          result.add(of(m.get("array_element")));
        } else {
          result.add(of(d));
        }
      }
      return new ListStruct(result);
    } else if ("".equals(value)) {
      return NULL;
    } else if (value instanceof String s) {
      return new StringValue(s);
    } else if (value instanceof Number n) {
      return new Numeric(n);
    } else if (value instanceof ByteBuffer b) {
      byte[] result = new byte[b.remaining()];
      b.get(result);
      return new Bytes(result);
    } else if (value instanceof GenericData.Fixed fixed) {
      return new Bytes(fixed.bytes());
    } else if (value instanceof Boolean b) {
      return new BooleanStruct(b);
    }
    throw new IllegalArgumentException("Unknown type " + value.getClass() + " " + value);
  }

  default boolean isNull() {
    return false;
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

  default String asJson() {
    try {
      return isNull() ? null : mapper.writeValueAsString(rawValue());
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }

  default <T> T as(Class<T> clazz) {
    return mapper.convertValue(rawValue(), clazz);
  }

  default Long asTimestamp() {
    return null;
  }

  default List<Struct> asList() {
    return List.of(this);
  }

  default byte[] asBytes() {
    return null;
  }

  default Struct get(String key) {
    return NULL;
  }

  default Struct get(String key, String... others) {
    Struct struct = get(key);
    for (String other : others) {
      struct = struct.get(other);
      if (struct.isNull()) {
        return NULL;
      }
    }
    return struct;
  }

  Object rawValue();

  abstract class AStruct<T> implements Struct {

    final T value;
    private String asJson;

    AStruct(T value) {
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
  }

  class MapStruct extends AStruct<Map<String, Struct>> {

    MapStruct(Map<String, Struct> value) {
      super(value);
    }

    @Override
    public Struct get(String key) {
      return value.getOrDefault(key, NULL);
    }
  }

  class ListStruct extends AStruct<List<Struct>> {
    ListStruct(List<Struct> value) {
      super(value);
    }

    @Override
    public List<Struct> asList() {
      return value;
    }
  }

  class BooleanStruct extends AStruct<Boolean> {

    BooleanStruct(boolean value) {
      super(value);
    }

    @Override
    public Boolean asBoolean() {
      return value == null || !value ? null : true;
    }
  }

  class Numeric extends AStruct<Number> {

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
  }

  class StringValue extends AStruct<String> {
    private Struct struct = null;

    StringValue(String value) {
      super(value);
    }

    @Override
    public byte[] asBytes() {
      return value.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Long asTimestamp() {
      return Instant.parse(value + (value.endsWith("Z") ? "" : "Z")).toEpochMilli();
    }

    @Override
    public Struct get(String key) {
      if (struct == null) {
        try {
          struct = of(mapper.readValue(value, Object.class));
        } catch (JsonProcessingException e) {
          struct = NULL;
        }
      }
      return struct.get(key);
    }
  }

  class Bytes extends AStruct<byte[]> {

    Bytes(byte[] value) {
      super(value);
    }

    @Override
    public byte[] asBytes() {
      return value;
    }

    @Override
    public String asString() {
      return new String(value, StandardCharsets.UTF_8);
    }

    @Override
    public Long asTimestamp() {
      ByteBuffer buf = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN);
      long timeOfDayNanos = buf.getLong();
      int julianDay = buf.getInt();
      long nanosecondsSinceUnixEpoch = (julianDay - 2440588) * (86400L * 1000 * 1000 * 1000) + timeOfDayNanos;
      return Duration.ofNanos(nanosecondsSinceUnixEpoch).toMillis();
    }
  }
}
