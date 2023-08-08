package com.onthegomap.planetiler.overture;

import static com.onthegomap.planetiler.overture.OvertureSchema.mapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;


public class Struct {
  private final Object value;
  private static final Struct NULL = new Struct(null);

  private Struct(Object value) {
    this.value = value;
  }

  public static Struct of(Object value) {
    return value == null ? NULL : new Struct(value);
  }

  public static Object convert(Object value) {
    if (value instanceof GenericRecord r) {
      Map<String, Object> result = new HashMap<>(r.getSchema().getFields().size());
      for (Schema.Field f : r.getSchema().getFields()) {
        var v = convert(r.get(f.name()));
        if (v != null) {
          result.put(f.name(), v);
        }
      }
      return result;
    } else if (value instanceof Map<?, ?> map) {
      Map<String, Object> result = new HashMap<>(map.size());
      for (Map.Entry<?, ?> e : map.entrySet()) {
        var v = convert(e.getValue());
        if (v != null) {
          result.put(e.getKey().toString(), v);
        }
      }
      return result;
    } else if (value instanceof Collection<?> collection) {
      List<Object> result = new ArrayList<>(collection.size());
      for (var d : collection) {
        if (d instanceof GenericRecord r && r.hasField("array_element")) {
          result.add(convert(r.get("array_element")));
        } else if (d instanceof Map<?, ?> m && m.containsKey("array_element")) {
          result.add(convert(m.get("array_element")));
        } else {
          result.add(convert(d));
        }
      }
      return result;
    } else if ("".equals(value)) {
      return null;
    }
    return value;
  }

  private static final Pattern DOT = Pattern.compile("\\.");

  // Schema records are implemented as org.apache.avro.generic.GenericRecord.
  //Schema enums are implemented as org.apache.avro.generic.GenericEnumSymbol.
  //Schema arrays are implemented as java.util.Collection.
  //Schema maps are implemented as java.util.Map.
  //Schema fixed are implemented as org.apache.avro.generic.GenericFixed.
  //Schema strings are implemented as CharSequence.
  //Schema bytes are implemented as java.nio.ByteBuffer.
  //Schema ints are implemented as Integer.
  //Schema longs are implemented as Long.
  //Schema floats are implemented as Float.
  //Schema doubles are implemented as Double.
  //Schema booleans are implemented as Boolean.

  private Struct getChild(String field) {
    Object child = null;
    if (value instanceof GenericRecord rec) {
      child = rec.get(field);
    } else if (value instanceof Map<?, ?> map) {
      child = map.get(field);
    } else if (value instanceof String s) {
      try {
        child = mapper.readValue(s, Map.class).get(field);
      } catch (JsonProcessingException e) {
        return of(null);
      }
    }
    return of(child);
  }

  public Struct get(String path) {
    String[] parts = DOT.split(path);
    Struct subject = this;
    for (String part : parts) {
      subject = subject.getChild(part);
      if (subject.isNull()) {
        break;
      }
    }
    return subject;
  }

  public boolean isNull() {
    return value == null;
  }


  public Integer asInt() {
    return value instanceof Integer i ? i : value instanceof Number n ? n.intValue() : null;
  }

  public Long asLong() {
    return value instanceof Long i ? i : value instanceof Number n ? n.longValue() : null;
  }

  public Boolean asBoolean() {
    return value instanceof Boolean b && b;
  }

  public String asString() {
    return value == null ? null : value.toString();
  }

  public Double asDouble() {
    return value instanceof Double i ? i : value instanceof Number n ? n.doubleValue() : null;
  }

  public String asJson() {
    try {
      return value == null ? null : mapper.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }

  public Long asTimestamp() {
    if (value instanceof GenericData.Fixed fixed) {
      ByteBuffer buf = ByteBuffer.wrap(fixed.bytes()).order(ByteOrder.LITTLE_ENDIAN);
      long timeOfDayNanos = buf.getLong();
      int julianDay = buf.getInt();
      long nanosecondsSinceUnixEpoch = (julianDay - 2440588) * (86400L * 1000 * 1000 * 1000) + timeOfDayNanos;
      return Duration.ofNanos(nanosecondsSinceUnixEpoch).toMillis();
    } else if (value instanceof String s) {
      return Instant.parse(s + (s.endsWith("Z") ? "" : "Z")).toEpochMilli();
    }
    return null;
  }

  public List<Struct> asList() {
    if (isNull()) {
      return List.of();
    } else if (value instanceof Collection<?> c) {
      List<Struct> result = new ArrayList<>(c.size());
      for (var d : c) {
        result.add(of(d));
      }
      return result;
    } else {
      return List.of(this);
    }
  }

  public byte[] asBytes() {
    if (value instanceof ByteBuffer buf) {
      byte[] bytes = new byte[buf.remaining()];
      buf.get(bytes);
      return bytes;
    } else if (value instanceof GenericData.Fixed fixed) {
      return fixed.bytes();
    }
    return null;
  }

  public <T> T as(Class<T> clazz) {
    return mapper.convertValue(value, clazz);
  }

  public Object rawValue() {
    return value;
  }
}
