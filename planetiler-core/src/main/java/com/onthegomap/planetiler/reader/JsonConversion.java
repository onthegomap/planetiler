package com.onthegomap.planetiler.reader;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.UncheckedIOException;

/**
 * Utilities for converting between JSON strings and java objects using Jackson utilities.
 * <p>
 * {@link ObjectMapper} are expensive to construct, but not thread safe, so this class reuses the same object mapper
 * within each thread but does not share between threads.
 */
class JsonConversion {
  private JsonConversion() {}

  private static final ThreadLocal<ObjectMapper> MAPPERS = ThreadLocal.withInitial(() -> JsonMapper.builder()
    .addModule(
      new JavaTimeModule().addSerializer(Struct.class, new StructSerializer())
    )
    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
    .build());

  public static String writeValueAsString(Object o) {
    try {
      return o == null ? null : MAPPERS.get().writeValueAsString(o);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static <T> T convertValue(Object o, Class<T> clazz) {
    return o == null ? null : MAPPERS.get().convertValue(o, clazz);
  }

  public static <T> T readValue(String string, Class<T> clazz) {
    try {
      return string == null ? null : MAPPERS.get().readValue(string, clazz);
    } catch (JsonProcessingException e) {
      return null;
    }
  }
}
