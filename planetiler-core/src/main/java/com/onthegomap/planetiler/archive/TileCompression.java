package com.onthegomap.planetiler.archive;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@JsonDeserialize(using = TileCompression.Deserializer.class)
public enum TileCompression {

  @JsonProperty("none")
  NONE("none"),
  @JsonProperty("gzip")
  GZIP("gzip"),
  @JsonProperty("unknown")
  UNKNWON("unknown");

  private final String id;

  TileCompression(String id) {
    this.id = id;
  }

  public static TileCompression fromId(String id) {
    return findById(id)
      .orElseThrow(() -> new IllegalArgumentException("invalid compression ID; expected one of " +
        Stream.of(TileCompression.values()).map(TileCompression::id).toList()));
  }

  public static Optional<TileCompression> findById(String id) {
    return availableValues()
      .stream()
      .filter(tdc -> tdc.id().equals(id))
      .findFirst();
  }

  public static Set<TileCompression> availableValues() {
    return Arrays.stream(TileCompression.values()).filter(tc -> tc != UNKNWON).collect(Collectors.toUnmodifiableSet());
  }

  public String id() {
    return id;
  }

  static class Deserializer extends JsonDeserializer<TileCompression> {
    @Override
    public TileCompression deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      return findById(p.getValueAsString()).orElse(TileCompression.UNKNWON);
    }

    @Override
    public TileCompression getNullValue(DeserializationContext ctxt) {
      return TileCompression.GZIP;
    }
  }
}
