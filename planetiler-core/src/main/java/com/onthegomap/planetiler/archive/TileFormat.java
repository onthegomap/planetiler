package com.onthegomap.planetiler.archive;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public enum TileFormat {

  @JsonProperty("mvt")
  MVT("mvt"),
  @JsonProperty("mlt")
  MLT("mlt");

  private final String id;

  TileFormat(String id) {
    this.id = id;
  }

  public static TileFormat fromId(String id) {
    return findById(id)
      .orElseThrow(() -> new IllegalArgumentException("invalid format ID; expected one of " +
        Stream.of(TileFormat.values()).map(TileFormat::id).toList()));
  }

  public static Optional<TileFormat> findById(String id) {
    return availableValues()
      .stream()
      .filter(tdc -> tdc.id().equals(id))
      .findFirst();
  }

  public static Set<TileFormat> availableValues() {
    return Set.of(TileFormat.values());
  }

  public String id() {
    return id;
  }

  static class Deserializer extends JsonDeserializer<TileFormat> {
    @Override
    public TileFormat deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      return findById(p.getValueAsString()).orElse(TileFormat.MVT);
    }

    @Override
    public TileFormat getNullValue(DeserializationContext ctxt) {
      return TileFormat.MVT;
    }
  }
}
