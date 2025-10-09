package com.onthegomap.planetiler.archive;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@JsonDeserialize(using = TileFormat.Deserializer.class)
public enum TileFormat {

  @JsonProperty("pbf")
  MVT("mvt", "pbf"),
  @JsonProperty("application/vnd.maplibre-vector-tile")
  MLT("mlt", "application/vnd.maplibre-vector-tile"),
  @JsonProperty("unknown")
  UNKNOWN("unknown", null);

  private final String id;
  private final String mbtiles;

  TileFormat(String id, String mbtiles) {
    this.id = id;
    this.mbtiles = mbtiles;
  }

  public static TileFormat fromId(String id) {
    return findById(id)
      .orElseThrow(() -> new IllegalArgumentException("invalid format ID; expected one of " +
        Stream.of(TileFormat.values()).map(TileFormat::id).toList()));
  }

  public static Optional<TileFormat> findById(String id) {
    return availableValues()
      .stream()
      .filter(tdc -> tdc.id().equals(id) || Objects.equals(tdc.mbtiles, id))
      .findFirst();
  }

  public static Set<TileFormat> availableValues() {
    return Arrays.stream(TileFormat.values()).filter(tc -> tc != UNKNOWN).collect(Collectors.toUnmodifiableSet());
  }

  public String id() {
    return id;
  }

  public String mbtilesValue() {
    return mbtiles;
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
