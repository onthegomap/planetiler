package com.onthegomap.planetiler.archive;

import java.util.List;
import java.util.stream.Stream;

/**
 * Which encoder {@link TileEncoder#factory} uses to write MLT tiles, set by {@code mlt_encoder}. There is no automatic
 * fallback between them because they write different bytes for the same tile.
 */
public enum MltEncoderType {
  /** The native (Rust) encoder of {@code mlt-ffi-java}, failing on platforms where it does not work. */
  NATIVE("native"),
  /** The pure-Java encoder of {@code org.maplibre:mlt}, which works on every platform. */
  JAVA("java");

  private final String id;

  MltEncoderType(String id) {
    this.id = id;
  }

  public static MltEncoderType fromId(String id) {
    return Stream.of(values()).filter(type -> type.id.equals(id)).findFirst()
      .orElseThrow(() -> new IllegalArgumentException("invalid MLT encoder ID " + id + "; expected one of " + ids()));
  }

  public static List<String> ids() {
    return Stream.of(values()).map(MltEncoderType::id).toList();
  }

  public String id() {
    return id;
  }
}
