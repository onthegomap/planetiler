package com.onthegomap.planetiler.archive;

import java.util.List;
import java.util.Optional;

/** Which encoder {@link TileEncoders} uses to write MLT tiles, set by {@code mlt_encoder}. */
public enum MltEncoderType {
  /** The native encoder where its library loads, and the Java encoder everywhere else. */
  AUTO("auto"),
  /** The native (Rust) encoder of {@code mlt-ffi-java}, failing on platforms where its library does not load. */
  NATIVE("native"),
  /** The pure-Java encoder of {@code org.maplibre:mlt}, which works on every platform. */
  JAVA("java");

  private final String id;

  MltEncoderType(String id) {
    this.id = id;
  }

  public static MltEncoderType fromId(String id) {
    return findById(id)
      .orElseThrow(() -> new IllegalArgumentException("invalid MLT encoder ID " + id + "; expected one of " +
        availableValues().stream().map(MltEncoderType::id).toList()));
  }

  public static Optional<MltEncoderType> findById(String id) {
    return availableValues().stream().filter(type -> type.id.equals(id)).findFirst();
  }

  public static List<MltEncoderType> availableValues() {
    return List.of(values());
  }

  public String id() {
    return id;
  }
}
