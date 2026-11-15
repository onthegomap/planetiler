package com.onthegomap.planetiler.archive;

import java.util.List;
import java.util.Optional;
import org.maplibre.mlt.ffi.MltException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Which encoder {@link TileEncoder#factory} uses to write MLT tiles
 */
public enum MltEncoderType {
  /** The native (Rust) encoder of {@code mlt-ffi-java}, failing on platforms where it does not work. */
  NATIVE("native"),
  /** The pure-Java encoder of {@code org.maplibre:mlt}, which works on every platform. */
  JAVA("java");

  private static final Logger LOGGER = LoggerFactory.getLogger(MltEncoderType.class);

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

  /**
   * Returns whether to use the native encoder, first running {@code checkNativeEncoder} unless this is {@link #JAVA}.
   *
   * @throws IllegalStateException if this is {@link #NATIVE} and {@code checkNativeEncoder} fails
   */
  boolean useNative(Runnable checkNativeEncoder) {
    if (this == NATIVE) {
      try {
        checkNativeEncoder.run();
      } catch (UnsatisfiedLinkError | IllegalCallerException | MltException e) {
        switch (e) {
          case IllegalCallerException _ -> throw new IllegalStateException("Native MLT encoder needs the JVM option --enable-native-access=ALL-UNNAMED. If you don't want to do this you may use --mlt-encoder=java instead");
          default -> throw new IllegalStateException("Native MLT encoder is not included in the jar on " + System.getProperty("os.name") + " " +
            System.getProperty("os.arch") + " (" + e.getMessage() + "). If you don't want to do this you may use --mlt-encoder=java instead");
        }
      }
    }
    LOGGER.info("Using the {} MLT encoder", this == NATIVE ? "native" : "Java");
    return this == NATIVE;
  }
}
