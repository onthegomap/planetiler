package com.onthegomap.planetiler.collection;

import java.nio.file.Path;

/**
 * Storage method to use for {@link LongLongMap} and {@link AppendStore} implementations.
 */
public enum Storage {
  /** Primitive {@code int[]} or {@code long[]} arrays stored on the JVM heap. */
  RAM("ram"),
  /** Memory-mapped files stored on disk. */
  MMAP("mmap"),
  /** Off-heap native byte buffers stored in-memory but outside the JVM heap. */
  DIRECT("direct");

  private final String id;

  Storage(String id) {
    this.id = id;
  }

  public String id() {
    return id;
  }

  /**
   * Returns the storage type associated with {@code id} or throws {@link IllegalArgumentException} if no match is
   * found.
   */
  public static Storage from(String id) {
    for (Storage value : values()) {
      if (value.id.equalsIgnoreCase(id.trim())) {
        return value;
      }
    }
    throw new IllegalArgumentException("Unexpected storage type: " + id);
  }

  public record Params(Path path, boolean madvise) {

    public Params resolve(String suffix) {
      return new Params(path.resolve(suffix), madvise);
    }
  }
}
