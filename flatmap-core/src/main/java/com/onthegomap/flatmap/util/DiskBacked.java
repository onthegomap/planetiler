package com.onthegomap.flatmap.util;

/**
 * A resource backed by a file or directory on disk.
 */
public interface DiskBacked {

  /** Returns the current size of that file or directory in bytes. */
  long diskUsageBytes();
}
