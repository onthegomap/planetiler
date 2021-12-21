package com.graphhopper.reader;

import java.util.Map;

/**
 * Utility to gain access to protected method {@link ReaderElement#getTags()}
 */
public class ReaderElementUtils {

  public static Map<String, Object> getTags(ReaderElement elem) {
    return elem.getTags();
  }
}
