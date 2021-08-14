package com.graphhopper.reader;

import java.util.Map;

/**
 * Allows access to protected method ReaderElement.getTags
 */
public class ReaderElementUtils {

  public static Map<String, Object> getTags(ReaderElement elem) {
    return elem.getTags();
  }
}
