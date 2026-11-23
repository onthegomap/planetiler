package com.onthegomap.planetiler.util;

import java.util.Collection;

/** Finds similarity between strings using Levenshtein distance. */
public final class EditDistance {

  private EditDistance() {}

  /**
   * Returns the entry in {@code candidates} closest to {@code input} by edit distance, or {@code null} if none is close
   * enough to be a likely typo.
   */
  public static String closest(String input, Collection<String> candidates) {
    // allow about one typo per 4 characters, so short words don't match everything
    int maxDistance = Math.max(1, input.length() / 4);
    String best = null;
    int bestDistance = Integer.MAX_VALUE;
    for (String candidate : candidates) {
      int distance = editDistance(input, candidate);
      if (distance <= maxDistance && distance < bestDistance) {
        best = candidate;
        bestDistance = distance;
      }
    }
    return best;
  }

  /** Returns the Levenshtein distance between {@code a} and {@code b}: insertions, deletions and substitutions. */
  public static int editDistance(String a, String b) {
    int[] previous = new int[b.length() + 1];
    int[] current = new int[b.length() + 1];
    for (int j = 0; j <= b.length(); j++) {
      previous[j] = j;
    }
    for (int i = 1; i <= a.length(); i++) {
      current[0] = i;
      for (int j = 1; j <= b.length(); j++) {
        int substitution = previous[j - 1] + (a.charAt(i - 1) == b.charAt(j - 1) ? 0 : 1);
        current[j] = Math.min(substitution, Math.min(previous[j], current[j - 1]) + 1);
      }
      int[] swap = previous;
      previous = current;
      current = swap;
    }
    return previous[b.length()];
  }
}
