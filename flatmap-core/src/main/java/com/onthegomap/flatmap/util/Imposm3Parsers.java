/* ****************************************************************
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ****************************************************************/
package com.onthegomap.flatmap.util;

import com.carrotsearch.hppc.ObjectIntMap;
import com.graphhopper.coll.GHObjectIntHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Parse utilities ported to Java from <a href="https://github.com/omniscale/imposm3/blob/master/mapping/columns.go">omniscale/imposm3:mapping/columns.go</a>
 */
public class Imposm3Parsers {

  private Imposm3Parsers() {
  }

  private static String string(Object object) {
    return object == null ? null : object.toString();
  }

  private static final ObjectIntMap<String> defaultRank = new GHObjectIntHashMap<>();

  static {
    defaultRank.put("minor", 3);
    defaultRank.put("road", 3);
    defaultRank.put("unclassified", 3);
    defaultRank.put("residential", 3);
    defaultRank.put("tertiary_link", 3);
    defaultRank.put("tertiary", 4);
    defaultRank.put("secondary_link", 3);
    defaultRank.put("secondary", 5);
    defaultRank.put("primary_link", 3);
    defaultRank.put("primary", 6);
    defaultRank.put("trunk_link", 3);
    defaultRank.put("trunk", 8);
    defaultRank.put("motorway_link", 3);
    defaultRank.put("motorway", 9);
  }

  /**
   * Returns a z-order for an OSM road based on the tags that are present. Bridges are above roads appear above tunnels
   * and major roads appear above minor.
   */
  public static int wayzorder(Map<String, Object> tags) {
    long z = Parse.parseLong(tags.get("layer")) * 10 +
      defaultRank.getOrDefault(
        string(tags.get("highway")),
        tags.containsKey("railway") ? 7 : 0
      ) +
      (boolInt(tags.get("tunnel")) * -10L) +
      (boolInt(tags.get("bridge")) * 10L);
    return Math.abs(z) < 10_000 ? (int) z : 0;
  }

  private static final Set<String> forwardDirections = Set.of("1", "yes", "true");

  /**
   * Returns the direction value for an input string -1 is reverse, 1 is forward ("1" "yes" or "true"), and 0 is other.
   *
   * @see <a href="https://wiki.openstreetmap.org/wiki/Key:oneway">OSM one-way</a>
   */
  public static int direction(Object string) {
    if (string == null) {
      return 0;
    } else if (forwardDirections.contains(string(string))) {
      return 1;
    } else if ("-1".equals(string)) {
      return -1;
    } else {
      return 0;
    }
  }

  private static final Set<String> booleanFalseValues = Set.of("", "0", "false", "no");

  /** Returns {@code false} if {@code tag} is empty, "0", "false", or "no" and {@code true} otherwise. */
  public static boolean bool(Object tag) {
    return !(tag == null || booleanFalseValues.contains(tag.toString()));
  }

  /** Returns 1 if {@link #bool(Object)} is {@code false}, 0 otherwise. */
  public static int boolInt(Object tag) {
    return bool(tag) ? 1 : 0;
  }

}
