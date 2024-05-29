package com.onthegomap.planetiler.util;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A utility for merging maps with tags that only apply to a specific range, by default from {@code [0, 1)}.
 * <p>
 * For example:
 * {@snippet :
 * RangeMapMap map = new RangeMapMap();
 * map.put(0, 0.5, "key", "value");
 * // overrides value for key from [0.4, 0.5), and sets from [0.5, 1)
 * map.put(0.4, 1, "key", "value2");
 * // adds key2 from [0.9, 1)
 * map.put(0.9, 1, "key2", "value3");
 * // returns [
 * //   Partial[start=0.0, end=0.4, value={key=value}],
 * //   Partial[start=0.4, end=0.9, value={key=value2}],
 * //   Partial[start=0.9, end=1.0, value={key2=value3, key=value2}]
 * // ]
 * map.result();
 * }
 */
public class RangeMapMap {

  private final RangeMap<Double, Map<Object, Object>> items = TreeRangeMap.create();

  public RangeMapMap() {
    this(Map.of());
  }

  public RangeMapMap(Map<Object, Object> identity) {
    this(0, 1, identity);
  }

  public RangeMapMap(double lo, double hi, Map<Object, Object> identity) {
    items.put(Range.closedOpen(lo, hi), identity);
  }

  /** Returns the distinct set of ranges where adjacent maps with identical tags are merged. */
  public List<Partial> result() {
    List<Partial> result = new ArrayList<>();
    for (var entry : items.asMapOfRanges().entrySet()) {
      result.add(new Partial(entry.getKey().lowerEndpoint(), entry.getKey().upperEndpoint(), entry.getValue()));
    }
    return result;
  }

  /** Removes {@code key} from maps within {@code range}. */
  public void remove(Range<Double> range, Object key) {
    var overlaps = new ArrayList<>(items.subRangeMap(range).asMapOfRanges().entrySet());
    for (var overlap : overlaps) {
      Map<Object, Object> merged = new HashMap<>(overlap.getValue());
      merged.remove(key);
      items.putCoalescing(overlap.getKey(), merged);
    }
  }

  /** Subset of the range and attributes that apply to it. */
  public record Partial(double start, double end, Map<Object, Object> value) {}

  public void put(double start, double end, Object key, Object value) {
    put(start, end, Map.of(key, value));
  }

  public void put(double start, double end, Map<Object, Object> add) {
    put(Range.closedOpen(start, end), add);
  }

  public void put(Range<Double> range, Map<Object, Object> add) {
    var overlaps = new ArrayList<>(items.subRangeMap(range).asMapOfRanges().entrySet());
    items.putCoalescing(range, add);
    for (var overlap : overlaps) {
      Map<Object, Object> merged = new HashMap<>(overlap.getValue());
      merged.putAll(add);
      items.putCoalescing(overlap.getKey(), merged);
    }
  }
}
