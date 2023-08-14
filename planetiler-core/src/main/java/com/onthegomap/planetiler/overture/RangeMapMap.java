package com.onthegomap.planetiler.overture;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RangeMapMap {

  private final RangeMap<Double, Map<String, Object>> items = TreeRangeMap.create();

  {
    items.put(Range.closedOpen(0d, 1d), Map.of());
  }

  public List<Partial> result() {
    List<Partial> result = new ArrayList<>();
    for (var entry : items.asMapOfRanges().entrySet()) {
      result.add(new Partial(entry.getKey().lowerEndpoint(), entry.getKey().upperEndpoint(), entry.getValue()));
    }
    return result;
  }

  public record Partial(double start, double end, Map<String, Object> value) {}

  public void put(double start, double end, Map<String, Object> add) {
    put(Range.closedOpen(start, end), add);
  }

  public void put(Range<Double> range, Map<String, Object> add) {
    var overlaps = new ArrayList<>(items.subRangeMap(range).asMapOfRanges().entrySet());
    items.put(range, add);
    for (var overlap : overlaps) {
      Map<String, Object> merged = new HashMap<>(overlap.getValue());
      for (var e : add.entrySet()) {
        merged.merge(e.getKey(), e.getValue(), (a, b) -> {
          if (a instanceof Collection<?> aList) {
            List<Object> result = new ArrayList<>(aList);
            if (b instanceof Collection<?> bList) {
              result.addAll(bList);
            } else {
              result.add(b);
            }
            return result;
          } else if (b instanceof Collection<?> bList) {
            List<Object> result = new ArrayList<>(bList);
            result.add(a);
            return result;
          } else {
            return b;
          }
        });
      }
      items.put(overlap.getKey(), merged);
    }

  }
}
