package com.onthegomap.planetiler.overture;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.util.ZoomFunction;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class OvertureTest {
  @Test
  void testFlags() {
    test(
      Map.of(
        "flags", "isBridge"
      ),
      partial(Map.of(
        "flags.isBridge", 1
      ))
    );
    test(
      Map.of(
        "flags", List.of("isBridge")
      ),
      partial(Map.of(
        "flags.isBridge", 1
      ))
    );
    test(
      Map.of(
        "flags", Map.of("value", "isBridge", "at", List.of(0d, 1d))
      ),
      partial(Map.of(
        "flags.isBridge", 1
      ))
    );
    test(
      Map.of(
        "flags", Map.of("value", "isBridge")
      ),
      partial(Map.of(
        "flags.isBridge", 1
      ))
    );
    test(
      Map.of(
        "flags", Map.of("values", "isBridge")
      ),
      partial(Map.of(
        "flags.isBridge", 1
      ))
    );
    test(
      Map.of(
        "flags", List.of(Map.of("value", List.of("isBridge"), "at", List.of(0d, 1d)))
      ),
      partial(Map.of(
        "flags.isBridge", 1
      ))
    );
    test(
      Map.of(
        "flags", List.of(Map.of("values", List.of("isBridge"), "at", List.of(0d, 1d)))
      ),
      partial(Map.of(
        "flags.isBridge", 1
      ))
    );
    test(
      Map.of(
        "flags", List.of(Map.of("values", "isBridge", "at", List.of(0d, 1d)))
      ),
      partial(Map.of(
        "flags.isBridge", 1
      ))
    );
  }

  @Test
  void testMultiFlags() {
    test(
      Map.of(
        "flags", List.of(
          Map.of("value", "isBridge", "at", List.of(0, 0.75)),
          Map.of("values", "isLink", "at", List.of(0.25, 1))
        )
      ),
      partial(0, 0.25, Map.of(
        "flags.isBridge", 1
      )),
      partial(0.25, 0.75, Map.of(
        "flags.isBridge", 1,
        "flags.isLink", 1
      )),
      partial(0.75, 1d, Map.of(
        "flags.isLink", 1
      ))
    );
  }

  @Test
  void testSurface() {
    test(
      Map.of(
        "surface", List.of(Map.of("values", "paved", "at", List.of(0d, 1d)))
      ),
      partial(Map.of(
        "surface", "paved"
      ))
    );
  }

  @Test
  void testNames() {
    test(
      Map.of(
        "roadNames", List.of(Map.of(
          "common", Map.of(
            "value", "Main Street",
            "language", "local"
          ),
          "at", List.of(0.5d, 1d)))
      ),
      partial(0, 0.5, Map.of()),
      partial(0.5, 1d, Map.of(
        "name", "Main Street",
        "name.common.local", "Main Street"
      ))
    );
    test(
      Map.of(
        "roadNames", List.of(Map.of(
          "common", Map.of(
            "value", "Main Street",
            "language", "local"
          )
        ))
      ),
      partial(Map.of(
        "name", "Main Street",
        "name.common.local", "Main Street"
      ))
    );
  }

  @Test
  void testMaxSpeed() {
    test(
      Map.of(
        "restrictions", Map.of(
          "speedLimits", List.of(Map.of(
            "maxSpeed", List.of(50, "km/h"),
            "minSpeed", List.of(30, "mph"),
            "isMaxSpeedVariable", true
          ))
        )
      ),
      partial(Map.of(
        "restrictions.speedLimits.maxSpeed", "50km/h",
        "restrictions.speedLimits.minSpeed", "30mph",
        "restrictions.speedLimits.isMaxSpeedVariable", 1
      ))
    );
  }

  @Test
  void testMaxSpeedPartial() {
    test(
      Map.of(
        "restrictions", Map.of(
          "speedLimits", List.of(Map.of(
            "maxSpeed", List.of(50, "km/h"),
            "minSpeed", List.of(30, "mph"),
            "isMaxSpeedVariable", true,
            "at", List.of(0, 0.75)
          ), Map.of(
            "maxSpeed", List.of(10, "km/h"),
            "at", List.of(0.25, 1)
          ))
        )
      ),
      partial(0, 0.25, Map.of(
        "restrictions.speedLimits.maxSpeed", "50km/h",
        "restrictions.speedLimits.minSpeed", "30mph",
        "restrictions.speedLimits.isMaxSpeedVariable", 1
      )),
      partial(0.25, 0.75, Map.of(
        "restrictions.speedLimits.maxSpeed", "10km/h",
        "restrictions.speedLimits.minSpeed", "30mph",
        "restrictions.speedLimits.isMaxSpeedVariable", 1
      )),
      partial(0.75, 1, Map.of(
        "restrictions.speedLimits.maxSpeed", "10km/h"
      ))
    );
  }

  @Test
  void testAccessRestriction() {
    test(
      Map.of(
        "restrictions", Map.of(
          "access", List.of("allowed")
        )
      ),
      partial(Map.of(
        "restrictions.access.allowed", 1
      ))
    );
    test(
      Map.of(
        "restrictions", Map.of("access",
          List.of(Map.of(
            "denied", Map.of(
              "at", List.of(0, 0.5)
            )
          ))
        )
      ),
      partial(0, 0.5, Map.of(
        "restrictions.access.denied", 1
      )),
      partial(0.5, 1, Map.of())
    );
    test(
      Map.of(
        "restrictions", Map.of("access",
          List.of(
            Map.of(
              "denied", Map.of(
                "at", List.of(0, 0.5)
              )
            ),
            Map.of(
              "denied", Map.of(
                "at", List.of(0.5, 1),
                "when", Map.of("mode", List.of("foot"))
              )
            )
          )
        )
      ),
      partial(0, 0.5, Map.of(
        "restrictions.access.denied", 1
      )),
      partial(0.5, 1, Map.of(
        "restrictions.access.denied", """
          {"when":{"mode":["foot"]}}
          """.trim()
      ))
    );
  }

  @Test
  void testLanes() {
    test(
      Map.of(
        "lanes", List.of(
          Map.of("test", 1),
          Map.of("test", 2)
        )
      ),
      partial(Map.of(
        "lanes", """
          [{"test":1},{"test":2}]
          """.trim()
      ))
    );
    test(
      Map.of(
        "lanes", List.of(
          Map.of(
            "at", List.of(0, 0.5),
            "value", List.of(
              Map.of("test", 1),
              Map.of("test", 2)
            )
          )
        )
      ),
      partial(0, 0.5, Map.of(
        "lanes", """
          [{"test":1},{"test":2}]
          """.trim()
      )),
      partial(0.5, 1, Map.of())
    );
  }

  private void test(Map<String, Object> flags, RangeMapMap.Partial... partials) {
    assertEquals(List.of(partials), Overture.parseRoadPartials(Struct.of(flags)).result()
      .stream()
      .map(d -> new RangeMapMap.Partial(d.start(), d.end(), d.value().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey,
          e -> e.getValue()instanceof ZoomFunction<?> zf ? zf.apply(14) : e.getValue()))))
      .toList());
  }

  private RangeMapMap.Partial partial(Map<String, Object> map) {
    return partial(0, 1, map);
  }

  private RangeMapMap.Partial partial(double start, double end, Map<String, Object> map) {
    return new RangeMapMap.Partial(start, end, map);
  }
}
