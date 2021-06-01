package com.onthegomap.flatmap;

import static com.onthegomap.flatmap.TestUtils.newLineString;
import static com.onthegomap.flatmap.TestUtils.newMultiLineString;
import static com.onthegomap.flatmap.TestUtils.newPoint;
import static com.onthegomap.flatmap.TestUtils.rectangle;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.flatmap.geo.GeometryException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;

public class FeatureMergeTest {

  private VectorTileEncoder.Feature feature(long id, Geometry geom, Map<String, Object> attrs) {
    return new VectorTileEncoder.Feature(
      "layer",
      id,
      VectorTileEncoder.encodeGeometry(geom),
      attrs
    );
  }

  @Test
  public void mergeMergeZeroLineStrings() throws GeometryException {
    assertEquals(
      List.of(),
      FeatureMerge.mergeLineStrings(
        List.of(),
        0,
        0,
        0
      )
    );
  }

  @Test
  public void mergeMergeOneLineStrings() throws GeometryException {
    assertEquals(
      List.of(
        feature(1, newLineString(10, 10, 20, 20), Map.of())
      ),
      FeatureMerge.mergeLineStrings(
        List.of(
          feature(1, newLineString(10, 10, 20, 20), Map.of())
        ),
        0,
        0,
        0
      )
    );
  }

  @Test
  public void dontMergeDisconnectedLineStrings() throws GeometryException {
    assertEquals(
      List.of(
        feature(1, newMultiLineString(
          newLineString(10, 10, 20, 20),
          newLineString(30, 30, 40, 40)
        ), Map.of())
      ),
      FeatureMerge.mergeLineStrings(
        List.of(
          feature(1, newLineString(10, 10, 20, 20), Map.of()),
          feature(2, newLineString(30, 30, 40, 40), Map.of())
        ),
        0,
        0,
        0
      )
    );
  }

  @Test
  public void dontMergeConnectedLineStringsDifferentAttr() throws GeometryException {
    assertEquals(
      List.of(
        feature(1, newLineString(10, 10, 20, 20), Map.of("a", 1)),
        feature(2, newLineString(20, 20, 30, 30), Map.of("b", 2))
      ),
      FeatureMerge.mergeLineStrings(
        List.of(
          feature(1, newLineString(10, 10, 20, 20), Map.of("a", 1)),
          feature(2, newLineString(20, 20, 30, 30), Map.of("b", 2))
        ),
        0,
        0,
        0
      )
    );
  }

  @Test
  public void mergeConnectedLineStringsSameAttrs() throws GeometryException {
    assertEquals(
      List.of(
        feature(1, newLineString(10, 10, 30, 30), Map.of("a", 1))
      ),
      FeatureMerge.mergeLineStrings(
        List.of(
          feature(1, newLineString(10, 10, 20, 20), Map.of("a", 1)),
          feature(2, newLineString(20, 20, 30, 30), Map.of("a", 1))
        ),
        0,
        0,
        0
      )
    );
  }

  @Test
  public void mergeMultiLineString() throws GeometryException {
    assertEquals(
      List.of(
        feature(1, newLineString(10, 10, 40, 40), Map.of("a", 1))
      ),
      FeatureMerge.mergeLineStrings(
        List.of(
          feature(1, newMultiLineString(
            newLineString(10, 10, 20, 20),
            newLineString(30, 30, 40, 40)
          ), Map.of("a", 1)),
          feature(2, newLineString(20, 20, 30, 30), Map.of("a", 1))
        ),
        0,
        0,
        0
      )
    );
  }

  @Test
  public void mergeLineStringIgnoreNonLineString() throws GeometryException {
    assertEquals(
      List.of(
        feature(3, newPoint(5, 5), Map.of("a", 1)),
        feature(4, rectangle(50, 60), Map.of("a", 1)),
        feature(1, newLineString(10, 10, 30, 30), Map.of("a", 1))
      ),
      FeatureMerge.mergeLineStrings(
        List.of(
          feature(1, newLineString(10, 10, 20, 20), Map.of("a", 1)),
          feature(2, newLineString(20, 20, 30, 30), Map.of("a", 1)),
          feature(3, newPoint(5, 5), Map.of("a", 1)),
          feature(4, rectangle(50, 60), Map.of("a", 1))
        ),
        0,
        0,
        0
      )
    );
  }

  @Test
  public void mergeLineStringRemoveDetailOutsideTile() throws GeometryException {
    assertEquals(
      List.of(
        feature(1, newMultiLineString(
          newLineString(
            10, 10,
            -10, 20,
            10, 30,
            -10, 40,
            -10, 50,
            10, 60,
            -10, 70
          ),
          newLineString(
            -10, 100,
            10, 100
          )
        ), Map.of("a", 1))
      ),
      FeatureMerge.mergeLineStrings(
        List.of(
          // one point goes out - dont clip
          feature(1, newLineString(10, 10, -10, 20), Map.of("a", 1)),
          feature(2, newLineString(-10, 20, 10, 30), Map.of("a", 1)),
          feature(3, newLineString(10, 30, -10, 40), Map.of("a", 1)),
          // two points goes out - dont clip
          feature(4, newLineString(-10, 40, -10, 50), Map.of("a", 1)),
          feature(5, newLineString(-10, 50, 10, 60), Map.of("a", 1)),
          feature(5, newLineString(10, 60, -10, 70), Map.of("a", 1)),
          // three points out - do clip
          feature(6, newLineString(-10, 70, -10, 80), Map.of("a", 1)),
          feature(7, newLineString(-10, 80, -11, 90), Map.of("a", 1)),
          feature(8, newLineString(-10, 90, -10, 100), Map.of("a", 1)),
          feature(9, newLineString(-10, 100, 10, 100), Map.of("a", 1))
        ),
        0,
        0,
        1
      )
    );
  }

  @Test
  public void mergeLineStringMinLength() throws GeometryException {
    assertEquals(
      List.of(
        feature(2, newLineString(20, 20, 20, 25), Map.of("a", 1))
      ),
      FeatureMerge.mergeLineStrings(
        List.of(
          // too short - omit entire feature
          feature(1, newLineString(10, 10, 10, 14), Map.of("b", 1)),

          // too short - omit from combined group
          feature(2, newLineString(20, 10, 20, 12), Map.of("a", 1)),
          feature(3, newLineString(20, 12, 20, 14), Map.of("a", 1)),

          // just long enough
          feature(4, newLineString(20, 20, 20, 24), Map.of("a", 1)),
          feature(5, newLineString(20, 24, 20, 25), Map.of("a", 1))
        ),
        5,
        0,
        0
      )
    );
  }
}
