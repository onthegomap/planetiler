package com.onthegomap.planetiler.reader.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.geo.GeoUtils;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;

class GeoArrowTest {
  @Test
  void testPointXY() throws ParseException {
    assertSame(
      "POINT(1 2)",
      GeoArrow.point(Map.of("x", 1, "y", 2)),
      GeoArrow.point(List.of(1, 2))
    );
  }

  @Test
  void testPointXYZ() throws ParseException {
    assertSame(
      "POINT Z(1 2 3)",
      GeoArrow.point(Map.of("x", 1, "y", 2, "z", 3)),
      GeoArrow.point(List.of(1, 2, 3))
    );
  }

  @Test
  void testPointXYZM() throws ParseException {
    assertSame(
      "POINT ZM(1 2 3 4)",
      GeoArrow.point(Map.of("x", 1, "y", 2, "z", 3, "m", 4)),
      GeoArrow.point(List.of(1, 2, 3, 4))
    );
  }

  @Test
  void testLine() throws ParseException {
    assertSame(
      "LINESTRING(1 2, 3 4)",
      GeoArrow.linestring(List.of(
        Map.of("x", 1, "y", 2),
        Map.of("x", 3, "y", 4)
      )),
      GeoArrow.linestring(List.of(
        List.of(1, 2),
        List.of(3, 4)
      ))
    );
  }

  @Test
  void testLineZ() throws ParseException {
    assertSame(
      "LINESTRING Z(1 2 3, 4 5 6)",
      GeoArrow.linestring(List.of(
        Map.of("x", 1, "y", 2, "z", 3),
        Map.of("x", 4, "y", 5, "z", 6)
      )),
      GeoArrow.linestring(List.of(
        List.of(1, 2, 3),
        List.of(4, 5, 6)
      ))
    );
  }

  @Test
  void testLineZM() throws ParseException {
    assertSame(
      "LINESTRING ZM(1 2 3 4, 5 6 7 8)",
      GeoArrow.linestring(List.of(
        Map.of("x", 1, "y", 2, "z", 3, "m", 4),
        Map.of("x", 5, "y", 6, "z", 7, "m", 8)
      )),
      GeoArrow.linestring(List.of(
        List.of(1, 2, 3, 4),
        List.of(5, 6, 7, 8)
      ))
    );
  }

  @Test
  void testPolygon() throws ParseException {
    assertSame(
      "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))",
      GeoArrow.polygon(List.of(
        List.of(
          Map.of("x", 0, "y", 0),
          Map.of("x", 0, "y", 1),
          Map.of("x", 1, "y", 1),
          Map.of("x", 1, "y", 0),
          Map.of("x", 0, "y", 0)
        ))),
      GeoArrow.polygon(List.of(
        List.of(
          List.of(0, 0),
          List.of(0, 1),
          List.of(1, 1),
          List.of(1, 0),
          List.of(0, 0)
        )
      ))
    );
  }

  @Test
  void testPolygonWithHole() throws ParseException {
    assertSame(
      "POLYGON((-2 -2, 2 -2, 0 2, -2 -2), (-1 -1, 1 -1, 0 1, -1 -1))",
      GeoArrow.polygon(List.of(
        List.of(
          Map.of("x", -2, "y", -2),
          Map.of("x", 2, "y", -2),
          Map.of("x", 0, "y", 2),
          Map.of("x", -2, "y", -2)
        ),
        List.of(
          Map.of("x", -1, "y", -1),
          Map.of("x", 1, "y", -1),
          Map.of("x", 0, "y", 1),
          Map.of("x", -1, "y", -1)
        )
      )),
      GeoArrow.polygon(List.of(
        List.of(
          List.of(-2, -2),
          List.of(2, -2),
          List.of(0, 2),
          List.of(-2, -2)
        ),
        List.of(
          List.of(-1, -1),
          List.of(1, -1),
          List.of(0, 1),
          List.of(-1, -1)
        )
      ))
    );
  }

  @Test
  void testMultipoint() throws ParseException {
    assertSame(
      "MULTIPOINT(1 2, 3 4)",
      GeoArrow.multipoint(List.of(
        Map.of("x", 1, "y", 2),
        Map.of("x", 3, "y", 4)
      )),
      GeoArrow.multipoint(List.of(
        List.of(1, 2),
        List.of(3, 4)
      ))
    );
  }

  @Test
  void testMultilinestring() throws ParseException {
    assertSame(
      "MULTILINESTRING((1 2, 3 4), (5 6, 7 8))",
      GeoArrow.multilinestring(List.of(
        List.of(
          Map.of("x", 1, "y", 2),
          Map.of("x", 3, "y", 4)
        ),
        List.of(
          Map.of("x", 5, "y", 6),
          Map.of("x", 7, "y", 8)
        )
      )),
      GeoArrow.multilinestring(List.of(
        List.of(
          List.of(1, 2),
          List.of(3, 4)
        ),
        List.of(
          List.of(5, 6),
          List.of(7, 8)
        )
      ))
    );
  }

  @Test
  void testMultipolygon() throws ParseException {
    assertSame(
      "MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)), ((2 0, 3 0, 3 1, 2 1, 2 0)))",
      GeoArrow.multipolygon(List.of(
        List.of(List.of(
          Map.of("x", 0, "y", 0),
          Map.of("x", 1, "y", 0),
          Map.of("x", 1, "y", 1),
          Map.of("x", 0, "y", 1),
          Map.of("x", 0, "y", 0)
        )),
        List.of(List.of(
          Map.of("x", 2, "y", 0),
          Map.of("x", 3, "y", 0),
          Map.of("x", 3, "y", 1),
          Map.of("x", 2, "y", 1),
          Map.of("x", 2, "y", 0)
        ))
      )),
      GeoArrow.multipolygon(List.of(
        List.of(List.of(
          List.of(0, 0),
          List.of(1, 0),
          List.of(1, 1),
          List.of(0, 1),
          List.of(0, 0)
        )),
        List.of(List.of(
          List.of(2, 0),
          List.of(3, 0),
          List.of(3, 1),
          List.of(2, 1),
          List.of(2, 0)
        ))
      ))
    );
  }

  private static void assertSame(String wkt, Geometry... geometry) throws ParseException {
    Geometry expected = GeoUtils.wktReader().read(wkt);
    for (int i = 0; i < geometry.length; i++) {
      assertEquals(expected, geometry[i], "geometry #" + i);
    }
  }
}
