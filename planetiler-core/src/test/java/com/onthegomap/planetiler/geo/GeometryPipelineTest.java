package com.onthegomap.planetiler.geo;

import static com.onthegomap.planetiler.geo.GeometryPipeline.compose;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.util.ZoomFunction;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.util.AffineTransformation;

class GeometryPipelineTest {
  GeometryPipeline move(int i) {
    return AffineTransformation.translationInstance(i, i)::transform;
  }

  private final ZoomFunction<GeometryPipeline> moveByZoom = this::move;

  Point point(int i) {
    return GeoUtils.JTS_FACTORY.createPoint(new CoordinateXY(i, i));
  }

  @Test
  void testSingle() {
    assertEquals(point(2), move(1).apply(point(1)));
    assertEquals(point(2), move(1).apply(3).apply(point(1)));
  }

  @Test
  void testSingleByZoom() {
    assertEquals(point(4), moveByZoom.apply(3).apply(point(1)));
  }

  @Test
  void testCompose2() {
    // pipeline, pipeline
    assertEquals(point(4), compose(move(1), move(2)).apply(point(1)));
    assertEquals(point(4), compose(move(1), move(2)).apply(2).apply(point(1)));

    // pipeline, ZoomFunction<pipeline>
    assertEquals(point(5), compose(move(1), moveByZoom).apply(3).apply(point(1)));

    // ZoomFunction<pipeline>, pipeline
    assertEquals(point(5), compose(moveByZoom, move(1)).apply(3).apply(point(1)));

    // ZoomFunction<pipeline>, ZoomFunction<pipeline>
    assertEquals(point(7), compose(moveByZoom, moveByZoom).apply(3).apply(point(1)));
  }

  @Test
  void testComposeMany() {
    assertEquals(point(8), compose(move(1), moveByZoom, moveByZoom).apply(3).apply(point(1)));
    assertEquals(point(9), compose(move(1), moveByZoom, moveByZoom, move(1)).apply(3).apply(point(1)));
    assertEquals(point(12), compose(move(1), moveByZoom, moveByZoom, move(1), moveByZoom).apply(3).apply(point(1)));
  }
}
