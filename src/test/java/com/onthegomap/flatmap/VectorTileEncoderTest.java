package com.onthegomap.flatmap;

import static com.onthegomap.flatmap.geo.GeoUtils.gf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.flatmap.VectorTileEncoder.DecodedFeature;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import vector_tile.VectorTile.Tile.GeomType;

public class VectorTileEncoderTest {

  @Test
  public void testRoundTripPoint() throws IOException {
    testRoundTripGeometry(gf.createPoint(new CoordinateXY(1, 2)));
  }

  @Test
  public void testRoundTripMultipoint() throws IOException {
    testRoundTripGeometry(gf.createMultiPointFromCoords(new Coordinate[]{
      new CoordinateXY(1, 2),
      new CoordinateXY(3, 4)
    }));
  }

  @Test
  public void testRoundTripLineString() throws IOException {
    testRoundTripGeometry(gf.createLineString(new Coordinate[]{
      new CoordinateXY(1, 2),
      new CoordinateXY(3, 4)
    }));
  }

  @Test
  public void testRoundTripPolygon() throws IOException {
    testRoundTripGeometry(gf.createPolygon(
      gf.createLinearRing(new Coordinate[]{
        new CoordinateXY(0, 0),
        new CoordinateXY(4, 0),
        new CoordinateXY(4, 4),
        new CoordinateXY(0, 4),
        new CoordinateXY(0, 0)
      }),
      new LinearRing[]{
        gf.createLinearRing(new Coordinate[]{
          new CoordinateXY(1, 1),
          new CoordinateXY(1, 2),
          new CoordinateXY(2, 2),
          new CoordinateXY(2, 1),
          new CoordinateXY(1, 1)
        })
      }
    ));
  }

  @Test
  public void testRoundTripMultiPolygon() throws IOException {
    testRoundTripGeometry(gf.createMultiPolygon(new Polygon[]{
      gf.createPolygon(new Coordinate[]{
        new CoordinateXY(0, 0),
        new CoordinateXY(1, 0),
        new CoordinateXY(1, 1),
        new CoordinateXY(0, 1),
        new CoordinateXY(0, 0)
      }),
      gf.createPolygon(new Coordinate[]{
        new CoordinateXY(3, 0),
        new CoordinateXY(4, 0),
        new CoordinateXY(4, 1),
        new CoordinateXY(3, 1),
        new CoordinateXY(3, 0)
      })
    }));
  }

  @Test
  public void testRoundTripAttributes() throws IOException {
    testRoundTripAttrs(Map.of(
      "string", "string",
      "long", 1L,
      "double", 3.5d,
      "true", true,
      "false", false
    ));
  }

  @Test
  public void testMultipleFeaturesMultipleLayer() throws IOException {
    Point point = gf.createPoint(new CoordinateXY(0, 0));
    Map<String, Object> attrs1 = Map.of("a", 1L, "b", 2L);
    Map<String, Object> attrs2 = Map.of("b", 3L, "c", 2L);
    byte[] encoded = new VectorTileEncoder().addLayerFeatures("layer1", List.of(
      new LayerFeature(false, 0, 0, attrs1,
        (byte) GeomType.POINT.getNumber(), VectorTileEncoder.getCommands(point), 1L),
      new LayerFeature(false, 0, 0, attrs2,
        (byte) GeomType.POINT.getNumber(), VectorTileEncoder.getCommands(point), 2L)
    )).addLayerFeatures("layer2", List.of(
      new LayerFeature(false, 0, 0, attrs1,
        (byte) GeomType.POINT.getNumber(), VectorTileEncoder.getCommands(point), 3L)
    )).encode();

    List<DecodedFeature> decoded = VectorTileEncoder.decode(encoded);
    assertEquals(attrs1, decoded.get(0).attributes());
    assertEquals("layer1", decoded.get(0).layerName());

    assertEquals(attrs2, decoded.get(1).attributes());
    assertEquals("layer1", decoded.get(1).layerName());

    assertEquals(attrs1, decoded.get(2).attributes());
    assertEquals("layer2", decoded.get(2).layerName());
  }

  private void testRoundTripAttrs(Map<String, Object> attrs) throws IOException {
    testRoundTrip(gf.createPoint(new CoordinateXY(0, 0)), "layer", attrs, 1);
  }

  private void testRoundTripGeometry(Geometry input) throws IOException {
    testRoundTrip(input, "layer", Map.of(), 1);
  }

  private void testRoundTrip(Geometry input, String layer, Map<String, Object> attrs, long id) throws IOException {
    int[] commands = VectorTileEncoder.getCommands(input);
    byte geomType = (byte) VectorTileEncoder.toGeomType(input).ordinal();
    Geometry output = VectorTileEncoder.decode(geomType, commands);
    assertTrue(input.equalsExact(output), "\n" + input + "\n!=\n" + output);

    byte[] encoded = new VectorTileEncoder().addLayerFeatures(layer, List.of(
      new LayerFeature(false, 0, 0, attrs,
        (byte) VectorTileEncoder.toGeomType(input).getNumber(), VectorTileEncoder.getCommands(input), id)
    )).encode();

    List<DecodedFeature> decoded = VectorTileEncoder.decode(encoded);
    DecodedFeature expected = new DecodedFeature(layer, 4096, input, attrs, id);
    assertEquals(List.of(expected), decoded);
  }
}
