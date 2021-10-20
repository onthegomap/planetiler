package com.onthegomap.flatmap.mbtiles;

import static com.onthegomap.flatmap.TestUtils.newPolygon;
import static com.onthegomap.flatmap.geo.GeoUtils.point;
import static com.onthegomap.flatmap.util.Gzip.gzip;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.flatmap.VectorTile;
import com.onthegomap.flatmap.geo.TileCoord;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class VerifyTest {

  private Mbtiles mbtiles;

  @BeforeEach
  public void setup() {
    mbtiles = Mbtiles.newInMemoryDatabase();
  }

  @AfterEach
  public void teardown() throws IOException {
    mbtiles.close();
  }

  @Test
  public void testEmptyFileInvalid() {
    assertInvalid(mbtiles);
  }

  @Test
  public void testEmptyTablesInvalid() {
    mbtiles.createTables().addTileIndex();
    assertInvalid(mbtiles);
  }

  @Test
  public void testValidWithNameAndOneTile() throws IOException {
    mbtiles.createTables().addTileIndex();
    mbtiles.metadata().setName("name");
    try (var writer = mbtiles.newBatchedTileWriter()) {
      VectorTile tile = new VectorTile();
      tile.addLayerFeatures("layer", List.of(new VectorTile.Feature(
        "layer",
        1,
        VectorTile.encodeGeometry(point(0, 0)),
        Map.of()
      )));
      writer.write(TileCoord.ofXYZ(0, 0, 0), gzip(tile.encode()));
    }
    assertValid(mbtiles);
  }

  @Test
  public void testInvalidGeometry() throws IOException {
    mbtiles.createTables().addTileIndex();
    mbtiles.metadata().setName("name");
    try (var writer = mbtiles.newBatchedTileWriter()) {
      VectorTile tile = new VectorTile();
      tile.addLayerFeatures("layer", List.of(new VectorTile.Feature(
        "layer",
        1,
        // self-intersecting bow-tie shape
        VectorTile.encodeGeometry(newPolygon(
          0, 0,
          10, 0,
          0, 10,
          10, 10,
          0, 0
        )),
        Map.of()
      )));
      writer.write(TileCoord.ofXYZ(0, 0, 0), gzip(tile.encode()));
    }
    assertInvalid(mbtiles);
  }

  private void assertInvalid(Mbtiles mbtiles) {
    assertTrue(Verify.verify(mbtiles).numErrors() > 0);
  }

  private void assertValid(Mbtiles mbtiles) {
    assertEquals(0, Verify.verify(mbtiles).numErrors());
  }
}
