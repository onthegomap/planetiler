package com.onthegomap.planetiler.basemap.util;

import static com.onthegomap.planetiler.geo.GeoUtils.point;
import static com.onthegomap.planetiler.util.Gzip.gzip;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class VerifyMonacoTest {

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
  public void testStilInvalidWithOneTile() throws IOException {
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
    assertInvalid(mbtiles);
  }

  private void assertInvalid(Mbtiles mbtiles) {
    assertTrue(VerifyMonaco.verify(mbtiles).numErrors() > 0);
  }
}
