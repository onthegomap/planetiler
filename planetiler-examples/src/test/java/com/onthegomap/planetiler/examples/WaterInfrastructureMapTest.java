package com.onthegomap.planetiler.examples;

import com.onthegomap.planetiler.TestContext;
import com.onthegomap.planetiler.WaterInfrastructureProfile;
import com.onthegomap.planetiler.reader.SimpleFeature;
import java.util.Map;
import org.junit.jupiter.api.Test;


public class WaterInfrastructureMapTest extends TestContext {

  @Test
  public void testWaterWellMapping() {

    //1. create a fake OSM well

    var input = SimpleFeature.create(
      SimpleFeature.Type.POINT,
      Map.of("man_made", "water_well", "name", "Juba Central Well")

    );
    //2. Execute
    var profile = new WaterInfrastructureProfile();
    var collector = featureCollector();
    profile.processFeature(input, collector);

    // 3. Verify: Should be in 'water_supply' layer with the right name
    assertGeneratedPoints(collector, "water_supply", 1);
    var feature = getFirstFeature(collector, "water_supply");
    assertEquals("Juba Central Well", feature.getAttr("name"));

  }

  @Test
  public void testIgnoreNonWaterAmenities() {
    // Setup: Create a school (which we decided to exclude)
    var input = SimpleFeature.create(
      SimpleFeature.Type.POINT,
      Map.of("amenity", "school")
    );

    var profile = new WaterInfrastructureProfile();
    var collector = featureCollector();
    profile.processFeature(input, collector);

    // Verify: The 'water_supply' layer should be empty
    assertGeneratedPoints(collector, "water_supply", 0);
  }


}
