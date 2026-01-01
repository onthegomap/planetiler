package com.onthegomap.planetiler.custommap;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.custommap.configschema.FeatureItem;
import com.onthegomap.planetiler.custommap.configschema.FeatureGeometry;
import com.onthegomap.planetiler.custommap.expression.ParseException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RelationMembersValidationTest {

  @Test
  void testMemberFieldsWithWrongGeometry() {
    // member_types with line geometry should fail
    assertThrows(ParseException.class, () -> {
      new FeatureItem(
        List.of("osm"),
        null, null, null, null, null, null,
        FeatureGeometry.LINE,
        Map.of("highway", "primary"),
        null,
        List.of(),
        List.of("way"), // member_types
        null, null, null, null
      );
    }, "member_types should not be allowed with line geometry");

    // member_roles with polygon geometry should fail
    assertThrows(ParseException.class, () -> {
      new FeatureItem(
        List.of("osm"),
        null, null, null, null, null, null,
        FeatureGeometry.POLYGON,
        Map.of("natural", "water"),
        null,
        List.of(),
        null,
        List.of("outer"), // member_roles
        null, null, null
      );
    }, "member_roles should not be allowed with polygon geometry");

    // member_include_when with point geometry should fail
    assertThrows(ParseException.class, () -> {
      new FeatureItem(
        List.of("osm"),
        null, null, null, null, null, null,
        FeatureGeometry.POINT,
        Map.of("amenity", "restaurant"),
        null,
        List.of(),
        null, null,
        Map.of("highway", "primary"), // member_include_when
        null, null
      );
    }, "member_include_when should not be allowed with point geometry");
  }

  @Test
  void testInvalidMemberTypes() {
    // Invalid member_types value should fail
    assertThrows(ParseException.class, () -> {
      new FeatureItem(
        List.of("osm"),
        null, null, null, null, null, null,
        FeatureGeometry.RELATION_MEMBERS,
        Map.of("type", "route"),
        null,
        List.of(),
        List.of("invalid_type"), // invalid member_types
        null, null, null, null
      );
    }, "Invalid member_types value should throw ParseException");
  }

  @Test
  void testValidRelationMembers() {
    // Valid relation_members configuration should not throw
    new FeatureItem(
      List.of("osm"),
      null, null, null, null, null, null,
      FeatureGeometry.RELATION_MEMBERS,
      Map.of("type", "route"),
      null,
      List.of(),
      List.of("way"), // member_types
      List.of(""), // member_roles
      Map.of("highway", "primary"), // member_include_when
      Map.of("service", "__any__"), // member_exclude_when
      List.of() // member_attributes
    );
    // Should not throw
  }
}

