package com.onthegomap.planetiler.custommap;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.onthegomap.planetiler.custommap.configschema.FeatureItem;
import com.onthegomap.planetiler.custommap.configschema.FeatureGeometry;
import com.onthegomap.planetiler.custommap.expression.ParseException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RelationMembersValidationTest {

  private static FeatureItem createFeatureItemWithMemberTypes(FeatureGeometry geometry, List<String> memberTypes) {
    return new FeatureItem(
      List.of("osm"),
      null, null, null, null, null, null,
      geometry,
      Map.of("highway", "primary"),
      null,
      List.of(),
      memberTypes,
      null, null, null, null
    );
  }

  private static FeatureItem createFeatureItemWithMemberRoles(FeatureGeometry geometry, List<String> memberRoles) {
    return new FeatureItem(
      List.of("osm"),
      null, null, null, null, null, null,
      geometry,
      Map.of("natural", "water"),
      null,
      List.of(),
      null,
      memberRoles,
      null, null, null
    );
  }

  private static FeatureItem createFeatureItemWithMemberIncludeWhen(FeatureGeometry geometry, 
    Map<String, Object> memberIncludeWhen) {
    return new FeatureItem(
      List.of("osm"),
      null, null, null, null, null, null,
      geometry,
      Map.of("amenity", "restaurant"),
      null,
      List.of(),
      null, null,
      memberIncludeWhen,
      null, null
    );
  }

  private static FeatureItem createFeatureItemWithInvalidMemberTypes(List<String> memberTypes) {
    return new FeatureItem(
      List.of("osm"),
      null, null, null, null, null, null,
      FeatureGeometry.RELATION_MEMBERS,
      Map.of("type", "route"),
      null,
      List.of(),
      memberTypes,
      null, null, null, null
    );
  }

  @Test
  void testMemberFieldsWithWrongGeometry() {
    // member_types with line geometry should fail
    assertThrows(ParseException.class, () -> createFeatureItemWithMemberTypes(FeatureGeometry.LINE, List.of("way")),
      "member_types should not be allowed with line geometry");

    // member_roles with polygon geometry should fail
    assertThrows(ParseException.class, () -> createFeatureItemWithMemberRoles(FeatureGeometry.POLYGON, List.of("outer")),
      "member_roles should not be allowed with polygon geometry");

    // member_include_when with point geometry should fail
    assertThrows(ParseException.class, 
      () -> createFeatureItemWithMemberIncludeWhen(FeatureGeometry.POINT, Map.of("highway", "primary")),
      "member_include_when should not be allowed with point geometry");
  }

  @Test
  void testInvalidMemberTypes() {
    // Invalid member_types value should fail
    assertThrows(ParseException.class, () -> createFeatureItemWithInvalidMemberTypes(List.of("invalid_type")),
      "Invalid member_types value should throw ParseException");
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

