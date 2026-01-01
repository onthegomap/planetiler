package com.onthegomap.planetiler.custommap.configschema;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.onthegomap.planetiler.custommap.expression.ParseException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public record FeatureItem(
  @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY) List<String> source,
  @JsonProperty("min_zoom") Object minZoom,
  @JsonProperty("max_zoom") Object maxZoom,
  @JsonProperty("min_size") Object minSize,
  @JsonProperty("min_size_at_max_zoom") Object minSizeAtMaxZoom,
  @JsonProperty("tolerance") Object tolerance,
  @JsonProperty("tolerance_at_max_zoom") Object toleranceAtMaxZoom,
  @JsonProperty FeatureGeometry geometry,
  @JsonProperty("include_when") Object includeWhen,
  @JsonProperty("exclude_when") Object excludeWhen,
  Collection<AttributeDefinition> attributes,
  @JsonProperty("member_types") List<String> memberTypes,
  @JsonProperty("member_roles") List<String> memberRoles,
  @JsonProperty("member_include_when") Object memberIncludeWhen,
  @JsonProperty("member_exclude_when") Object memberExcludeWhen,
  @JsonProperty("member_attributes") Collection<AttributeDefinition> memberAttributes
) {

  private static final Set<String> VALID_MEMBER_TYPES = Set.of("node", "way", "relation");

  public FeatureItem {
    // Validate that member_* fields are only used with relation_members geometry
    FeatureGeometry actualGeometry = geometry == null ? FeatureGeometry.ANY : geometry;
    boolean hasMemberFields = memberTypes != null || memberRoles != null || memberIncludeWhen != null ||
      memberExcludeWhen != null || (memberAttributes != null && !memberAttributes.isEmpty());
    
    if (hasMemberFields && actualGeometry != FeatureGeometry.RELATION_MEMBERS) {
      throw new ParseException(
        "member_types, member_roles, member_include_when, member_exclude_when, and member_attributes can only be used with geometry: relation_members");
    }

    // Validate member_types values
    if (memberTypes != null) {
      for (String type : memberTypes) {
        if (!VALID_MEMBER_TYPES.contains(type)) {
          throw new ParseException(
            "Invalid member_types value: '" + type + "'. Valid values are: node, way, relation");
        }
      }
    }
  }

  @Override
  public Collection<AttributeDefinition> attributes() {
    return attributes == null ? List.of() : attributes;
  }

  @Override
  public FeatureGeometry geometry() {
    return geometry == null ? FeatureGeometry.ANY : geometry;
  }

  @Override
  public List<String> source() {
    return source == null ? List.of() : source;
  }

  @Override
  public List<String> memberTypes() {
    return memberTypes == null ? List.of() : memberTypes;
  }

  @Override
  public List<String> memberRoles() {
    return memberRoles == null ? List.of() : memberRoles;
  }

  @Override
  public Collection<AttributeDefinition> memberAttributes() {
    return memberAttributes == null ? List.of() : memberAttributes;
  }
}
