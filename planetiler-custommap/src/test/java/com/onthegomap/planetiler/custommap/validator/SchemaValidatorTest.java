package com.onthegomap.planetiler.custommap.validator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.custommap.configschema.SchemaConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class SchemaValidatorTest {
  SchemaValidator.Result validate(String schema, String spec) {
    var result = SchemaValidator.validate(
      SchemaConfig.load(schema),
      SchemaSpecification.load(spec),
      Arguments.of()
    );
    for (var example : result.results()) {
      if (example.exception().isPresent()) {
        throw new RuntimeException(example.example().name() + " threw exception", example.exception().get());
      }
    }
    return result;
  }

  String waterSchema = """
    sources:
      osm:
        type: osm
        url: geofabrik:rhode-island
    layers:
    - name: water
      features:
      - source: osm
        geometry: polygon
        include_when:
          natural: water
        attributes:
        - key: natural
    """;

  private SchemaValidator.Result validateWater(String layer, String geometry, String tags, String allowExtraTags) {
    return validate(
      waterSchema,
      """
        examples:
        - name: test output
          input:
            source: osm
            geometry: polygon
            tags:
              natural: water
          output:
            layer: %s
            geometry: %s
            %s
            tags:
              %s
        """.formatted(layer, geometry, allowExtraTags == null ? "" : allowExtraTags,
        tags == null ? "" : tags.indent(6).strip())
    );
  }

  @ParameterizedTest
  @CsvSource(value = {
    "true,water,polygon,natural: water,",
    "true,water,polygon,,",
    "true,water,polygon,'natural: water\nother: null',",
    "false,water,polygon,natural: null,",
    "false,water2,polygon,natural: water,",
    "false,water,line,natural: water,",
    "false,water,line,natural: water,",
    "false,water,polygon,natural: water2,",
    "false,water,polygon,'natural: water\nother: value',",

    "true,water,polygon,natural: water,allow_extra_tags: true",
    "true,water,polygon,natural: water,allow_extra_tags: false",
    "true,water,polygon,,allow_extra_tags: true",
    "false,water,polygon,,allow_extra_tags: false",
  })
  void testValidateWaterPolygon(boolean shouldBeOk, String layer, String geometry, String tags, String allowExtraTags) {
    var results = validateWater(layer, geometry, tags, allowExtraTags);
    assertEquals(1, results.results().size());
    assertEquals("test output", results.results().get(0).example().name());
    if (shouldBeOk) {
      assertTrue(results.ok(), results.toString());
    } else {
      assertFalse(results.ok(), "Expected an issue, but there were none");
    }
  }

  @Test
  void testValidationFailsWrongNumberOfFeatures() {
    var results = validate(
      waterSchema,
      """
        examples:
        - name: test output
          input:
            source: osm
            geometry: polygon
            tags:
              natural: water
          output:
        """
    );
    assertFalse(results.ok(), results.toString());

    results = validate(
      waterSchema,
      """
        examples:
        - name: test output
          input:
            source: osm
            geometry: polygon
            tags:
              natural: water
          output:
          - layer: water
            geometry: polygon
            tags:
              natural: water
          - layer: water2
            geometry: polygon
            tags:
              natural: water2
        """
    );
    assertFalse(results.ok(), results.toString());
  }
}
