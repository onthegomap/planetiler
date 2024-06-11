package com.onthegomap.planetiler.validator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.reader.SourceFeature;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class BaseSchemaValidatorTest {

  private final String goodSpecString = """
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
    """;
  private final SchemaSpecification goodSpec = SchemaSpecification.load(goodSpecString);

  private final Profile waterSchema = new Profile() {
    @Override
    public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
      if (sourceFeature.canBePolygon() && sourceFeature.hasTag("natural", "water")) {
        features.polygon("water")
          .setMinPixelSize(10)
          .inheritAttrFromSource("natural");
      }
    }

    @Override
    public String name() {
      return "test profile";
    }
  };

  private Result validate(Profile profile, String spec) {
    var result = BaseSchemaValidator.validate(
      profile,
      SchemaSpecification.load(spec),
      PlanetilerConfig.defaults()
    );
    for (var example : result.results()) {
      if (example.issues().isFailure()) {
        assertNotNull(example.issues().get());
      }
    }
    // also exercise the cli writer and return what it would have printed to stdout
    var cliOutput = validateCli(profile, SchemaSpecification.load(spec));
    return new Result(result, cliOutput);
  }

  private String validateCli(Profile profile, SchemaSpecification spec) {
    try (
      var baos = new ByteArrayOutputStream();
      var printStream = new PrintStream(baos, true, StandardCharsets.UTF_8)
    ) {
      new BaseSchemaValidator(Arguments.of(), printStream) {
        @Override
        protected Result validate(Set<Path> pathsToWatch) {
          return validate(profile, spec, PlanetilerConfig.defaults());
        }
      }.validateFromCli();
      return baos.toString(StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private Result validateWater(String layer, String geometry, String tags, String allowExtraTags) throws IOException {
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

    "true,water,polygon,,min_size: 10",
    "false,water,polygon,,min_size: 9",
  })
  void testValidateWaterPolygon(boolean shouldBeOk, String layer, String geometry, String tags, String allowExtraTags)
    throws IOException {
    var results = validateWater(layer, geometry, tags, allowExtraTags);
    assertEquals(1, results.output.results().size());
    assertEquals("test output", results.output.results().get(0).example().name());
    if (shouldBeOk) {
      assertTrue(results.output.ok(), results.toString());
      assertFalse(results.cliOutput.contains("FAIL"), "contained FAIL but should not have: " + results.cliOutput);
    } else {
      assertFalse(results.output.ok(), "Expected an issue, but there were none");
      assertTrue(results.cliOutput.contains("FAIL"), "did not contain FAIL but should have: " + results.cliOutput);
    }
  }

  @Test
  void testPartialLengthAttribute() {
    var results = validate(
      (sourceFeature, features) -> features.line("layer")
        .linearRange(0, 0.6).setAttr("a", 1)
        .linearRange(0.4, 1).setAttr("b", 2),
      """
        examples:
        - name: test output
          input:
            source: osm
            geometry: line
            tags:
              natural: water
          output:
          - layer: layer
            geometry: line
            tags: {a:1}
          - layer: layer
            geometry: line
            tags: {a:1,b:2}
          - layer: layer
            geometry: line
            tags: {b:2}
        """
    );
    assertEquals(1, results.output.results().size());
    assertEquals("test output", results.output.results().getFirst().example().name());
    assertTrue(results.output.results().getFirst().ok());
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
    assertFalse(results.output.ok(), results.toString());

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
    assertFalse(results.output.ok(), results.toString());
  }

  @TestFactory
  Stream<DynamicNode> testJunitAdapterSpec() {
    return TestUtils.validateProfile(waterSchema, goodSpec);
  }

  @TestFactory
  Stream<DynamicNode> testJunitAdapterString() {
    return TestUtils.validateProfile(waterSchema, goodSpecString);
  }


  record Result(BaseSchemaValidator.Result output, String cliOutput) {}
}
