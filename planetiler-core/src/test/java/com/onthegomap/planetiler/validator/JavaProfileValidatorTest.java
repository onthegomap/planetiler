package com.onthegomap.planetiler.validator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class JavaProfileValidatorTest {
  private final Arguments args = Arguments.of();
  @TempDir
  Path tmpDir;

  record Result(BaseSchemaValidator.Result output, String cliOutput) {}

  private Result validate(Profile profile, String spec) throws IOException {
    var specPath = Files.writeString(tmpDir.resolve("spec.yaml"), spec);
    try (
      var baos = new ByteArrayOutputStream();
      var printStream = new PrintStream(baos, true, StandardCharsets.UTF_8)
    ) {
      var validator = new JavaProfileValidator(PlanetilerConfig.from(args), specPath, profile, printStream);
      var summary = validator.validateFromCli();
      assertEquals(Set.of(specPath), summary.paths());
      return new Result(summary.result(), baos.toString(StandardCharsets.UTF_8));
    }
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
    var results = validate(
      (sourceFeature, features) -> {
        if (sourceFeature.canBePolygon() && sourceFeature.hasTag("natural", "water")) {
          features.polygon("water")
            .inheritAttrFromSource("natural")
            .setMinPixelSize(10);
        }
      },
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
    assertEquals(1, results.output.results().size());
    assertEquals("test output", results.output.results().getFirst().example().name());
    if (shouldBeOk) {
      assertTrue(results.output.ok(), results.toString());
      assertFalse(results.cliOutput.contains("FAIL"), "contained FAIL but should not have: " + results.cliOutput);
    } else {
      assertFalse(results.output.ok(), "Expected an issue, but there were none");
      assertTrue(results.cliOutput.contains("FAIL"), "did not contain FAIL but should have: " + results.cliOutput);
    }
  }
}
