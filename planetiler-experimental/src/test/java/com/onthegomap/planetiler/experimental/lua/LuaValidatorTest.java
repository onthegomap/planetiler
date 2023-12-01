package com.onthegomap.planetiler.experimental.lua;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.validator.BaseSchemaValidator;
import com.onthegomap.planetiler.validator.SchemaSpecification;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class LuaValidatorTest {
  private final Arguments args = Arguments.of();
  @TempDir
  Path tmpDir;

  record Result(BaseSchemaValidator.Result output, String cliOutput) {}

  private Result validate(String schema, String spec) throws IOException {
    var result = LuaValidator.validate(
      LuaEnvironment.loadScript(args, schema, "schema.lua").profile,
      SchemaSpecification.load(spec),
      PlanetilerConfig.defaults()
    );
    for (var example : result.results()) {
      if (example.issues().isFailure()) {
        assertNotNull(example.issues().get());
      }
    }
    Path schemaPath = tmpDir.resolve("schema.lua");
    Path specPath = tmpDir.resolve("spec.yml");
    // also exercise the cli writer and return what it would have printed to stdout
    String script = schema + "\nplanetiler.examples= '" +
      Files.writeString(specPath, spec).toString().replace("\\", "\\\\").replace("'", "\\'") + "'";
    var cliOutput = validateCli(Files.writeString(schemaPath, script));

    return new Result(result, cliOutput);
  }

  private String validateCli(Path path) {
    try (
      var baos = new ByteArrayOutputStream();
      var printStream = new PrintStream(baos, true, StandardCharsets.UTF_8)
    ) {
      new LuaValidator(args, path.toString(), printStream).validateFromCli();
      return baos.toString(StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
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
      """
        function planetiler.process_feature(source, features)
          if source:can_be_polygon() and source:has_tag("natural", "water") then
            features:polygon("water")
                :inherit_attr_from_source("natural")
                :set_min_pixel_size(10)
          end
        end
        function main() end
        """,
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
    assertEquals("test output", results.output.results().get(0).example().name());
    if (shouldBeOk) {
      assertTrue(results.output.ok(), results.toString());
      assertFalse(results.cliOutput.contains("FAIL"), "contained FAIL but should not have: " + results.cliOutput);
    } else {
      assertFalse(results.output.ok(), "Expected an issue, but there were none");
      assertTrue(results.cliOutput.contains("FAIL"), "did not contain FAIL but should have: " + results.cliOutput);
    }
  }
}
