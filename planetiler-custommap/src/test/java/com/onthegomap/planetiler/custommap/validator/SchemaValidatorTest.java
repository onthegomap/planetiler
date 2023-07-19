package com.onthegomap.planetiler.custommap.validator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.custommap.configschema.SchemaConfig;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class SchemaValidatorTest {
  @TempDir
  Path tmpDir;

  record Result(SchemaValidator.Result output, String cliOutput) {}

  private Result validate(String schema, String spec) throws IOException {
    var result = SchemaValidator.validate(
      SchemaConfig.load(schema),
      SchemaSpecification.load(spec)
    );
    for (var example : result.results()) {
      if (example.issues().isFailure()) {
        assertNotNull(example.issues().get());
      }
    }
    // also exercise the cli writer and return what it would have printed to stdout
    var cliOutput = validateCli(Files.writeString(tmpDir.resolve("schema"),
      schema + "\nexamples: " + Files.writeString(tmpDir.resolve("spec.yml"), spec)));

    // also test the case where the examples are embedded in the schema itself
    assertEquals(
      cliOutput,
      validateCli(Files.writeString(tmpDir.resolve("schema"), schema + "\n" + spec))
    );

    // also test where examples points to a relative path (written in previous step)
    assertEquals(
      cliOutput,
      validateCli(Files.writeString(tmpDir.resolve("schema"), schema + "\nexamples: spec.yml"))
    );
    return new Result(result, cliOutput);
  }

  private String validateCli(Path path) {
    try (
      var baos = new ByteArrayOutputStream();
      var printStream = new PrintStream(baos, true, StandardCharsets.UTF_8)
    ) {
      SchemaValidator.validateFromCli(
        path,
        printStream
      );
      return baos.toString(StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  String waterSchema = """
    sources:
      osm:
        type: osm
        url: geofabrik:rhode-island
    layers:
    - id: water
      features:
      - source: osm
        geometry: polygon
        min_size: 10
        include_when:
          natural: water
        attributes:
        - key: natural
    """;

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
  void testValidationFailsWrongNumberOfFeatures() throws IOException {
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

  @Test
  void testValidationWiresInArguments() throws IOException {
    var results = validate(
      """
        sources:
          osm:
            type: osm
            url: geofabrik:rhode-island
        args:
          key: default_value
        layers:
        - id: water
          features:
          - source: osm
            geometry: polygon
            include_when:
              natural: water
            attributes:
            - key: from_arg
              arg_value: key
            - key: threads
              value: '${ args.threads + 1 }'
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
            layer: water
            tags:
              from_arg: default_value
              threads: %s
        """.formatted(1 + Math.max(Runtime.getRuntime().availableProcessors(), 2))
    );
    assertTrue(results.output.ok(), results.toString());
  }
}
