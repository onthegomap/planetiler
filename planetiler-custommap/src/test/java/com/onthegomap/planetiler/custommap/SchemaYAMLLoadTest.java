package com.onthegomap.planetiler.custommap;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;

class SchemaYAMLLoadTest {

  /**
   * Test to ensure that all schemas in the schema folder load to POJOs.
   * 
   * @throws Exception
   */
  @Test
  void testSchemaLoad() throws Exception {
    var schemaDir =
      Paths.get("src", "main", "resources", "samples");

    var schemaFiles = Files.walk(schemaDir)
      .filter(p -> p.getFileName().toString().endsWith(".yml"))
      .toList();

    for (Path schemaFile : schemaFiles) {
      assertNotNull(ConfiguredMapMain.loadConfig(schemaFile), () -> "Failed to load " + schemaFile.toString());
    }
  }
}
