package com.onthegomap.planetiler.custommap;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;

class SchemaYAMLLoadTest {

  /**
   * Test to ensure that all bundled schemas load to POJOs.
   * 
   * @throws Exception
   */
  @Test
  void testSchemaLoad() throws IOException {
    testSchemasInFolder(Paths.get("src", "main", "resources", "samples"));
    testSchemasInFolder(Paths.get("src", "test", "resources"));
  }

  private void testSchemasInFolder(Path path) throws IOException {
    var schemaFiles = Files.walk(path)
      .filter(p -> p.getFileName().toString().endsWith(".yml"))
      .toList();

    for (Path schemaFile : schemaFiles) {
      assertNotNull(ConfiguredMapMain.loadConfig(schemaFile), () -> "Failed to load " + schemaFile.toString());
    }
  }
}
