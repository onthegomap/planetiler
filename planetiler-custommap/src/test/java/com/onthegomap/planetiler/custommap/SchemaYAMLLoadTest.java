package com.onthegomap.planetiler.custommap;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class SchemaYAMLLoadTest {

  /**
   * Test to ensure that all schemas in the schema folder load to POJOs.
   * 
   * @throws Exception
   */
  @Test
  public void testSchemaLoad() throws Exception {
    var schemaDir =
      Paths.get("samples");

    var schemaFiles = Files.walk(schemaDir)
      .filter(p -> p.getFileName().toString().endsWith(".yml"))
      .collect(Collectors.toList());

    for (Path schemaFile : schemaFiles) {
      assertNotNull(ConfiguredMapMain.loadConfig(schemaFile));
    }
  }


}
