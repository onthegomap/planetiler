package com.onthegomap.planetiler.custommap.configschema;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

public class SchemaYAMLLoadTest {

  /**
   * Test to ensure that all schemas in the schema folder load to POJOs.
   * 
   * @throws Exception
   */
  @Test
  public void testSchemaLoad() throws Exception {
    Path schemaDir =
      Paths.get("src", "main", "resources", "schemas");

    List<File> schemaFiles = Files.walk(schemaDir)
      .filter(p -> p.getFileName().toString().endsWith(".yml"))
      .map(p -> p.toFile())
      .collect(Collectors.toList());

    for (File schemaFile : schemaFiles) {
      Yaml yml = new Yaml();
      SchemaConfig config = yml.loadAs(new FileInputStream(schemaFile), SchemaConfig.class);
      assertNotNull(config);
    }
  }


}
