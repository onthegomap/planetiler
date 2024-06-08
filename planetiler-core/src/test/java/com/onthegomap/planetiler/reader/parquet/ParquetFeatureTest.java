package com.onthegomap.planetiler.reader.parquet;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

class ParquetFeatureTest {
  private static ParquetFeature feature(Map<String, Object> tags) throws IOException {
    var schema = Types.buildMessage().addField(Types.required(PrimitiveType.PrimitiveTypeName.BINARY).named("geometry"))
      .named("root");
    return new ParquetFeature("overture", "layer", 1,
      new GeometryReader(GeoParquetMetadata.parse(new FileMetaData(schema, Map.of(), "geometry"))),
      new HashMap<>(tags),
      Path.of(""), schema);
  }

  @Test
  void testHasTag() throws IOException {
    var feature = feature(Map.of("names", Map.of("primary", "name")));
    assertTrue(feature.hasTag("names.primary"));
    assertTrue(feature.hasTag("names[].primary"));
    assertTrue(feature.hasTag("names.primary", "name"));
    assertTrue(feature.hasTag("names[].primary", "name"));
    assertTrue(feature.hasTag("names[].primary", List.of("name", "name2")));
    assertFalse(feature.hasTag("names.primary", "not name"));
    assertFalse(feature.hasTag("names.primary", List.of("not name", "not name 2")));
  }

  @Test
  void testHasTagWithArg() throws IOException {
    var feature = feature(Map.of("names", Map.of("primary", "name")));
    assertTrue(feature.hasTag("names.primary", "name1", "name"));
    assertTrue(feature.hasTag("names.primary", "name", "name1"));
    assertTrue(feature.hasTag("names.primary", List.of("name"), "name1"));
    assertTrue(feature.hasTag("names.primary", "name1", List.of("name")));
  }
}
