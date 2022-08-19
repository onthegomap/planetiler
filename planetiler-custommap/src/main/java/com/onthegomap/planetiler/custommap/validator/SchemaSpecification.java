package com.onthegomap.planetiler.custommap.validator;

import static com.onthegomap.planetiler.config.PlanetilerConfig.MAX_MAXZOOM;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.onthegomap.planetiler.custommap.YAML;
import com.onthegomap.planetiler.geo.GeometryType;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public record SchemaSpecification(List<Example> examples) {

  public static SchemaSpecification load(Path path) {
    return YAML.load(path, SchemaSpecification.class);
  }

  public static SchemaSpecification load(String string) {
    return YAML.load(string, SchemaSpecification.class);
  }

  public record Example(
    String name,
    InputFeature input,
    @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY) List<OutputFeature> output
  ) {

    @Override
    public List<OutputFeature> output() {
      return output == null ? List.of() : output;
    }
  }

  public record InputFeature(
    String source,
    String geometry,
    Map<String, Object> tags
  ) {

    @Override
    public Map<String, Object> tags() {
      return tags == null ? Map.of() : tags;
    }
  }
  public record OutputFeature(
    String layer,
    GeometryType geometry,
    Integer minZoom,
    Integer maxZoom,
    Integer atZoom,
    Map<String, Object> tags
  ) {

    @Override
    public Map<String, Object> tags() {
      return tags == null ? Map.of() : tags;
    }

    @Override
    public Integer atZoom() {
      return atZoom == null ? MAX_MAXZOOM : atZoom;
    }
  }
}
