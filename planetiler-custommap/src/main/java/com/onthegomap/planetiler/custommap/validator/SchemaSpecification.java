package com.onthegomap.planetiler.custommap.validator;

import static com.onthegomap.planetiler.config.PlanetilerConfig.MAX_MAXZOOM;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.onthegomap.planetiler.custommap.YAML;
import com.onthegomap.planetiler.geo.GeometryType;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/** A model of example input source features and expected output vector tile features that a schema should produce. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record SchemaSpecification(List<Example> examples) {

  public static SchemaSpecification load(Path path) {
    return YAML.load(path, SchemaSpecification.class);
  }

  public static SchemaSpecification load(String string) {
    return YAML.load(string, SchemaSpecification.class);
  }

  /** An individual test case */
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

  /** Description of an input feature from a source that the schema will process. */
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

  /** Description of an expected vector tile feature that the schema should produce. */
  public record OutputFeature(
    String layer,
    GeometryType geometry,
    @JsonProperty("min_zoom") Integer minZoom,
    @JsonProperty("max_zoom") Integer maxZoom,
    @JsonProperty("min_size") Double minSize,
    @JsonProperty("at_zoom") Integer atZoom,
    @JsonProperty("allow_extra_tags") Boolean allowExtraTags,
    @JsonProperty("tags") Map<String, Object> tags
  ) {

    @Override
    public Map<String, Object> tags() {
      return tags == null ? Map.of() : tags;
    }

    @Override
    public Boolean allowExtraTags() {
      return allowExtraTags == null || allowExtraTags;
    }

    @Override
    public Integer atZoom() {
      return atZoom == null ? MAX_MAXZOOM : atZoom;
    }
  }
}
