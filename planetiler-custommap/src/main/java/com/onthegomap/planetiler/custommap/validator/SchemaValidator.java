package com.onthegomap.planetiler.custommap.validator;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.custommap.ConfiguredProfile;
import com.onthegomap.planetiler.custommap.configschema.SchemaConfig;
import com.onthegomap.planetiler.geo.GeometryType;
import com.onthegomap.planetiler.reader.SimpleFeature;
import com.onthegomap.planetiler.stats.Stats;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import org.geotools.geometry.jts.WKTReader2;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;

public class SchemaValidator {
  private static Geometry parseGeometry(String geometry) {
    String wkt = switch (geometry.toLowerCase(Locale.ROOT).trim()) {
      case "point" -> "POINT (0 0)";
      case "line" -> "LINESTRING (0 0, 1 1)";
      case "polygon" -> "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))";
      default -> geometry;
    };
    try {
      return new WKTReader2().read(wkt);
    } catch (ParseException e) {
      throw new IllegalArgumentException("""
        Bad geometry: "%s", must be "point" "line" "polygon" or a valid WKT string.
        """.formatted(geometry));
    }
  }

  public static Result validate(SchemaConfig schema, SchemaSpecification specification) {
    var profile = new ConfiguredProfile(schema);
    var featureCollectorFactory = new FeatureCollector.Factory(PlanetilerConfig.defaults(), Stats.inMemory());
    return new Result(specification.examples().stream().map(example -> {
      List<String> issues = new ArrayList<>();
      Exception exception = null;
      try {
        var input = example.input();
        var expectedFeatures = example.output();
        var geometry = parseGeometry(input.geometry());
        var feature = SimpleFeature.create(geometry, input.tags(), input.source(), null, 0);
        var collector = featureCollectorFactory.get(feature);
        profile.processFeature(feature, collector);
        List<FeatureCollector.Feature> result = new ArrayList<>();
        collector.forEach(result::add);
        if (result.size() != expectedFeatures.size()) {
          issues.add(
            "Different number of elements, expected=%s actual=%s".formatted(expectedFeatures.size(), result.size()));
        } else {
          var expectedList =
            expectedFeatures.stream().sorted(Comparator.comparing(d -> d.layer())).toList();
          var actualList = result.stream()
            .sorted(Comparator.comparing(d -> d.getLayer()))
            .toList();
          for (int i = 0; i < expectedList.size(); i++) {
            var expected = expectedList.get(i);
            var actual = actualList.get(i);
            var actualTags = actual.getAttrsAtZoom(expected.atZoom());
            validate("layer", issues, expected.layer(), actual.getLayer());
            validate("minzoom", issues, expected.minZoom(), actual.getMinZoom());
            validate("maxzoom", issues, expected.maxZoom(), actual.getMaxZoom());
            validate("geometry", issues, expected.geometry(), GeometryType.valueOf(actual.getGeometry()));
            expected.tags().forEach((tag, value) -> {
              validate("tags[\"%s\"]".formatted(tag), issues, value, actualTags.get(tag), false);
            });
          }
        }
      } catch (Exception e) {
        exception = e;
      }
      return new ExampleResult(example, Optional.ofNullable(exception), issues);
    }).toList());
  }

  private static <T> void validate(String field, List<String> issues, T expected, T actual, boolean ignoreWhenNull) {
    if ((!ignoreWhenNull || expected != null) && !Objects.equals(expected, actual)) {
      issues.add("%s: expected %s actual %s".formatted(field, expected, actual));
    }
  }

  private static <T> void validate(String field, List<String> issues, T expected, T actual) {
    validate(field, issues, expected, actual, true);
  }

  public record ExampleResult(
    SchemaSpecification.Example example,
    Optional<Exception> exception,
    List<String> issues) {
    public boolean ok() {
      return exception.isEmpty() && issues.isEmpty();
    }
  }

  public record Result(List<ExampleResult> results) {
    public boolean ok() {
      return results.stream().allMatch(ExampleResult::ok);
    }
  }
}
