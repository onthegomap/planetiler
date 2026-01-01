package com.onthegomap.planetiler.custommap.configschema;

import com.fasterxml.jackson.annotation.JsonEnumDefaultValue;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.expression.Expression;
import com.onthegomap.planetiler.geo.GeometryType;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.WithTags;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmSourceFeature;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public enum FeatureGeometry {
  @JsonProperty("any") @JsonEnumDefaultValue
  ANY(GeometryType.UNKNOWN, FeatureCollector::anyGeometry),
  @JsonProperty("point")
  POINT(GeometryType.POINT, FeatureCollector::point),
  @JsonProperty("line")
  LINE(GeometryType.LINE, FeatureCollector::line),
  @JsonProperty("polygon")
  POLYGON(GeometryType.POLYGON, FeatureCollector::polygon),
  @JsonProperty("polygon_centroid")
  POLYGON_CENTROID(GeometryType.POLYGON, FeatureCollector::centroid),
  @JsonProperty("line_centroid")
  LINE_CENTROID(GeometryType.LINE, FeatureCollector::centroid),
  @JsonProperty("line_midpoint")
  LINE_MIDPOINT(GeometryType.LINE, FeatureCollector::lineMidpoint),
  @JsonProperty("centroid")
  CENTROID(GeometryType.UNKNOWN, FeatureCollector::centroid),
  @JsonProperty("polygon_centroid_if_convex")
  POLYGON_CENTROID_IF_CONVEX(GeometryType.POLYGON, FeatureCollector::centroidIfConvex),
  @JsonProperty("polygon_point_on_surface")
  POLYGON_POINT_ON_SURFACE(GeometryType.POLYGON, FeatureCollector::pointOnSurface),
  @JsonProperty("point_on_line")
  POINT_ON_LINE(GeometryType.LINE, FeatureCollector::pointOnSurface),
  @JsonProperty("innermost_point")
  INNERMOST_POINT(GeometryType.UNKNOWN, FeatureCollector::innermostPoint),
  @JsonProperty("relation_members")
  RELATION_MEMBERS(GeometryType.UNKNOWN, null) {
    @Override
    public Expression featureTest() {
      // Use inline script to check if feature is a relation
      // We'll create the expression lazily when needed, but for now return a simple check
      return new Expression() {
        @Override
        public boolean evaluate(WithTags input, List<String> matchKeys) {
          if (input instanceof SourceFeature sourceFeature && sourceFeature instanceof OsmSourceFeature osmFeature) {
            return osmFeature.originalElement().type() == OsmElement.Type.RELATION;
          }
          return false;
        }

        @Override
        public Expression simplifyOnce() {
          return this;
        }

        @Override
        public String generateJavaCode() {
          return "relationTypeTest()";
        }
      };
    }

    @Override
    public Function<FeatureCollector, FeatureCollector.Feature> newGeometryFactory(String layerName) {
      // This will be set up properly in ConfiguredFeature constructor
      // For now return a placeholder that will be replaced
      return features -> {
        throw new UnsupportedOperationException(
          "RELATION_MEMBERS geometry factory must be set up in ConfiguredFeature constructor");
      };
    }
  };

  public final GeometryType geometryType;
  public final BiFunction<FeatureCollector, String, FeatureCollector.Feature> geometryFactory;

  FeatureGeometry(GeometryType type, BiFunction<FeatureCollector, String, FeatureCollector.Feature> geometryFactory) {
    this.geometryType = type;
    this.geometryFactory = geometryFactory;
  }


  /**
   * Generates a test for whether a source feature is of the correct geometry to be included in the tile.
   *
   * @return geometry test method
   */
  public Expression featureTest() {
    return geometryType.featureTest();
  }

  /**
   * Generates a factory method which creates a {@link FeatureCollector.Feature} from a {@link FeatureCollector} of the
   * appropriate geometry type.
   *
   * @param layerName - name of the layer
   * @return geometry factory method
   */
  public Function<FeatureCollector, FeatureCollector.Feature> newGeometryFactory(String layerName) {
    return features -> geometryFactory.apply(features, layerName);
  }
}
