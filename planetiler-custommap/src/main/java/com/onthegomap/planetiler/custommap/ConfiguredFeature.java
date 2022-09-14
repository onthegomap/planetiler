package com.onthegomap.planetiler.custommap;

import static com.onthegomap.planetiler.custommap.TagCriteria.matcher;
import static com.onthegomap.planetiler.custommap.expression.ConfigFunction.constOf;
import static com.onthegomap.planetiler.expression.Expression.not;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureCollector.Feature;
import com.onthegomap.planetiler.custommap.configschema.AttributeDefinition;
import com.onthegomap.planetiler.custommap.configschema.FeatureGeometry;
import com.onthegomap.planetiler.custommap.configschema.FeatureItem;
import com.onthegomap.planetiler.custommap.expression.Contexts;
import com.onthegomap.planetiler.expression.DataTypes;
import com.onthegomap.planetiler.expression.Expression;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SourceFeature;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * A map feature, configured from a YML configuration file.
 *
 * {@link #matchExpression()} returns a filtering expression to limit input elements to ones this feature cares about,
 * and {@link #processFeature(Contexts.ProcessFeature.PostMatch, FeatureCollector)} processes matching elements.
 */
public class ConfiguredFeature {
  private final Expression geometryTest;
  private final Function<FeatureCollector, Feature> geometryFactory;
  private final Expression tagTest;
  private final Function<Contexts.ProcessFeature.PostMatch, Integer> featureMinZoom;
  private final Function<Contexts.ProcessFeature.PostMatch, Integer> featureMaxZoom;
  private final TagValueProducer tagValueProducer;

  private static final double LOG4 = Math.log(4);

  private final List<AttributeProcessor> attributeProcessors;
  private final Set<String> sources;


  @FunctionalInterface
  private interface AttributeProcessor {
    void process(Contexts.ProcessFeature.PostMatch context, Feature outputFeature);
  }

  public ConfiguredFeature(String layerName, TagValueProducer tagValueProducer, FeatureItem feature) {
    sources = Set.copyOf(feature.source());

    FeatureGeometry geometryType = feature.geometry();

    //Test to determine whether this type of geometry is included
    geometryTest = geometryType.featureTest();

    //Factory to treat OSM tag values as specific data type values
    this.tagValueProducer = tagValueProducer;

    //Test to determine whether this feature is included based on tagging
    Expression filter;
    if (feature.includeWhen() == null) {
      filter = Expression.TRUE;
    } else {
      filter = matcher(feature.includeWhen(), tagValueProducer, Contexts.ProcessFeature.DESCRIPTION);
    }
    if (feature.excludeWhen() != null) {
      filter = Expression.and(
        filter,
        Expression.not(matcher(feature.excludeWhen(), tagValueProducer, Contexts.ProcessFeature.DESCRIPTION))
      );
    }
    tagTest = filter;

    //Test to determine at which zooms to include this feature based on tagging
    featureMinZoom = feature.minZoom() == null ? constOf(null) : TagFunction.function(
      feature.minZoom(),
      tagValueProducer,
      Contexts.ProcessFeature.PostMatch.DESCRIPTION,
      Integer.class
    );
    featureMaxZoom = feature.maxZoom() == null ? constOf(null) : TagFunction.function(
      feature.maxZoom(),
      tagValueProducer,
      Contexts.ProcessFeature.PostMatch.DESCRIPTION,
      Integer.class
    );

    //Factory to generate the right feature type from FeatureCollector
    geometryFactory = geometryType.newGeometryFactory(layerName);

    //Configure logic for each attribute in the output tile
    attributeProcessors = feature.attributes()
      .stream()
      .map(this::attributeProcessor)
      .toList();
  }

  /**
   * Produces logic that generates attribute values based on configuration and input data. If both a constantValue
   * configuration and a tagValue configuration are set, this is likely a mistake, and the constantValue will take
   * precedence.
   *
   * @param attribute - attribute definition configured from YML
   * @return a function that generates an attribute value from a {@link SourceFeature} based on an attribute
   *         configuration.
   */
  private Function<Contexts.ProcessFeature.PostMatch, Object> attributeValueProducer(AttributeDefinition attribute) {
    Function<Contexts.ProcessFeature.PostMatch, Object> result;
    String type = attribute.type();

    if (attribute.value() != null) {
      result = TagFunction.function(attribute.value(), tagValueProducer, Contexts.ProcessFeature.PostMatch.DESCRIPTION,
        Object.class);
    } else if (attribute.tagValue() != null) {
      result = tagValueProducer.valueProducerForKey(attribute.tagValue());
    } else if ("match_key".equals(type)) {
      result = Contexts.ProcessFeature.PostMatch::matchKey;
    } else if ("match_value".equals(type)) {
      result = Contexts.ProcessFeature.PostMatch::matchValue;
    } else {
      result = tagValueProducer.valueProducerForKey(attribute.key());
    }

    // if type is set, coerce the result to the desired datatype
    if (type != null && !Set.of("match_key", "match_value").contains(type)) {
      var dataType = DataTypes.from(attribute.type());
      if (dataType == DataTypes.GET_TAG) {
        throw new IllegalArgumentException("Unrecognized value for type: " + type);
      }
      var previousResult = result;
      result = ctx -> dataType.convertFrom(previousResult.apply(ctx));
    }

    return result;
  }

  /**
   * Generate logic which determines the minimum zoom level for a feature based on a configured pixel size limit.
   *
   * @param minTilePercent - minimum percentage of a tile that a feature must cover to be shown
   * @param rawMinZoom     - global minimum zoom for this feature, or an expression providing the min zoom dynamically
   * @param minZoomByValue - map of tag values to zoom level
   * @return minimum zoom function
   */
  private Function<Contexts.ProcessFeature.PostMatch.AttrZoom, Integer> attributeZoomThreshold(
    Double minTilePercent, Object rawMinZoom, Map<Object, Integer> minZoomByValue) {

    var result = TagFunction.function(rawMinZoom, tagValueProducer,
      Contexts.ProcessFeature.PostMatch.AttrZoom.DESCRIPTION, Integer.class);

    if ((result.equals(constOf(0)) ||
      result.equals(constOf(null))) && minZoomByValue.isEmpty()) {
      return null;
    }

    if (minZoomByValue.isEmpty()) {
      return context -> Math.max(result.apply(context), minZoomFromTilePercent(context.feature(), minTilePercent));
    }

    //Attribute value-specific zooms override static zooms
    return context -> {
      var value = minZoomByValue.get(context.value());
      return value != null ? value :
        Math.max(result.apply(context), minZoomFromTilePercent(context.feature(), minTilePercent));
    };
  }

  private static int minZoomFromTilePercent(SourceFeature sf, Double minTilePercent) {
    if (minTilePercent == null) {
      return 0;
    }
    try {
      return (int) (Math.log(minTilePercent / sf.area()) / LOG4);
    } catch (GeometryException e) {
      return 14;
    }
  }

  /**
   * Generates a function which produces a fully-configured attribute for a feature.
   *
   * @param attribute - configuration for this attribute
   * @return processing logic
   */
  private AttributeProcessor attributeProcessor(AttributeDefinition attribute) {
    var tagKey = attribute.key();

    Object attributeMinZoom = attribute.minZoom();
    attributeMinZoom = attributeMinZoom == null ? "0" : attributeMinZoom;

    var minZoomByValue = attribute.minZoomByValue();
    minZoomByValue = minZoomByValue == null ? Map.of() : minZoomByValue;

    //Workaround because numeric keys are mapped as String
    minZoomByValue = tagValueProducer.remapKeysByType(tagKey, minZoomByValue);

    var attributeValueProducer = attributeValueProducer(attribute);
    var fallback = attribute.fallback();

    var attrIncludeWhen = attribute.includeWhen();
    var attrExcludeWhen = attribute.excludeWhen();

    var attributeTest =
      Expression.and(
        attrIncludeWhen == null ? Expression.TRUE :
          matcher(attrIncludeWhen, tagValueProducer, Contexts.ProcessFeature.PostMatch.DESCRIPTION),
        attrExcludeWhen == null ? Expression.TRUE :
          not(matcher(attrExcludeWhen, tagValueProducer, Contexts.ProcessFeature.PostMatch.DESCRIPTION))
      ).simplify();

    var minTileCoverage = attrIncludeWhen == null ? null : attribute.minTileCoverSize();

    Function<Contexts.ProcessFeature.PostMatch.AttrZoom, Integer> attributeZoomProducer =
      attributeZoomThreshold(minTileCoverage, attributeMinZoom, minZoomByValue);

    return (context, f) -> {
      Object value = null;
      if (attributeTest.evaluate(context)) {
        value = attributeValueProducer.apply(context);
        if ("".equals(value)) {
          value = null;
        }
      }
      if (value == null) {
        value = fallback;
      }
      if (value != null) {
        if (attributeZoomProducer != null) {
          f.setAttrWithMinzoom(tagKey, value, attributeZoomProducer.apply(context.createAttrZoomContext(value)));
        } else {
          f.setAttr(tagKey, value);
        }
      }
    };
  }

  /**
   * Returns an expression that evaluates to true if a source feature should be included in the output.
   */
  public Expression matchExpression() {
    return Expression.and(geometryTest, tagTest);
  }

  /**
   * Generates a tile feature based on a source feature.
   *
   * @param context  The evaluation context containing the source feature
   * @param features output rendered feature collector
   */
  public void processFeature(Contexts.ProcessFeature.PostMatch context, FeatureCollector features) {
    var sourceFeature = context.feature();

    // Ensure that this feature is from the correct source (index should enforce this)
    assert sources.contains(sourceFeature.getSource());

    var f = geometryFactory.apply(features);
    var minZoom = featureMinZoom.apply(context);
    if (minZoom != null) {
      f.setMinZoom(minZoom);
    }
    var maxZoom = featureMaxZoom.apply(context);
    if (maxZoom != null) {
      f.setMaxZoom(maxZoom);
    }

    for (var processor : attributeProcessors) {
      processor.process(context, f);
    }
  }
}
