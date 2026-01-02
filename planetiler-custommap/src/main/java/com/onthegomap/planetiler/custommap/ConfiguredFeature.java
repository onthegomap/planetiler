package com.onthegomap.planetiler.custommap;

import static com.onthegomap.planetiler.custommap.expression.ConfigExpression.constOf;
import static com.onthegomap.planetiler.expression.Expression.not;

import com.carrotsearch.hppc.LongArrayList;
import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureCollector.Feature;
import com.onthegomap.planetiler.custommap.configschema.AttributeDefinition;
import com.onthegomap.planetiler.custommap.configschema.FeatureGeometry;
import com.onthegomap.planetiler.custommap.configschema.FeatureItem;
import com.onthegomap.planetiler.custommap.configschema.FeatureLayer;
import com.onthegomap.planetiler.custommap.expression.ScriptEnvironment;
import com.onthegomap.planetiler.expression.Expression;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.RelationMemberDataProvider;
import com.onthegomap.planetiler.reader.osm.RelationSourceFeature;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ObjDoubleConsumer;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;

/**
 * A map feature, configured from a YML configuration file.
 *
 * {@link #matchExpression()} returns a filtering expression to limit input elements to ones this feature cares about,
 * and {@link #processFeature(Contexts.FeaturePostMatch, FeatureCollector)} processes matching elements.
 */
public class ConfiguredFeature {
  private static final double LOG4 = Math.log(4);
  private final Expression geometryTest;
  private final Function<FeatureCollector, Feature> geometryFactory;
  private final Expression tagTest;
  private final TagValueProducer tagValueProducer;
  private final List<BiConsumer<Contexts.FeaturePostMatch, Feature>> featureProcessors;
  private final Set<String> sources;
  private final String layerId;
  private final ScriptEnvironment<Contexts.ProcessFeature> processFeatureContext;
  private final ScriptEnvironment<Contexts.FeatureAttribute> featureAttributeContext;
  private ScriptEnvironment<Contexts.FeaturePostMatch> featurePostMatchContext;
  private ScriptEnvironment<Contexts.MemberContext> memberContext;
  
  // Member processing configuration (only used for RELATION_MEMBERS geometry)
  private final boolean isRelationMembers;
  private final Set<String> memberTypes;
  private final Set<String> memberRoles;
  private final Expression memberIncludeWhen;
  private final Expression memberExcludeWhen;
  private final List<BiConsumer<Contexts.MemberContext, Feature>> memberAttributeProcessors;


  public ConfiguredFeature(FeatureLayer layer, TagValueProducer tagValueProducer, FeatureItem feature,
    Contexts.Root rootContext) {
    sources = Set.copyOf(feature.source());
    layerId = layer.id();

    FeatureGeometry geometryType = feature.geometry();

    //Test to determine whether this type of geometry is included
    geometryTest = geometryType.featureTest();

    //Factory to treat OSM tag values as specific data type values
    this.tagValueProducer = tagValueProducer;
    processFeatureContext = Contexts.ProcessFeature.description(rootContext);
    featurePostMatchContext = Contexts.FeaturePostMatch.description(rootContext);
    featureAttributeContext = Contexts.FeatureAttribute.description(rootContext);

    //Test to determine whether this feature is included based on tagging
    Expression filter;
    if (feature.includeWhen() == null) {
      filter = Expression.TRUE;
    } else {
      filter =
        BooleanExpressionParser.parse(feature.includeWhen(), tagValueProducer,
          processFeatureContext);
    }
    if (!feature.source().isEmpty()) {
      filter = Expression.and(
        filter,
        Expression.or(feature.source().stream().map(Expression::matchSource).toList())
      );
    }
    if (feature.excludeWhen() != null) {
      filter = Expression.and(
        filter,
        Expression.not(
          BooleanExpressionParser.parse(feature.excludeWhen(), tagValueProducer,
            processFeatureContext))
      );
    }
    tagTest = filter;

    //Factory to generate the right feature type from FeatureCollector
    geometryFactory = geometryType.newGeometryFactory(layer.id());

    // Member processing configuration (only used for RELATION_MEMBERS geometry)
    isRelationMembers = geometryType == FeatureGeometry.RELATION_MEMBERS;
    
    if (isRelationMembers) {
      memberContext = Contexts.MemberContext.description(rootContext);
      
      List<String> memberTypesList = feature.memberTypes();
      memberTypes = memberTypesList.isEmpty() ? 
        Set.of("node", "way", "relation") : Set.copyOf(memberTypesList);
      
      List<String> memberRolesList = feature.memberRoles();
      memberRoles = memberRolesList == null || memberRolesList.isEmpty() ? 
        null : Set.copyOf(memberRolesList);
      
      if (feature.memberIncludeWhen() != null) {
        memberIncludeWhen = BooleanExpressionParser.parse(
          feature.memberIncludeWhen(), tagValueProducer, memberContext);
      } else {
        memberIncludeWhen = Expression.TRUE;
      }
      
      if (feature.memberExcludeWhen() != null) {
        memberExcludeWhen = BooleanExpressionParser.parse(
          feature.memberExcludeWhen(), tagValueProducer, memberContext);
      } else {
        memberExcludeWhen = Expression.FALSE;
      }
      
      List<BiConsumer<Contexts.MemberContext, Feature>> memberAttrProcessors = new ArrayList<>();
      for (var memberAttr : feature.memberAttributes()) {
        memberAttrProcessors.add(memberAttributeProcessor(memberAttr));
      }
      memberAttributeProcessors = memberAttrProcessors;
    } else {
      memberTypes = null;
      memberRoles = null;
      memberIncludeWhen = null;
      memberExcludeWhen = null;
      memberAttributeProcessors = null;
    }

    //Configure logic for each attribute in the output tile
    List<BiConsumer<Contexts.FeaturePostMatch, Feature>> processors = new ArrayList<>();
    for (var attribute : feature.attributes()) {
      processors.add(attributeProcessor(attribute));
    }
    processors.add(makeFeatureProcessor(feature.minZoom(), Integer.class, Feature::setMinZoom));
    processors.add(makeFeatureProcessor(feature.maxZoom(), Integer.class, Feature::setMaxZoom));

    addPostProcessingImplications(layer, feature, processors, rootContext);
    
    // per-feature tolerance settings should take precedence over defaults from post-processing config
    processors.add(makeFeatureProcessor(feature.tolerance(), Double.class, Feature::setPixelTolerance));
    processors.add(makeFeatureProcessor(feature.toleranceAtMaxZoom(), Double.class, Feature::setPixelToleranceAtMaxZoom));

    featureProcessors = processors.stream().filter(Objects::nonNull).toList();
  }

  /** Consider implications of Post Processing on the feature's processors **/
  private void addPostProcessingImplications(FeatureLayer layer, FeatureItem feature,
    List<BiConsumer<Contexts.FeaturePostMatch, Feature>> processors,
    Contexts.Root rootContext) {
    var postProcess = layer.postProcess();

    // Consider min_size and min_size_at_max_zoom
    if (postProcess == null) {
      processors.add(makeFeatureProcessor(feature.minSize(), Double.class, Feature::setMinPixelSize));
      processors.add(makeFeatureProcessor(feature.minSizeAtMaxZoom(), Double.class, Feature::setMinPixelSizeAtMaxZoom));
      return;
    }
    // In order for Post-processing to receive all features, the default MinPixelSize* are zero when features are collected
    processors.add(makeFeatureProcessor(Objects.requireNonNullElse(feature.minSize(),0), Double.class, Feature::setMinPixelSize));
    processors.add(makeFeatureProcessor(Objects.requireNonNullElse(feature.minSizeAtMaxZoom(),0), Double.class, Feature::setMinPixelSizeAtMaxZoom));
    // Implications of tile_post_process.merge_line_strings
    var mergeLineStrings = postProcess.mergeLineStrings();
    if (mergeLineStrings != null) {
      processors.add(makeLineFeatureProcessor(mergeLineStrings.tolerance(),Feature::setPixelTolerance));
      processors.add(makeLineFeatureProcessor(mergeLineStrings.toleranceAtMaxZoom(),Feature::setPixelToleranceAtMaxZoom));
      // postProcess.mergeLineStrings.minLength* and postProcess.mergeLineStrings.buffer
      var bufferPixels = maxIgnoringNulls(mergeLineStrings.minLength(), mergeLineStrings.buffer());
      var bufferPixelsAtMaxZoom = maxIgnoringNulls(mergeLineStrings.minLengthAtMaxZoom(), mergeLineStrings.buffer());
      int maxZoom = rootContext.config().maxzoomForRendering();
      if (bufferPixels != null || bufferPixelsAtMaxZoom != null) {
        processors.add((context, f) -> {
          if (f.isLine()) {
            f.setBufferPixelOverrides(z -> z == maxZoom ? bufferPixelsAtMaxZoom : bufferPixels);
          }
        });
      }

    }
    // Implications of tile_post_process.merge_polygons
    var mergePolygons = postProcess.mergePolygons();
    if (mergePolygons != null) {
      // postProcess.mergePolygons.tolerance*
      processors.add(makePolygonFeatureProcessor(mergePolygons.tolerance(),Feature::setPixelTolerance));
      processors.add(makePolygonFeatureProcessor(mergePolygons.toleranceAtMaxZoom(),Feature::setPixelToleranceAtMaxZoom));
      // TODO: postProcess.mergeLineStrings.minArea*
    }
  }

  private <T> BiConsumer<Contexts.FeaturePostMatch, Feature> makeFeatureProcessor(Object input, Class<T> clazz,
    BiConsumer<Feature, T> consumer) {
    if (input == null) {
      return null;
    }
    var expression = ConfigExpressionParser.parse(
      input,
      tagValueProducer,
      featurePostMatchContext,
      clazz
    );
    if (expression.equals(constOf(null))) {
      return null;
    }
    return (context, feature) -> {
      var result = expression.apply(context);
      if (result != null) {
        consumer.accept(feature, result);
      }
    };
  }

  private BiConsumer<Contexts.FeaturePostMatch, Feature> makeLineFeatureProcessor(Double input,
    ObjDoubleConsumer<Feature> consumer) {
    if (input == null) {
      return null;
    }
    return (context, feature) -> {
      if (feature.isLine()) {
        consumer.accept(feature, input);
      }
    };
  }

  private BiConsumer<Contexts.FeaturePostMatch, Feature> makePolygonFeatureProcessor(Double input,
    ObjDoubleConsumer<Feature> consumer) {
    if (input == null) {
      return null;
    }
    return (context, feature) -> {
      if (feature.isPolygon()) {
        consumer.accept(feature, input);
      }
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
   * Produces logic that generates attribute values based on configuration and input data. If both a constantValue
   * configuration and a tagValue configuration are set, this is likely a mistake, and the constantValue will take
   * precedence.
   *
   * @param attribute - attribute definition configured from YML
   * @return a function that generates an attribute value from a {@link SourceFeature} based on an attribute
   *         configuration.
   */
  private Function<Contexts.FeaturePostMatch, Object> attributeValueProducer(AttributeDefinition attribute) {
    Object type = attribute.type();

    // some expression features are hoisted to the top-level for attribute values for brevity,
    // so just map them to what the equivalent expression syntax would be and parse as an expression.
    Map<String, Object> value = new HashMap<>();
    if ("match_key".equals(type)) {
      value.put("value", "${match_key}");
    } else if ("match_value".equals(type)) {
      value.put("value", "${match_value}");
    } else {
      if (type != null) {
        value.put("type", type);
      }
      if (attribute.coalesce() != null) {
        value.put("coalesce", attribute.coalesce());
      } else if (attribute.value() != null) {
        value.put("value", attribute.value());
      } else if (attribute.tagValue() != null) {
        value.put("tag_value", attribute.tagValue());
      } else if (attribute.argValue() != null) {
        value.put("arg_value", attribute.argValue());
      } else {
        value.put("tag_value", attribute.key());
      }
    }

    return ConfigExpressionParser.parse(value, tagValueProducer, featurePostMatchContext, Object.class);
  }

  /**
   * Generate logic which determines the minimum zoom level for a feature based on a configured pixel size limit.
   *
   * @param minTilePercent - minimum percentage of a tile that a feature must cover to be shown
   * @param rawMinZoom     - global minimum zoom for this feature, or an expression providing the min zoom dynamically
   * @param minZoomByValue - map of tag values to zoom level
   * @return minimum zoom function
   */
  private Function<Contexts.FeatureAttribute, Integer> attributeZoomThreshold(
    Double minTilePercent, Object rawMinZoom, Map<Object, Integer> minZoomByValue) {

    var result = ConfigExpressionParser.parse(rawMinZoom, tagValueProducer,
      featureAttributeContext, Integer.class);

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

  /**
   * Generates a function which produces a fully-configured attribute for a feature.
   *
   * @param attribute - configuration for this attribute
   * @return processing logic
   */
  private BiConsumer<Contexts.FeaturePostMatch, Feature> attributeProcessor(AttributeDefinition attribute) {
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
          BooleanExpressionParser.parse(attrIncludeWhen, tagValueProducer,
            featurePostMatchContext),
        attrExcludeWhen == null ? Expression.TRUE :
          not(BooleanExpressionParser.parse(attrExcludeWhen, tagValueProducer,
            featurePostMatchContext))
      ).simplify();

    var minTileCoverage = attrIncludeWhen == null ? null : attribute.minTileCoverSize();

    Function<Contexts.FeatureAttribute, Integer> attributeZoomProducer =
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
          Integer minzoom = attributeZoomProducer.apply(context.createAttrZoomContext(value));
          if (minzoom != null) {
            f.setAttrWithMinzoom(tagKey, value, minzoom);
          } else {
            f.setAttr(tagKey, value);
          }
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
  public void processFeature(Contexts.FeaturePostMatch context, FeatureCollector features) {
    var sourceFeature = context.feature();

    // Ensure that this feature is from the correct source (index should enforce this, so just check when assertions enabled)
    assert sources.isEmpty() || sources.contains(sourceFeature.getSource());

    // Special handling for relation_members geometry
    if (isRelationMembers && sourceFeature instanceof RelationSourceFeature relationFeature) {
      processRelationMembers(context, relationFeature, features);
      return;
    }

    var f = geometryFactory.apply(features);
    for (var processor : featureProcessors) {
      processor.accept(context, f);
    }
  }
  
  /**
   * Process relation members for relation_members geometry type.
   * Iterates over relation members, filters them, and creates features for each qualifying member.
   */
  private void processRelationMembers(Contexts.FeaturePostMatch context, RelationSourceFeature relationFeature,
    FeatureCollector features) {
    var relation = relationFeature.relation();
    var relationPostMatch = context;
    
    Set<Long> processedRefs = new HashSet<>();
    
    for (var member : relation.members()) {
      if (shouldProcessMember(member, processedRefs, relationFeature, relationPostMatch)) {
          try {
          createMemberFeature(member, getMemberContext(member, relationFeature, relationPostMatch), 
            relationPostMatch, features);
        } catch (Exception e) {
          org.slf4j.LoggerFactory.getLogger(ConfiguredFeature.class)
            .warn("Error creating feature for relation member {} in relation {}: {}", 
              member.ref(), relation.id(), e.getMessage());
        }
      }
    }
  }
  
  /**
   * Check if a relation member should be processed based on filters.
   * Returns true if the member should be processed, false otherwise.
   */
  private boolean shouldProcessMember(OsmElement.Relation.Member member, Set<Long> processedRefs,
    RelationSourceFeature relationFeature, Contexts.FeaturePostMatch relationPostMatch) {
    if (!processedRefs.add(member.ref())) {
      return false;
    }
    
    String memberTypeStr = member.type().name().toLowerCase();
    if (!memberTypes.contains(memberTypeStr)) {
      return false;
    }
    
    // Nested relations are not supported
    if (member.type() == OsmElement.Type.RELATION) {
      return false;
    }
    
    if (memberRoles != null) {
      String role = member.role();
      // Empty string matches members with no role
      if (!memberRoles.contains(role) && !(role.isEmpty() && memberRoles.contains(""))) {
        return false;
      }
    }
    
    Map<String, Object> memberTags = getMemberTags(member, relationFeature);
    
    var memberContextInstance = new Contexts.MemberContext(
      relationPostMatch,
      memberTags,
      member.role(),
      memberTypeStr,
      member.ref()
    );
    
    if (!memberIncludeWhen.evaluate(memberContextInstance, new ArrayList<>())) {
      return false;
    }
    
    if (memberExcludeWhen.evaluate(memberContextInstance, new ArrayList<>())) {
      return false;
    }
    
    return true;
  }
  
  /**
   * Create a member context for expression evaluation.
   */
  private Contexts.MemberContext getMemberContext(OsmElement.Relation.Member member,
    RelationSourceFeature relationFeature, Contexts.FeaturePostMatch relationPostMatch) {
    Map<String, Object> memberTags = getMemberTags(member, relationFeature);
    String memberTypeStr = member.type().name().toLowerCase();
    return new Contexts.MemberContext(
      relationPostMatch,
      memberTags,
      member.role(),
      memberTypeStr,
      member.ref()
    );
  }
  
  /**
   * Get tags for a relation member from the stored member data.
   */
  private Map<String, Object> getMemberTags(OsmElement.Relation.Member member, 
    RelationSourceFeature relationFeature) {
    RelationMemberDataProvider dataProvider = relationFeature.memberDataProvider();
    if (dataProvider == null) {
      return Map.of();
    }
    
    if (member.type() == OsmElement.Type.WAY) {
      Map<String, Object> tags = dataProvider.getWayTags(member.ref());
      return tags != null ? tags : Map.of();
    } else if (member.type() == OsmElement.Type.NODE) {
      Map<String, Object> tags = dataProvider.getNodeTags(member.ref());
      return tags != null ? tags : Map.of();
    }
    
    return Map.of();
  }
  
  /**
   * Create a feature for a relation member with actual geometry.
   */
  private void createMemberFeature(OsmElement.Relation.Member member,
    Contexts.MemberContext memberContext,
    Contexts.FeaturePostMatch relationContext,
    FeatureCollector features) {
    
    RelationSourceFeature relationFeature = (RelationSourceFeature) relationContext.feature();
    RelationMemberDataProvider dataProvider = relationFeature.memberDataProvider();
    
    if (dataProvider == null) {
      // No data provider available - skip this member
      return;
    }
    
    FeatureCollector.Feature memberFeature;
    Geometry geometry = null;
    
    if (member.type() == OsmElement.Type.NODE) {
      org.locationtech.jts.geom.Coordinate coord = dataProvider.getNodeCoordinate(member.ref());
      if (coord == null) {
        return;
      }
      geometry = GeoUtils.JTS_FACTORY.createPoint(coord);
      memberFeature = features.geometry(layerId, geometry);
      
    } else if (member.type() == OsmElement.Type.WAY) {
      LongArrayList nodeIds = dataProvider.getWayGeometry(member.ref());
      if (nodeIds == null || nodeIds.isEmpty()) {
        return;
      }
      
      CoordinateSequence coords = buildCoordinateSequence(nodeIds, dataProvider);
      if (coords == null || coords.size() < 2) {
        return;
      }
      
      // Determine if closed way should be polygon or line based on area tag and geometry
      boolean closed = coords.size() > 1 && 
        coords.getCoordinate(0).equals(coords.getCoordinate(coords.size() - 1));
      Map<String, Object> wayTags = dataProvider.getWayTags(member.ref());
      String area = wayTags != null ? (String) wayTags.get("area") : null;
      boolean canBePolygon = closed && !"no".equals(area) && coords.size() >= 4;
      
      if (canBePolygon) {
        geometry = GeoUtils.JTS_FACTORY.createPolygon(coords);
        memberFeature = features.geometry(layerId, geometry);
      } else {
        geometry = GeoUtils.JTS_FACTORY.createLineString(coords);
        memberFeature = features.geometry(layerId, geometry);
      }
      
    } else {
      // Nested relations are not supported
      return;
    }
    
    for (var processor : featureProcessors) {
      processor.accept(relationContext, memberFeature);
    }
    
    if (memberAttributeProcessors != null) {
      for (var processor : memberAttributeProcessors) {
        processor.accept(memberContext, memberFeature);
      }
    }
    
    // Generate unique feature ID: relation ID * 1M + member ref to avoid collisions
    long uniqueId = relationContext.feature().id() * 1000000L + member.ref();
    memberFeature.setId(uniqueId);
  }
  
  /**
   * Build a coordinate sequence from node IDs using the data provider.
   */
  private CoordinateSequence buildCoordinateSequence(LongArrayList nodeIds, 
    RelationMemberDataProvider dataProvider) {
    org.locationtech.jts.geom.Coordinate[] coords = new org.locationtech.jts.geom.Coordinate[nodeIds.size()];
    int validCount = 0;
    
    for (int i = 0; i < nodeIds.size(); i++) {
      org.locationtech.jts.geom.Coordinate coord = dataProvider.getNodeCoordinate(nodeIds.get(i));
      if (coord != null) {
        coords[validCount++] = coord;
      } else {
        return null;
      }
    }
    
    if (validCount < 2) {
      return null;
    }
    
    // Defensive: trim array if coordinates were missing (shouldn't happen in practice)
    if (validCount < coords.length) {
      org.locationtech.jts.geom.Coordinate[] trimmed = new org.locationtech.jts.geom.Coordinate[validCount];
      System.arraycopy(coords, 0, trimmed, 0, validCount);
      coords = trimmed;
    }
    
    return new CoordinateArraySequence(coords);
  }
  
  /**
   * Generate logic which processes member-level attributes.
   */
  private BiConsumer<Contexts.MemberContext, Feature> memberAttributeProcessor(AttributeDefinition attribute) {
    var tagKey = attribute.key();
    var attributeValueProducer = memberAttributeValueProducer(attribute);
    var fallback = attribute.fallback();
    
    var attrIncludeWhen = attribute.includeWhen();
    var attrExcludeWhen = attribute.excludeWhen();
    
    var attributeTest = Expression.and(
      attrIncludeWhen == null ? Expression.TRUE :
        BooleanExpressionParser.parse(attrIncludeWhen, tagValueProducer, memberContext),
      attrExcludeWhen == null ? Expression.TRUE :
        Expression.not(BooleanExpressionParser.parse(attrExcludeWhen, tagValueProducer, memberContext))
    ).simplify();
    
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
        f.setAttr(tagKey, value);
      }
    };
  }
  
  /**
   * Produces logic that generates attribute values for member attributes.
   */
  private Function<Contexts.MemberContext, Object> memberAttributeValueProducer(AttributeDefinition attribute) {
    Object type = attribute.type();
    
    Map<String, Object> value = new HashMap<>();
    if ("match_key".equals(type)) {
      value.put("value", "${match_key}");
    } else if ("match_value".equals(type)) {
      value.put("value", "${match_value}");
    } else {
      if (type != null) {
        value.put("type", type);
      }
      if (attribute.coalesce() != null) {
        value.put("coalesce", attribute.coalesce());
      } else if (attribute.value() != null) {
        value.put("value", attribute.value());
      } else if (attribute.tagValue() != null) {
        value.put("tag_value", attribute.tagValue());
      } else if (attribute.argValue() != null) {
        value.put("arg_value", attribute.argValue());
      } else {
        value.put("tag_value", attribute.key());
      }
    }
    
    return ConfigExpressionParser.parse(value, tagValueProducer, memberContext, Object.class);
  }

  private Double maxIgnoringNulls(Double a, Double b) {
    if (a == null) return b;
    if (b == null) return a;
    return Double.max(a, b);
  }
}
