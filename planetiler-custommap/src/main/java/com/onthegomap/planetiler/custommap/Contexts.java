package com.onthegomap.planetiler.custommap;

import com.onthegomap.planetiler.custommap.expression.ScriptContext;
import com.onthegomap.planetiler.custommap.expression.ScriptEnvironment;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.WithGeometryType;
import com.onthegomap.planetiler.reader.WithTags;
import java.util.List;
import java.util.Map;
import org.projectnessie.cel.checker.Decls;
import org.projectnessie.cel.common.types.NullT;

/**
 * Wrapper objects that provide all available inputs to different parts of planetiler schema configs at runtime.
 * <p>
 * Contexts provide inputs to java code, and also global variable definitions to CEL expressions. Contexts are nested so
 * that all global variables from a parent context are also available to its child context.
 */
public class Contexts {

  private static Object wrapNullable(Object nullable) {
    return nullable == null ? NullT.NullValue : nullable;
  }

  public static Root root() {
    return new Root();
  }

  /**
   * Root context available everywhere in a planetiler schema config.
   */
  public record Root() implements ScriptContext {

    // TODO add argument parsing
    public static final ScriptEnvironment<Root> DESCRIPTION =
      ScriptEnvironment.root().forInput(Root.class);

    @Override
    public Object apply(String input) {
      return null;
    }
  }

  /**
   * Makes nested contexts adhere to {@link WithTags} and {@link WithGeometryType} by recursively fetching source
   * feature from the root context.
   */
  private interface FeatureContext extends ScriptContext, WithTags, WithGeometryType {
    default FeatureContext parent() {
      return null;
    }

    default SourceFeature feature() {
      return parent().feature();
    }

    @Override
    default Map<String, Object> tags() {
      return feature().tags();
    }

    @Override
    default TagValueProducer tagValueProducer() {
      var parent = parent();
      return parent == null ? TagValueProducer.EMPTY : parent.tagValueProducer();
    }

    @Override
    default boolean isPoint() {
      return feature().isPoint();
    }

    @Override
    default boolean canBeLine() {
      return feature().canBeLine();
    }

    @Override
    default boolean canBePolygon() {
      return feature().canBePolygon();
    }
  }

  /**
   * Context available when processing an input feature.
   *
   * @param feature          The input feature being processed
   * @param tagValueProducer Common parsing for input feature tags
   */
  public record ProcessFeature(@Override SourceFeature feature, @Override TagValueProducer tagValueProducer)
    implements FeatureContext {

    private static final String FEATURE_TAGS = "feature.tags";
    private static final String FEATURE_ID = "feature.id";
    private static final String FEATURE_SOURCE = "feature.source";
    private static final String FEATURE_SOURCE_LAYER = "feature.source_layer";

    public static final ScriptEnvironment<ProcessFeature> DESCRIPTION = ScriptEnvironment.root()
      .forInput(ProcessFeature.class)
      .withDeclarations(
        Decls.newVar(FEATURE_TAGS, Decls.newMapType(Decls.String, Decls.Any)),
        Decls.newVar(FEATURE_ID, Decls.Int),
        Decls.newVar(FEATURE_SOURCE, Decls.String),
        Decls.newVar(FEATURE_SOURCE_LAYER, Decls.String)
      );

    @Override
    public Object apply(String key) {
      if (key != null) {
        return switch (key) {
          case FEATURE_TAGS -> tagValueProducer.mapTags(feature);
          case FEATURE_ID -> feature.id();
          case FEATURE_SOURCE -> feature.getSource();
          case FEATURE_SOURCE_LAYER -> wrapNullable(feature.getSourceLayer());
          default -> null;
        };
      } else {
        return null;
      }
    }

    public FeaturePostMatch createPostMatchContext(List<String> matchKeys) {
      return new FeaturePostMatch(this, matchKeys);
    }

  }

  /**
   * Context available after a feature has been matched.
   *
   * Adds {@code match_key} and {@code match_value} variables that capture which tag key/value caused the feature to be
   * included.
   *
   * @param parent    The parent context
   * @param matchKeys Keys that triggered the match
   */
  public record FeaturePostMatch(@Override ProcessFeature parent, List<String> matchKeys) implements FeatureContext {

    private static final String MATCH_KEY = "match_key";
    private static final String MATCH_VALUE = "match_value";

    public static final ScriptEnvironment<FeaturePostMatch> DESCRIPTION = ProcessFeature.DESCRIPTION
      .forInput(FeaturePostMatch.class)
      .withDeclarations(
        Decls.newVar(MATCH_KEY, Decls.String),
        Decls.newVar(MATCH_VALUE, Decls.Any)
      );

    @Override
    public Object apply(String key) {
      if (key != null) {
        return switch (key) {
          case MATCH_KEY -> wrapNullable(matchKey());
          case MATCH_VALUE -> wrapNullable(matchValue());
          default -> parent.apply(key);
        };
      } else {
        return null;
      }
    }

    public String matchKey() {
      return matchKeys().isEmpty() ? null : matchKeys().get(0);
    }

    public Object matchValue() {
      String matchKey = matchKey();
      return matchKey == null ? null : parent.tagValueProducer.valueForKey(parent().feature(), matchKey);
    }

    public FeatureAttribute createAttrZoomContext(Object value) {
      return new FeatureAttribute(this, value);
    }

  }

  /**
   * Context available when configuring an attribute on an output feature after its value has been assigned (for example
   * setting min/max zoom).
   *
   * @param parent The parent context
   * @param value  Value of the attribute
   */
  public record FeatureAttribute(@Override FeaturePostMatch parent, Object value) implements FeatureContext {
    private static final String VALUE = "value";
    public static final ScriptEnvironment<FeatureAttribute> DESCRIPTION = FeaturePostMatch.DESCRIPTION
      .forInput(FeatureAttribute.class)
      .withDeclarations(Decls.newVar(VALUE, Decls.Any));

    @Override
    public Object apply(String key) {
      return VALUE.equals(key) ? wrapNullable(value) : parent.apply(key);
    }
  }
}
