package com.onthegomap.planetiler.custommap.expression;

import com.onthegomap.planetiler.custommap.TagValueProducer;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.WithGeometryType;
import com.onthegomap.planetiler.reader.WithTags;
import java.util.List;
import java.util.Map;
import org.projectnessie.cel.checker.Decls;
import org.projectnessie.cel.common.types.NullT;

/**
 * Wrapper objects that provide all available inputs to different parts of planetiler schema configs.
 *
 * Contexts provide inputs to java code, and also global variable definitions to CEL expressions. Contexts are nested so
 * that all global variables from a parent context are also available to its child context.
 */
public class Contexts {

  private static Object wrapNullable(Object nullable) {
    return nullable == null ? NullT.NullValue : nullable;
  }

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
  public record ProcessFeature(@Override SourceFeature feature, TagValueProducer tagValueProducer)
    implements FeatureContext {

    private static final String FEATURE_TAGS = "feature.tags";
    private static final String FEATURE_ID = "feature.id";
    private static final String FEATURE_SOURCE = "feature.source";
    private static final String FEATURE_SOURCE_LAYER = "feature.source_layer";

    public static final ScriptContextDescription<ProcessFeature> DESCRIPTION = ScriptContextDescription.root()
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
          case FEATURE_TAGS -> feature.tags();
          case FEATURE_ID -> feature.id();
          case FEATURE_SOURCE -> feature.getSource();
          case FEATURE_SOURCE_LAYER -> wrapNullable(feature.getSourceLayer());
          default -> null;
        };
      } else {
        return null;
      }
    }

    public PostMatch createPostMatchContext(List<String> matchKeys) {
      return new PostMatch(this, matchKeys);
    }

    /**
     * Context available after a feature has been matched.
     *
     * Adds {@code match_key} and {@code match_value} variables that capture which tag key/value caused the feature to
     * be included.
     *
     * @param parent    The parent context
     * @param matchKeys Keys that triggered the match
     */
    public record PostMatch(@Override ProcessFeature parent, List<String> matchKeys) implements FeatureContext {

      private static final String MATCH_KEY = "match_key";
      private static final String MATCH_VALUE = "match_value";

      public static final ScriptContextDescription<PostMatch> DESCRIPTION = ProcessFeature.DESCRIPTION
        .forInput(PostMatch.class)
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

      public AttrZoom createAttrZoomContext(Object value) {
        return new AttrZoom(this, value);
      }

      /**
       * Context available when processing a feature, after the value for an attribute has been assigned, for example to
       * control it's min_zoom.
       *
       * @param parent The parent context
       * @param value  Value of the attribute
       */
      public record AttrZoom(@Override PostMatch parent, Object value) implements FeatureContext {
        private static final String VALUE = "value";
        public static final ScriptContextDescription<AttrZoom> DESCRIPTION = PostMatch.DESCRIPTION
          .forInput(AttrZoom.class)
          .withDeclarations(Decls.newVar(VALUE, Decls.Any));

        @Override
        public Object apply(String key) {
          if (key != null) {
            return switch (key) {
              case VALUE -> wrapNullable(value);
              default -> parent.apply(key);
            };
          } else {
            return null;
          }
        }
      }
    }
  }
}
