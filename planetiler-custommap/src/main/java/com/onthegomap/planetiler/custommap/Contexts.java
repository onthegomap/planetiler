package com.onthegomap.planetiler.custommap;

import com.google.api.expr.v1alpha1.Constant;
import com.google.api.expr.v1alpha1.Decl;
import com.google.api.expr.v1alpha1.Type;
import com.google.common.collect.ForwardingMap;
import com.google.protobuf.NullValue;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.custommap.expression.ParseException;
import com.onthegomap.planetiler.custommap.expression.ScriptContext;
import com.onthegomap.planetiler.custommap.expression.ScriptEnvironment;
import com.onthegomap.planetiler.custommap.expression.stdlib.GeometryVal;
import com.onthegomap.planetiler.expression.DataType;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.WithGeometryType;
import com.onthegomap.planetiler.reader.WithSource;
import com.onthegomap.planetiler.reader.WithSourceLayer;
import com.onthegomap.planetiler.reader.WithTags;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmSourceFeature;
import com.onthegomap.planetiler.util.Try;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.projectnessie.cel.checker.Decls;
import org.projectnessie.cel.common.types.NullT;
import org.projectnessie.cel.common.types.pb.ProtoTypeRegistry;
import org.projectnessie.cel.common.types.ref.TypeAdapter;
import org.projectnessie.cel.common.types.ref.Val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper objects that provide all available inputs to different parts of planetiler schema configs at runtime.
 * <p>
 * Contexts provide inputs to java code, and also global variable definitions to CEL expressions. Contexts are nested so
 * that all global variables from a parent context are also available to its child context.
 */
public class Contexts {
  private static final Logger LOGGER = LoggerFactory.getLogger(Contexts.class);

  private static Object wrapNullable(Object nullable) {
    return nullable == null ? NullT.NullValue : nullable;
  }

  public static Root emptyRoot() {
    return new Root(Arguments.of().silence(), Map.of());
  }

  /**
   * Returns a {@link Root} context built from {@code schemaArgs} argument definitions and {@code origArguments}
   * arguments provided from the command-line/environment.
   * <p>
   * Arguments may depend on the value of other arguments so this iteratively evaluates the arguments until their values
   * settle.
   *
   * @throws ParseException if the argument definitions are malformed, or if there's an infinite loop
   */
  public static Contexts.Root buildRootContext(Arguments origArguments, Map<String, Object> schemaArgs) {
    boolean loggingEnabled = !origArguments.silenced();
    origArguments.silence();
    Map<String, String> argDescriptions = new LinkedHashMap<>();
    Map<String, Object> unparsedSchemaArgs = new HashMap<>(schemaArgs);
    Map<String, Object> parsedSchemaArgs = new HashMap<>(origArguments.toMap());
    Contexts.Root result = new Root(origArguments, parsedSchemaArgs);
    Arguments arguments = origArguments;
    int iters = 0;
    // arguments may reference the value of other arguments, so continue parsing until they all succeed...
    while (!unparsedSchemaArgs.isEmpty()) {
      final var root = result;
      final var args = arguments;
      Map<String, Exception> failures = new HashMap<>();

      Map.copyOf(unparsedSchemaArgs).forEach((key, value) -> {
        boolean builtin = root.builtInArgs.contains(key);
        String description;
        Object defaultValueObject;
        DataType type = null;
        if (value instanceof Map<?, ?> map) {
          if (builtin) {
            throw new ParseException("Cannot override built-in argument: " + key);
          }
          var typeObject = map.get("type");
          if (typeObject != null) {
            type = DataType.from(Objects.toString(typeObject));
          }
          var descriptionObject = map.get("description");
          description = descriptionObject == null ? "no description provided" : descriptionObject.toString();
          defaultValueObject = map.get("default");
          if (type != null) {
            var fromArgs = args.getString(key, description, null);
            if (fromArgs != null) {
              parsedSchemaArgs.put(key, type.convertFrom(fromArgs));
            }
          }
        } else {
          defaultValueObject = value;
          description = builtin ? root.builtInArgs.getString(key, description, null) : "no description provided";
        }
        argDescriptions.put(key, description);
        Try<Object> defaultValue = ConfigExpressionParser.tryStaticEvaluate(root, defaultValueObject, Object.class);
        if (defaultValue.isSuccess()) {
          Object raw = defaultValue.get();
          String asString = Objects.toString(raw);
          if (type == null) {
            type = DataType.typeOf(raw);
          }
          var stringResult = args.getString(key, description, asString);
          Object castedResult = type.convertFrom(stringResult);
          if (stringResult == null) {
            throw new ParseException("Missing required parameter: " + key + "(" + description + ")");
          } else if (castedResult == null) {
            throw new ParseException("Cannot convert value for " + key + " to " + type.id() + ": " + stringResult);
          }
          parsedSchemaArgs.put(key, castedResult);
          unparsedSchemaArgs.remove(key);
        } else {
          failures.put(key, defaultValue.exception());
        }
      });

      arguments = origArguments.orElse(Arguments.of(parsedSchemaArgs.entrySet().stream().collect(Collectors.toMap(
        Map.Entry::getKey,
        e -> Objects.toString(e.getValue()))
      )));
      result = new Root(arguments, parsedSchemaArgs);
      if (iters++ > 100) {
        failures
          .forEach(
            (key, failure) -> LOGGER.error("Error computing {}:\n{}", key,
              ExceptionUtils.getRootCause(failure).toString().indent(4)));
        throw new ParseException("Infinite loop while processing arguments: " + unparsedSchemaArgs.keySet());
      }
    }
    var finalArguments = loggingEnabled ? arguments.withExactlyOnceLogging() : arguments.silence();
    if (loggingEnabled) {
      argDescriptions.forEach((key, description) -> finalArguments.getString(key, description, null));
    }
    return new Root(finalArguments, parsedSchemaArgs);
  }

  /**
   * Root context available everywhere in a planetiler schema config.
   * <p>
   * Holds argument values parsed from the schema config and command-line args.
   */
  public static final class Root implements ScriptContext {
    private static final TypeAdapter TYPE_ADAPTER = ProtoTypeRegistry.newRegistry();
    private final Arguments arguments;
    private final PlanetilerConfig config;
    private final ScriptEnvironment<Root> description;
    private final Map<String, Val> bindings = new HashMap<>();
    private final Map<String, Object> argumentValues = new HashMap<>();
    public final Set<String> builtInArgs;

    public Arguments arguments() {
      return arguments;
    }

    public PlanetilerConfig config() {
      return config;
    }

    @Override
    public boolean equals(Object o) {
      return this == o || (o instanceof Root root && argumentValues.equals(root.argumentValues));
    }

    @Override
    public int hashCode() {
      return Objects.hash(argumentValues);
    }

    @Override
    public Object argument(String key) {
      return argumentValues.get(key);
    }

    private Root(Arguments arguments, Map<String, Object> schemaArgs) {
      this.arguments = arguments;
      this.config = PlanetilerConfig.from(arguments);
      argumentValues.put("threads", config.threads());
      argumentValues.put("write_threads", config.featureWriteThreads());
      argumentValues.put("process_threads", config.featureProcessThreads());
      argumentValues.put("feature_read_threads", config.featureReadThreads());
      //      args.put("loginterval", config.logInterval());
      argumentValues.put("minzoom", config.minzoom());
      argumentValues.put("maxzoom", config.maxzoom());
      argumentValues.put("render_maxzoom", config.maxzoomForRendering());
      argumentValues.put("force", config.force());
      argumentValues.put("compress_temp", config.compressTempStorage());
      argumentValues.put("mmap_temp", config.mmapTempStorage());
      argumentValues.put("sort_max_readers", config.sortMaxReaders());
      argumentValues.put("sort_max_writers", config.sortMaxWriters());
      argumentValues.put("nodemap_type", config.nodeMapType());
      argumentValues.put("nodemap_storage", config.nodeMapStorage());
      argumentValues.put("nodemap_madvise", config.nodeMapMadvise());
      argumentValues.put("multipolygon_geometry_storage", config.multipolygonGeometryStorage());
      argumentValues.put("multipolygon_geometry_madvise", config.multipolygonGeometryMadvise());
      argumentValues.put("http_user_agent", config.httpUserAgent());
      //      args.put("http_timeout", config.httpTimeout());
      argumentValues.put("http_retries", config.httpRetries());
      argumentValues.put("download_chunk_size_mb", config.downloadChunkSizeMB());
      argumentValues.put("download_threads", config.downloadThreads());
      argumentValues.put("download_max_bandwidth", config.downloadMaxBandwidth());
      argumentValues.put("min_feature_size_at_max_zoom", config.minFeatureSizeAtMaxZoom());
      argumentValues.put("min_feature_size", config.minFeatureSizeBelowMaxZoom());
      argumentValues.put("simplify_tolerance_at_max_zoom", config.simplifyToleranceAtMaxZoom());
      argumentValues.put("simplify_tolerance", config.simplifyToleranceBelowMaxZoom());
      argumentValues.put("skip_filled_tiles", config.skipFilledTiles());
      argumentValues.put("tile_warning_size_mb", config.tileWarningSizeBytes());
      builtInArgs = Set.copyOf(argumentValues.keySet());

      schemaArgs.forEach(argumentValues::putIfAbsent);
      config.arguments().toMap().forEach(argumentValues::putIfAbsent);

      argumentValues.forEach((k, v) -> bindings.put("args." + k, TYPE_ADAPTER.nativeToValue(v)));
      bindings.put("args", TYPE_ADAPTER.nativeToValue(this.argumentValues));

      description = ScriptEnvironment.root(this).forInput(Root.class)
        .withDeclarations(
          argumentValues.entrySet().stream()
            .map(entry -> decl(entry.getKey(), entry.getValue()))
            .toList()
        ).withDeclarations(
          Decls.newVar("args", Decls.newMapType(Decls.String, Decls.Any))
        );
    }

    private Decl decl(String name, Object value) {
      Type type;
      var builder = Constant.newBuilder();
      if (value instanceof String s) {
        builder.setStringValue(s);
        type = Decls.String;
      } else if (value instanceof Boolean b) {
        builder.setBoolValue(b);
        type = Decls.Bool;
      } else if (value instanceof Long || value instanceof Integer) {
        builder.setInt64Value(((Number) value).longValue());
        type = Decls.Int;
      } else if (value instanceof Double || value instanceof Float) {
        builder.setDoubleValue(((Number) value).doubleValue());
        type = Decls.Double;
      } else if (value == null) {
        builder.setNullValue(NullValue.NULL_VALUE);
        type = Decls.Null;
      } else {
        throw new IllegalArgumentException(
          "Unrecognized constant type: " + value + " (" + value.getClass().getName() + ")");
      }
      return Decls.newConst("args." + name, type, builder.build());
    }

    public ScriptEnvironment<Root> description() {
      return description;
    }

    @Override
    public Object apply(String input) {
      return bindings.get(input);
    }

    public ProcessFeature createProcessFeatureContext(SourceFeature sourceFeature, TagValueProducer tagValueProducer) {
      return new ProcessFeature(this, sourceFeature, tagValueProducer);
    }
  }

  private interface NestedContext extends ScriptContext {

    default Root root() {
      return null;
    }

    @Override
    default Object argument(String key) {
      return root().argument(key);
    }
  }

  /**
   * Makes nested contexts adhere to {@link WithTags} and {@link WithGeometryType} by recursively fetching source
   * feature from the root context.
   */
  private interface FeatureContext extends ScriptContext, WithTags, WithGeometryType, NestedContext, WithSourceLayer,
    WithSource {

    default FeatureContext parent() {
      return null;
    }

    default SourceFeature feature() {
      return parent().feature();
    }

    @Override
    default Root root() {
      return parent().root();
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

    @Override
    default String getSource() {
      return feature().getSource();
    }

    @Override
    default String getSourceLayer() {
      return feature().getSourceLayer();
    }
  }

  /**
   * Context available when processing an input feature.
   *
   * @param feature          The input feature being processed
   * @param tagValueProducer Common parsing for input feature tags
   */
  public record ProcessFeature(
    @Override Root root, @Override SourceFeature feature,
    @Override TagValueProducer tagValueProducer
  )
    implements FeatureContext {

    private static final String FEATURE_TAGS = "feature.tags";
    private static final String FEATURE_ID = "feature.id";
    private static final String FEATURE_SOURCE = "feature.source";
    private static final String FEATURE_SOURCE_LAYER = "feature.source_layer";
    private static final String FEATURE_OSM_CHANGESET = "feature.osm_changeset";
    private static final String FEATURE_OSM_VERSION = "feature.osm_version";
    private static final String FEATURE_OSM_TIMESTAMP = "feature.osm_timestamp";
    private static final String FEATURE_OSM_USER_ID = "feature.osm_user_id";
    private static final String FEATURE_OSM_USER_NAME = "feature.osm_user_name";
    private static final String FEATURE_OSM_TYPE = "feature.osm_type";
    private static final String FEATURE_GEOMETRY = "feature";

    public static ScriptEnvironment<ProcessFeature> description(Root root) {
      return root.description()
        .forInput(ProcessFeature.class)
        .withDeclarations(
          Decls.newVar(FEATURE_TAGS, Decls.newMapType(Decls.String, Decls.Any)),
          Decls.newVar(FEATURE_ID, Decls.Int),
          Decls.newVar(FEATURE_SOURCE, Decls.String),
          Decls.newVar(FEATURE_SOURCE_LAYER, Decls.String),
          Decls.newVar(FEATURE_OSM_CHANGESET, Decls.Int),
          Decls.newVar(FEATURE_OSM_VERSION, Decls.Int),
          Decls.newVar(FEATURE_OSM_TIMESTAMP, Decls.Int),
          Decls.newVar(FEATURE_OSM_USER_ID, Decls.Int),
          Decls.newVar(FEATURE_OSM_USER_NAME, Decls.String),
          Decls.newVar(FEATURE_OSM_TYPE, Decls.String),
          Decls.newVar(FEATURE_GEOMETRY, GeometryVal.PROTO_TYPE)
        );
    }

    @Override
    public Object apply(String key) {
      if (key != null) {
        return switch (key) {
          case FEATURE_TAGS -> mapWithDefault(tagValueProducer.mapTags(feature), NullValue.NULL_VALUE);
          case FEATURE_ID -> feature.id();
          case FEATURE_SOURCE -> feature.getSource();
          case FEATURE_SOURCE_LAYER -> wrapNullable(feature.getSourceLayer());
          case FEATURE_GEOMETRY -> new GeometryVal(feature);
          default -> {
            OsmElement elem = feature instanceof OsmSourceFeature osm ? osm.originalElement() : null;
            if (FEATURE_OSM_TYPE.equals(key)) {
              yield elem == null ? null : elem.type().name().toLowerCase();
            }
            OsmElement.Info info = elem != null ? elem.info() : null;
            yield info == null ? null : switch (key) {
              case FEATURE_OSM_CHANGESET -> info.changeset();
              case FEATURE_OSM_VERSION -> info.version();
              case FEATURE_OSM_TIMESTAMP -> info.timestamp();
              case FEATURE_OSM_USER_ID -> info.userId();
              case FEATURE_OSM_USER_NAME -> wrapNullable(info.user());
              default -> null;
            };
          }
        };
      } else {
        return null;
      }
    }

    private static <K, V> Map<K, V> mapWithDefault(Map<K, V> map, Object nullValue) {
      return new ForwardingMap<>() {
        @Override
        protected Map<K, V> delegate() {
          return map;
        }

        @Override
        public V get(Object key) {
          return map.getOrDefault(key, (V) nullValue);
        }
      };
    }

    public FeaturePostMatch createPostMatchContext(List<String> matchKeys) {
      return new FeaturePostMatch(this, matchKeys);
    }

  }

  /**
   * Context available after a feature has been matched.
   * <p>
   * Adds {@code match_key} and {@code match_value} variables that capture which tag key/value caused the feature to be
   * included.
   *
   * @param parent    The parent context
   * @param matchKeys Keys that triggered the match
   */
  public record FeaturePostMatch(@Override ProcessFeature parent, List<String> matchKeys) implements FeatureContext {

    private static final String MATCH_KEY = "match_key";
    private static final String MATCH_VALUE = "match_value";

    public static ScriptEnvironment<FeaturePostMatch> description(Root root) {
      return ProcessFeature.description(root)
        .forInput(FeaturePostMatch.class)
        .withDeclarations(
          Decls.newVar(MATCH_KEY, Decls.String),
          Decls.newVar(MATCH_VALUE, Decls.Any)
        );
    }

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
      return matchKeys().isEmpty() ? null : matchKeys().getFirst();
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

    public static ScriptEnvironment<FeatureAttribute> description(Root root) {
      return FeaturePostMatch.description(root)
        .forInput(FeatureAttribute.class)
        .withDeclarations(Decls.newVar(VALUE, Decls.Any));
    }

    @Override
    public Object apply(String key) {
      return VALUE.equals(key) ? wrapNullable(value) : parent.apply(key);
    }
  }
}
