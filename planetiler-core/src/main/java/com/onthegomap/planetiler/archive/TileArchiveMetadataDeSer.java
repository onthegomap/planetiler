package com.onthegomap.planetiler.archive;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_ABSENT;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.onthegomap.planetiler.util.Format;
import com.onthegomap.planetiler.util.LayerAttrStats;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Envelope;

/**
 * Container for everything related to (de-)serialization of {@link TileArchiveMetadata}
 */
public final class TileArchiveMetadataDeSer {

  private TileArchiveMetadataDeSer() {}

  private static final JsonMapper internalMapMapper = newBaseBuilder()
    .addMixIn(TileArchiveMetadata.class, InternalMapMixin.class)
    .build();

  private static final JsonMapper mbtilesMapper = newBaseBuilder()
    .build();

  public static JsonMapper internalMapMapper() {
    return internalMapMapper;
  }

  public static JsonMapper mbtilesMapper() {
    return mbtilesMapper;
  }

  public static JsonMapper.Builder newBaseBuilder() {
    return JsonMapper.builder()
      .addModule(new Jdk8Module())
      .serializationInclusion(NON_ABSENT);
  }

  public record InternalMapMixin(
    @JsonIgnore(true) TileArchiveMetadata.TileArchiveMetadataJson json,
    @JsonIgnore(false)
    @JsonSerialize(using = VectorLayersToStringSerializer.class) List<LayerAttrStats.VectorLayer> vectorLayers,
    @JsonIgnore(false) Double zoom,
    @JsonSerialize(using = CoordinateXYSerializer.class) Coordinate center
  ) {}

  public record StrictDeserializationMixin(
    @StrictDeserialization Coordinate center,
    @StrictDeserialization Envelope bounds
  ) {}

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.ANNOTATION_TYPE, ElementType.METHOD, ElementType.CONSTRUCTOR, ElementType.FIELD})
  public @interface StrictDeserialization {
  }

  private static boolean isStrictDeserialization(BeanProperty property) {
    return Optional.ofNullable(property.getAnnotation(StrictDeserialization.class))
      .or(() -> Optional.ofNullable(property.getContextAnnotation(StrictDeserialization.class)))
      .map(a -> Boolean.TRUE)
      .orElse(false);
  }

  private static void serializeEscapedJson(Object value, JsonGenerator gen) throws IOException {
    final ObjectCodec codec = gen.getCodec();
    final StringWriter writer = new StringWriter();
    final JsonGenerator subGen = gen.getCodec().getFactory().createGenerator(writer);
    codec.writeValue(subGen, value);
    final String escapedJson = writer.toString();
    gen.writeString(escapedJson);
  }

  private static Optional<List<Double>> doubleListFromCommaList(String commaList, int minItems, int maxItems,
    boolean strict) {
    final String[] splits = commaList.split(",");
    if (splits.length < minItems) {
      if (strict) {
        throw new IllegalArgumentException("expected at least " + minItems + " doubles");
      } else {
        return Optional.empty();
      }
    } else if (splits.length > 3 && strict) {
      throw new IllegalArgumentException("expected at most " + maxItems + " doubles");
    }
    return Optional.of(Arrays.stream(splits)
      .limit(maxItems)
      .map(Double::parseDouble)
      .toList());
  }

  static class EnvelopeSerializer extends JsonSerializer<Envelope> {

    @Override
    public void serialize(Envelope v, JsonGenerator gen, SerializerProvider provider) throws IOException {
      gen.writeString(Format.joinCoordinates(v.getMinX(), v.getMinY(), v.getMaxX(), v.getMaxY()));
    }
  }

  static class EnvelopeDeserializer extends JsonDeserializer<Envelope> implements ContextualDeserializer {

    private boolean strict = false;

    @Override
    public Envelope deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      var dsOption = doubleListFromCommaList(p.getValueAsString(), 4, 4, strict);
      if (dsOption.isEmpty()) {
        return null;
      }
      final List<Double> ds = dsOption.get();
      final double minX = ds.get(0);
      final double maxX = ds.get(2);
      final double minY = ds.get(1);
      final double maxY = ds.get(3);
      return new Envelope(minX, maxX, minY, maxY);
    }

    @Override
    public JsonDeserializer<?> createContextual(DeserializationContext ctxt, BeanProperty property)
      throws JsonMappingException {

      strict = isStrictDeserialization(property);
      return this;
    }
  }


  static class CoordinateSerializer extends JsonSerializer<Coordinate> {

    @Override
    public void serialize(Coordinate v, JsonGenerator gen, SerializerProvider provider) throws IOException {
      if (Double.isNaN(v.getZ())) {
        gen.writeString(Format.joinCoordinates(v.getX(), v.getY()));
      } else {
        gen.writeString(Format.joinCoordinates(v.getX(), v.getY(), Math.ceil(v.getZ())));
      }
    }
  }

  static class CoordinateXYSerializer extends JsonSerializer<Coordinate> {
    @Override
    public void serialize(Coordinate v, JsonGenerator gen, SerializerProvider provider) throws IOException {
      gen.writeString(Format.joinCoordinates(v.getX(), v.getY()));
    }
  }

  static class CoordinateDeserializer extends JsonDeserializer<Coordinate> implements ContextualDeserializer {

    boolean strict = false;

    @Override
    public Coordinate deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      var dsOption = doubleListFromCommaList(p.getValueAsString(), 2, 3, strict);
      if (dsOption.isEmpty()) {
        return null;
      }
      final List<Double> ds = dsOption.get();
      if (ds.size() == 2) {
        return new CoordinateXY(ds.get(0), ds.get(1));
      } else {
        return new Coordinate(ds.get(0), ds.get(1), ds.get(2));
      }
    }

    @Override
    public JsonDeserializer<?> createContextual(DeserializationContext ctxt, BeanProperty property)
      throws JsonMappingException {

      strict = isStrictDeserialization(property);
      return this;
    }
  }

  static class MetadataJsonDeserializer extends JsonDeserializer<TileArchiveMetadata.TileArchiveMetadataJson> {
    @Override
    public TileArchiveMetadata.TileArchiveMetadataJson deserialize(JsonParser p, DeserializationContext ctxt)
      throws IOException {

      try (JsonParser parser = p.getCodec().getFactory().createParser(p.getValueAsString())) {
        return parser.readValueAs(TileArchiveMetadata.TileArchiveMetadataJson.class);
      }
    }
  }

  static class MetadataJsonSerializer extends JsonSerializer<TileArchiveMetadata.TileArchiveMetadataJson> {
    @Override
    public void serialize(TileArchiveMetadata.TileArchiveMetadataJson value, JsonGenerator gen,
      SerializerProvider serializers) throws IOException {

      serializeEscapedJson(value, gen);
    }
  }

  static class VectorLayersToStringSerializer extends JsonSerializer<List<LayerAttrStats.VectorLayer>> {
    @Override
    public void serialize(List<LayerAttrStats.VectorLayer> value, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {

      serializeEscapedJson(value, gen);
    }
  }

  static class EmptyMapIfNullDeserializer extends JsonDeserializer<Map<String, String>> {
    @SuppressWarnings("unchecked")
    @Override
    public Map<String, String> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      return p.readValueAs(HashMap.class);
    }

    @Override
    public Map<String, String> getNullValue(DeserializationContext ctxt) {
      return new HashMap<>();
    }
  }
}
