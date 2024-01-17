package com.onthegomap.planetiler.archive;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.util.BuildInfo;
import com.onthegomap.planetiler.util.LayerAttrStats;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metadata associated with a tile archive.
 * <p>
 * The default (de-)serialization corresponds to the
 * <a href="https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md#metadata">mbtiles spec</a>. As such each
 * value is a string.
 */
public record TileArchiveMetadata(
  @JsonProperty(NAME_KEY) String name,
  @JsonProperty(DESCRIPTION_KEY) String description,
  @JsonProperty(ATTRIBUTION_KEY) String attribution,
  @JsonProperty(VERSION_KEY) String version,
  @JsonProperty(TYPE_KEY) String type,
  @JsonProperty(FORMAT_KEY) String format,
  @JsonSerialize(using = TileArchiveMetadataDeSer.EnvelopeSerializer.class)
  @JsonDeserialize(using = TileArchiveMetadataDeSer.EnvelopeDeserializer.class) Envelope bounds,
  @JsonSerialize(using = TileArchiveMetadataDeSer.CoordinateSerializer.class)
  @JsonDeserialize(using = TileArchiveMetadataDeSer.CoordinateDeserializer.class) Coordinate center,
  @JsonProperty(MINZOOM_KEY)
  @JsonSerialize(using = ToStringSerializer.class) Integer minzoom,
  @JsonProperty(MAXZOOM_KEY)
  @JsonSerialize(using = ToStringSerializer.class) Integer maxzoom,
  @JsonProperty(JSON_KEY)
  @JsonSerialize(using = TileArchiveMetadataDeSer.MetadataJsonSerializer.class)
  @JsonDeserialize(using = TileArchiveMetadataDeSer.MetadataJsonDeserializer.class) TileArchiveMetadataJson json,
  @JsonAnyGetter
  @JsonDeserialize(using = TileArchiveMetadataDeSer.EmptyMapIfNullDeserializer.class) Map<String, String> others,
  @JsonProperty(COMPRESSION_KEY) TileCompression tileCompression
) {

  public static final String NAME_KEY = "name";
  public static final String DESCRIPTION_KEY = "description";
  public static final String ATTRIBUTION_KEY = "attribution";
  public static final String VERSION_KEY = "version";
  public static final String TYPE_KEY = "type";
  public static final String FORMAT_KEY = "format";
  public static final String BOUNDS_KEY = "bounds";
  public static final String CENTER_KEY = "center";
  public static final String ZOOM_KEY = "zoom";
  public static final String MINZOOM_KEY = "minzoom";
  public static final String MAXZOOM_KEY = "maxzoom";
  public static final String VECTOR_LAYERS_KEY = "vector_layers";
  public static final String COMPRESSION_KEY = "compression";

  public static final String JSON_KEY = "json";

  public static final String MVT_FORMAT = "pbf";

  private static final Logger LOGGER = LoggerFactory.getLogger(TileArchiveMetadata.class);

  public TileArchiveMetadata(Profile profile, PlanetilerConfig config) {
    this(profile, config, null);
  }

  public TileArchiveMetadata(Profile profile, PlanetilerConfig config, List<LayerAttrStats.VectorLayer> vectorLayers) {
    this(
      getString(config, NAME_KEY, profile.name()),
      getString(config, DESCRIPTION_KEY, profile.description()),
      getString(config, ATTRIBUTION_KEY, profile.attribution()),
      getString(config, VERSION_KEY, profile.version()),
      getString(config, TYPE_KEY, profile.isOverlay() ? "overlay" : "baselayer"),
      getString(config, FORMAT_KEY, MVT_FORMAT),
      config.bounds().latLon(),
      new Coordinate(
        config.bounds().latLon().centre().getX(),
        config.bounds().latLon().centre().getY(),
        GeoUtils.getZoomFromLonLatBounds(config.bounds().latLon())
      ),
      config.minzoom(),
      config.maxzoom(),
      vectorLayers == null ? null : new TileArchiveMetadataJson(vectorLayers),
      mergeMaps(mapWithBuildInfo(),profile.extraArchiveMetadata()),
      config.tileCompression()
    );
  }

  // just used for the "internal map"-serialization - ignored by default
  @JsonIgnore
  @JsonProperty(ZOOM_KEY)
  public Double zoom() {
    if (center == null) {
      return null;
    }
    final double z = center.getZ();
    return Double.isNaN(z) ? null : z;
  }

  // just used for the "internal map"-serialization - ignored by default
  @JsonIgnore
  @JsonProperty(VECTOR_LAYERS_KEY)
  public List<LayerAttrStats.VectorLayer> vectorLayers() {
    return json == null ? null : json.vectorLayers;
  }

  private static String getString(PlanetilerConfig config, String key, String fallback) {
    return config.arguments()
      .getString("archive_" + key + "|mbtiles_" + key, "'" + key + "' attribute for tileset metadata", fallback);
  }

  private static Map<String, String> mapWithBuildInfo() {
    Map<String, String> result = new LinkedHashMap<>();
    var buildInfo = BuildInfo.get();
    if (buildInfo != null) {
      if (buildInfo.version() != null) {
        result.put("planetiler:version", buildInfo.version());
      }
      if (buildInfo.githash() != null) {
        result.put("planetiler:githash", buildInfo.githash());
      }
      if (buildInfo.buildTimeString() != null) {
        result.put("planetiler:buildtime", buildInfo.buildTimeString());
      }
    }
    return result;
  }

  /** Sets an extra metadata entry in {@link #others}. */
  public TileArchiveMetadata setExtraMetadata(String key, Object value) {
    if (key != null && value != null) {
      others.put(key, value.toString());
    }
    return this;
  }

  /**
   * Returns a map with all key-value pairs from this metadata entry, including {@link #others} hoisted to top-level
   * keys.
   */
  public Map<String, String> toMap() {
    final JsonMapper mapper = TileArchiveMetadataDeSer.internalMapMapper();
    return new LinkedHashMap<>(mapper.convertValue(this, new TypeReference<>() {}));
  }

  /** Returns a copy of this instance with {@link #json} set to {@code layerStats}. */
  public TileArchiveMetadata withLayerStats(List<LayerAttrStats.VectorLayer> layerStats) {
    return withJson(json == null ? TileArchiveMetadataJson.create(layerStats) : json.withLayers(layerStats));
  }

  /**
   * Returns a copy of this instance with {@link #json}'s {@link TileArchiveMetadataJson#vectorLayers()} set to
   * {@code layerStats}.
   */
  public TileArchiveMetadata withJson(TileArchiveMetadataJson json) {
    return new TileArchiveMetadata(name, description, attribution, version, type, format, bounds, center, minzoom,
      maxzoom, json, others, tileCompression);
  }

  /*
   * few workarounds to make collect unknown fields to others work,
   * because @JsonAnySetter does not yet work on constructor/creator arguments
   * https://github.com/FasterXML/jackson-databind/issues/3439
   */

  private static Map<String,String> mergeMaps(Map<String,String> m1, Map<String,String> m2) {
    var result = new TreeMap<>(m1);
    result.putAll(m2);
    return result;
  }

  @JsonAnySetter
  private void putUnknownFieldsToOthers(String name, String value) {
    others.put(name, value);
  }


  public record TileArchiveMetadataJson(
    @JsonProperty(VECTOR_LAYERS_KEY) List<LayerAttrStats.VectorLayer> vectorLayers
  ) {
    public TileArchiveMetadataJson withLayers(List<LayerAttrStats.VectorLayer> vectorLayers) {
      return TileArchiveMetadataJson.create(vectorLayers);
    }

    public static TileArchiveMetadataJson create(List<LayerAttrStats.VectorLayer> vectorLayers) {
      return vectorLayers == null ? null : new TileArchiveMetadataJson(vectorLayers);
    }
  }
}
