package com.onthegomap.planetiler.archive;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_ABSENT;
import static com.onthegomap.planetiler.util.Format.joinCoordinates;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.util.BuildInfo;
import com.onthegomap.planetiler.util.LayerStats;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Envelope;

/** Controls information that {@link TileArchiveWriter} will write to the archive metadata. */
public record TileArchiveMetadata(
  @JsonProperty(NAME_KEY) String name,
  @JsonProperty(DESCRIPTION_KEY) String description,
  @JsonProperty(ATTRIBUTION_KEY) String attribution,
  @JsonProperty(VERSION_KEY) String version,
  @JsonProperty(TYPE_KEY) String type,
  @JsonProperty(FORMAT_KEY) String format,
  @JsonIgnore Envelope bounds,
  @JsonIgnore CoordinateXY center,
  @JsonProperty(ZOOM_KEY) Double zoom,
  @JsonProperty(MINZOOM_KEY) Integer minzoom,
  @JsonProperty(MAXZOOM_KEY) Integer maxzoom,
  @JsonIgnore List<LayerStats.VectorLayer> vectorLayers,
  @JsonAnyGetter Map<String, String> others
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

  public static final String MVT_FORMAT = "pbf";

  public TileArchiveMetadata(Profile profile, PlanetilerConfig config) {
    this(profile, config, null);
  }

  public TileArchiveMetadata(Profile profile, PlanetilerConfig config, List<LayerStats.VectorLayer> vectorLayers) {
    this(
      getString(config, NAME_KEY, profile.name()),
      getString(config, DESCRIPTION_KEY, profile.description()),
      getString(config, ATTRIBUTION_KEY, profile.attribution()),
      getString(config, VERSION_KEY, profile.version()),
      getString(config, TYPE_KEY, profile.isOverlay() ? "overlay" : "baselayer"),
      getString(config, FORMAT_KEY, MVT_FORMAT),
      config.bounds().latLon(),
      new CoordinateXY(config.bounds().latLon().centre()),
      GeoUtils.getZoomFromLonLatBounds(config.bounds().latLon()),
      config.minzoom(),
      config.maxzoom(),
      vectorLayers,
      mapWithBuildInfo()
    );
  }

  private static String getString(PlanetilerConfig config, String key, String fallback) {
    return config.arguments()
      .getString("output_" + key + "|mbtiles_" + key, "'" + key + "' attribute for tileset metadata", fallback);
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

  public TileArchiveMetadata set(String key, Object value) {
    if (key != null && value != null) {
      others.put(key, value.toString());
    }
    return this;
  }
  private static final ObjectMapper mapper = new ObjectMapper()
    .registerModules(new Jdk8Module())
    .setSerializationInclusion(NON_ABSENT);

  public Map<String, String> toMap() {
    Map<String, String> result = new LinkedHashMap<>(mapper.convertValue(this, new TypeReference<>() {}));
    if (bounds != null) {
      result.put(BOUNDS_KEY, joinCoordinates(bounds.getMinX(), bounds.getMinY(), bounds.getMaxX(), bounds.getMaxY()));
    }
    if (center != null) {
      result.put(CENTER_KEY, joinCoordinates(center.getX(), center.getY()));
    }
    if (vectorLayers != null) {
      try {
        result.put(VECTOR_LAYERS_KEY, mapper.writeValueAsString(vectorLayers));
      } catch (JsonProcessingException e) {
        // ok
      }
    }
    return result;
  }

  public TileArchiveMetadata withLayerStats(List<LayerStats.VectorLayer> layerStats) {
    return new TileArchiveMetadata(name, description, attribution, version, type, format, bounds, center, zoom, minzoom,
      maxzoom, layerStats, others);
  }
}
