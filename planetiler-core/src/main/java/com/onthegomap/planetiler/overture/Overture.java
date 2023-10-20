package com.onthegomap.planetiler.overture;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Range;
import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureMerge;
import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.parquet.AvroParquetFeature;
import com.onthegomap.planetiler.util.Downloader;
import com.onthegomap.planetiler.util.ZoomFunction;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Overture implements Profile {

  private static final Logger LOGGER = LoggerFactory.getLogger(Overture.class);

  private final boolean connectors;
  private final boolean metadata;
  private final boolean ids;
  private final PlanetilerConfig config;
  private final boolean splitRoads;
  private final boolean sources;

  Overture(PlanetilerConfig config) {
    this.config = config;
    this.connectors = config.arguments().getBoolean("connectors", "include connectors", true);
    this.splitRoads =
      config.arguments().getBoolean("split_roads", "split roads based on \"at\" ranges on tag values", false);
    this.metadata =
      config.arguments().getBoolean("metadata", "include element metadata (version, update time)", false);
    this.ids =
      config.arguments().getBoolean("ids", "include ids on output features", true);
    this.sources =
      config.arguments().getBoolean("sources", "include sources on output features", true);
  }

  public static void main(String[] args) throws Exception {
    var base = Path.of("data", "sources", "overture");
    var arguments = Arguments.fromEnvOrArgs(args);
    var sample = arguments.getBoolean("sample", "only download smallest file from parquet source", false);
    var release = arguments.getString("release", "overture release", "2023-10-19-alpha.0");

    var pt = Planetiler.create(arguments)
      .addAvroParquetSource("overture", base)
      .setProfile(planetiler -> new Overture(planetiler.config()))
      .overwriteOutput(Path.of("data", "output.pmtiles"));

    if (arguments.getBoolean("download", "download overture files", true)) {
      downloadFiles(base, pt, release, sample);
    }

    pt.run();
  }

  private static void downloadFiles(Path base, Planetiler pt, String release, boolean sample) {
    var d = Downloader.create(pt.config(), pt.stats());
    var urls = sample ?
      OvertureUrls.sampleSmallest(pt.config(), "release/" + release) :
      OvertureUrls.getAll(pt.config(), "release/" + release);
    for (var url : urls) {
      String s = url.replaceAll("^.*" + Pattern.quote(release + "/"), "");
      var p = base.resolve(s);
      d.add(s, "https://overturemaps-us-west-2.s3.amazonaws.com/release/" + release + "/" + s, p);
    }
    var begin = pt.stats().startStage("download");
    d.run();
    begin.stop();
  }

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
    if (sourceFeature instanceof AvroParquetFeature avroFeature) {
      switch (sourceFeature.getSourceLayer()) {
        case "admins/administrativeBoundary" -> processAdministrativeBoundary(avroFeature, features);
        case "admins/locality" -> processLocality(avroFeature, features);
        case "buildings/building" -> processBuilding(avroFeature, features);
        case "places/place" -> processPlace(avroFeature, features);
        case "transportation/connector" -> processConnector(avroFeature, features);
        case "transportation/segment" -> processSegment(avroFeature, features);
      }
    }
  }

  @Override
  public List<VectorTile.Feature> postProcessLayerFeatures(String layer, int zoom, List<VectorTile.Feature> items)
    throws GeometryException {
    if (zoom >= 14) {
      return items;
    }
    double tolerance = config.tolerance(zoom);
    if (layer.equals("admins/administrativeBoundary")) {
      return FeatureMerge.mergeLineStrings(items, 0, tolerance, 4, true);
    } else if (layer.equals("transportation/segment")) {
      return FeatureMerge.mergeLineStrings(items, 0.25, tolerance, 4, true);
    }
    return items;
  }

  @Override
  public String name() {
    return "Overture";
  }

  @Override
  public String attribution() {
    return """
      <a href="https://www.openstreetmap.org/copyright" target="_blank">&copy; OpenStreetMap</a>
      <a href="https://overturemaps.org/download/overture-july-alpha-release-notes/" target="_blank">&copy; Overture Foundation</a>
      """
      .replaceAll("\n", " ")
      .trim();
  }


  private void processSegment(AvroParquetFeature sourceFeature, FeatureCollector features) {
    Struct struct = sourceFeature.getStruct();
    var subtype = struct.get("subtype").as(OvertureSchema.SegmentSubType.class);
    var roadClass = struct.get("road", "class").as(OvertureSchema.RoadClass.class);
    if (roadClass == null) {
      LOGGER.warn("Invalid road class: {}", struct.get("road", "class"));
      return;
    }
    if (subtype == null) {
      LOGGER.warn("Invalid subType: {}", struct.get("subtype"));
      return;
    }
    int minzoom = switch (subtype) {
      case ROAD -> switch (roadClass) {
          case MOTORWAY -> 4;
          case TRUNK -> 5;
          case PRIMARY -> 7;
          case SECONDARY -> 9;
          case TERTIARY -> 11;
          case RESIDENTIAL -> 12;
          case LIVINGSTREET -> 13;
          case UNCLASSIFIED -> 14;
          case PARKINGAISLE -> 14;
          case DRIVEWAY -> 14;
          case PEDESTRIAN -> 14;
          case FOOTWAY -> 14;
          case STEPS -> 14;
          case TRACK -> 14;
          case CYCLEWAY -> 14;
          case BRIDLEWAY -> 14;
          case UNKNOWN -> 14;
        };
      case RAIL -> 8;
      case WATER -> 10;
    };
    var commonTags = getCommonTags(struct);
    commonTags.put("subType", struct.get("subtype").asString());
    if (connectors) {
      commonTags.put("connectors", ZoomFunction.minZoom(14, join(",", struct.get("connectors"))));
    }

    Struct road = struct.get("road");
    if (road.isNull()) {
      features.line(sourceFeature.getSourceLayer())
        .setMinZoom(minzoom)
        .setMinPixelSize(0)
        .putAttrs(commonTags);
    } else {
      commonTags.put("class", roadClass.toString());
      if (splitRoads) {
        RangeMapMap tags = parseRoadPartials(road);
        try {
          var lineSplitter = new LineSplitter(sourceFeature.worldGeometry());
          for (var range : tags.result()) {
            var attrs = range.value();
            var splitLine = lineSplitter.get(range.start(), range.end());
            features.geometry(sourceFeature.getSourceLayer(), splitLine)
              .setMinZoom(attrs.containsKey("flags.isLink") ? Math.max(minzoom, 9) : minzoom)
              .setMinPixelSize(0)
              .putAttrs(attrs)
              .putAttrs(commonTags)
              .setAttr("restrictions.turns", road.get("restrictions", "turns").asJson());
          }
        } catch (GeometryException e) {
          LOGGER.error("Error splitting road {}", sourceFeature, e);
        }
      } else {
        var feature = features.line(sourceFeature.getSourceLayer())
          .setMinZoom(minzoom)
          .setMinPixelSize(0)
          .putAttrs(commonTags);
        if (road.get("flags").asList().stream().map(Struct::asString).anyMatch("isLink"::equals)) {
          feature.setMinZoom(Math.max(minzoom, 9));
        }
        List<Struct> names = road.get("roadNames").asList();
        Optional<Struct> fullLengthName = names.stream()
          .filter(d -> d.get("at").isNull())
          .findFirst();
        List<Struct> otherNames = names.stream().filter(d -> !d.get("at").isNull()).toList();
        feature
          .putAttrs(fullLengthName.map(Overture::getNames).orElse(Map.of()))
          .setAttr("roadNames", toJsonString(otherNames))
          .setAttr("flags", road.get("flags").asJson())
          .setAttr("lanes", road.get("lanes").asJson())
          .setAttr("surface", road.get("surface").asJson())
          .setAttr("restrictions", road.get("restrictions").asJson());
      }
    }
  }

  static RangeMapMap parseRoadPartials(Struct road) {
    RangeMapMap tags = new RangeMapMap();
    for (var flag : extractPartials(road.get("flags"))) {
      tags.put(flag.at, Map.of("flags." + flag.value, ZoomFunction.minZoom(9, 1)));
    }
    for (var surface : extractPartials(road.get("surface"))) {
      tags.put(surface.at, Map.of("surface", ZoomFunction.minZoom(9, surface.value)));
    }
    Struct lanes = road.get("lanes");
    if (lanes.isNull()) {
      // skip
    } else if (lanes.get(0).get("at").isNull()) {
      tags.put(FULL_LENGTH, Map.of("lanes", ZoomFunction.minZoom(9, lanes.asJson())));
    } else {
      for (var item : lanes.asList()) {
        tags.put(getRangeFromAt(item),
          Map.of("lanes", ZoomFunction.minZoom(9, item.get("value").orElse(item.get("values")).asJson())));
      }
    }
    for (var name : road.get("roadNames").asList()) {
      tags.put(getRangeFromAt(name), getNames(name));
    }

    for (var limits : road.get("restrictions", "speedLimits").asList()) {
      Range<Double> range = getRangeFromAt(limits);
      Map<String, Object> attrs = new HashMap<>();
      var max = limits.get("maxSpeed");
      if (!max.isNull()) {
        attrs.put("restrictions.speedLimits.maxSpeed",
          ZoomFunction.minZoom(9, max.get(0).asString() + max.get(1).asString()));
      }
      var min = limits.get("minSpeed");
      if (!min.isNull()) {
        attrs.put("restrictions.speedLimits.minSpeed",
          ZoomFunction.minZoom(9, min.get(0).asString() + min.get(1).asString()));
      }
      if (Boolean.TRUE.equals(limits.get("isMaxSpeedVariable").asBoolean())) {
        attrs.put("restrictions.speedLimits.isMaxSpeedVariable", ZoomFunction.minZoom(9, 1));
      }
      tags.put(range, attrs);
    }

    for (var restriction : road.get("restrictions", "access").asList()) {
      // TODO string (allowed/denied)
      // TODO allowed/denied/designated: details (possibly at)
      if (!restriction.isStruct()) {
        tags.put(FULL_LENGTH, Map.of("restrictions.access." + restriction.asString(), ZoomFunction.minZoom(9, 1)));
      } else {
        {
          var allowed = restriction.get("allowed");
          if (!allowed.isNull()) {
            tags.put(getRangeFromAt(allowed),
              Map.of("restrictions.access.allowed", ZoomFunction.minZoom(9, processAccessRestriction(allowed))));
          }
        }
        {
          var designated = restriction.get("designated");
          if (!designated.isNull()) {
            tags.put(getRangeFromAt(designated),
              Map.of("restrictions.access.designated", ZoomFunction.minZoom(9, processAccessRestriction(designated))));
          }
        }
        {
          var denied = restriction.get("denied");
          if (!denied.isNull()) {
            tags.put(getRangeFromAt(denied),
              Map.of("restrictions.access.denied", ZoomFunction.minZoom(9, processAccessRestriction(denied))));
          }
        }
      }
    }
    return tags;
  }

  private static Object processAccessRestriction(Struct allowed) {
    var result = allowed.without("at").asJson();
    return "{}".equals(result) ? 1 : result;
  }

  private static Range<Double> getRangeFromAt(Struct struct) {
    Range<Double> range = FULL_LENGTH;
    Struct at = struct.get("at");
    if (!at.isNull()) {
      Double lo = at.get(0).asDouble();
      Double hi = at.get(1).asDouble();
      range = Range.closedOpen(lo, hi);
    }
    return range;
  }

  private static List<Partial<String>> extractPartials(Struct flagStruct) {
    List<Partial<String>> flags = new ArrayList<>();
    for (var flag : flagStruct.asList()) {
      Range<Double> range = getRangeFromAt(flag);
      for (var value : flag.get("value").orElse(flag.get("values").orElse(flag)).asList()) {
        flags.add(new Partial<>(value.asString(), range));
      }
    }
    return flags;
  }

  private static final Range<Double> FULL_LENGTH = Range.closedOpen(0.0, 1.0);

  record Partial<T> (T value, Range<Double> at) {}

  private void processConnector(AvroParquetFeature sourceFeature, FeatureCollector features) {
    if (connectors) {
      Struct struct = sourceFeature.getStruct();
      features.point(sourceFeature.getSourceLayer())
        .setMinZoom(14)
        .putAttrs(getCommonTags(struct));
    }
  }

  private void processPlace(AvroParquetFeature sourceFeature, FeatureCollector features) {
    Struct struct = sourceFeature.getStruct();
    features.point(sourceFeature.getSourceLayer())
      .setMinZoom(14)
      .setAttr("emails", join(",", struct.get("emails")))
      .setAttr("phones", join(",", struct.get("phones")))
      .setAttr("socials", join(",", struct.get("socials")))
      .setAttr("websites", join(",", struct.get("websites")))
      .setAttr("confidence", struct.get("confidence").asDouble())
      .setAttr("brand.wikidata", struct.get("brand", "wikidata").asString())
      .putAttrs(getNames("brand", struct.get("brand", "names")))
      .setAttr("addresses", formatAddress(struct.get("addresses")))
      .setAttr("categories.main", struct.get("categories", "main").asString())
      .setAttr("categories.alternate", join(",", struct.get("categories", "alternate")))
      .putAttrs(getCommonTags(struct))
      .putAttrs(getNames(struct.get("names")));
  }

  private static String formatAddress(Struct addresses) {
    StringBuilder result = new StringBuilder();
    for (var address : addresses.asList()) {
      if (!result.isEmpty()) {
        result.append("; ");
      }
      result.append(address.get("freeform").asString());
      var locality = address.get("locality");
      var postCode = address.get("postCode");
      var region = address.get("region");
      var country = address.get("country");
      if (!locality.isNull()) {
        result.append(", ").append(locality.asString());
      }
      if (!postCode.isNull()) {
        result.append(", ").append(postCode.asString());
      }
      if (!region.isNull()) {
        result.append(", ").append(region.asString());
      }
      if (!country.isNull()) {
        result.append(", ").append(country.asString());
      }
    }
    return result.toString();
  }

  private static String toJsonString(List<?> list) {
    try {
      return list == null || list.isEmpty() ? null : Struct.mapper.writeValueAsString(list);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static String join(String sep, Struct struct) {
    List<Struct> items = struct.asList();
    if (items.isEmpty()) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    for (Struct item : items) {
      if (sb.length() > 0) {
        sb.append(sep);
      }
      sb.append(item.asString());
    }
    return sb.toString();
  }

  private void processBuilding(AvroParquetFeature sourceFeature, FeatureCollector features) {
    if (sourceFeature.canBePolygon()) {
      Struct struct = sourceFeature.getStruct();
      var commonTags = getCommonTags(struct);
      commonTags.put("class", struct.get("class").asString());
      commonTags.put("height", struct.get("height").asDouble());
      commonTags.put("numFloors", struct.get("numfloors").asInt());
      features.polygon(sourceFeature.getSourceLayer())
        .setMinZoom(13)
        .setMinPixelSize(2)
        .putAttrs(commonTags);
      var names = getNames(struct.get("names"));
      if (!names.isEmpty()) {
        features.centroidIfConvex(sourceFeature.getSourceLayer())
          .setMinZoom(14)
          .putAttrs(names)
          .putAttrs(commonTags);
      }
    }
  }

  private void processLocality(AvroParquetFeature sourceFeature, FeatureCollector features) {
    if (sourceFeature.canBePolygon()) {
      Struct struct = sourceFeature.getStruct();
      features.innermostPoint(sourceFeature.getSourceLayer())
        .setMinZoom(adminLevelMinZoom(struct.get("adminlevel").asInt()))
        .putAttrs(getNames(struct.get("names")))
        .setAttr("adminLevel", struct.get("adminlevel").asInt())
        .setAttr("subType", struct.get("subtype").asString())
        .setAttr("isoCountryCodeAlpha2", struct.get("isocountrycodealpha2").asString())
        .setAttr("isoSubCountryCode", struct.get("isosubcountrycode").asString())
        .putAttrs(getCommonTags(struct));
    }
  }

  private static Map<String, Object> getNames(Struct names) {
    return getNames(null, names);
  }

  private static Map<String, Object> getNames(String prefix, Struct names) {
    if (names.isNull()) {
      return Map.of();
    }
    String base = prefix == null ? "name" : (prefix + ".name");
    Map<String, Object> result = new LinkedHashMap<>();
    boolean first = true;
    for (String key : List.of("common", "official", "short", "alternate")) {
      for (var name : names.get(key).asList()) {
        String value = name.get("value").asString();
        if (value != null) {
          if (first) {
            first = false;
            put(result, "name", value);
          }
          put(result, base + "." + key + "." + name.get("language").asString(), value);
        }
      }
    }
    return result;
  }

  private static void put(Map<String, Object> attrs, String key, Object value) {
    int n = 1;
    String result = key;
    while (attrs.containsKey(result)) {
      result = key + "." + (++n);
    }
    attrs.put(result, value);
  }

  private Map<String, Object> getCommonTags(Struct info) {
    Map<String, Object> results = new HashMap<>(4);
    if (metadata) {
      results.put("version", info.get("version").asInt());
      results.put("updateTime", Instant.ofEpochMilli(info.get("updatetime").asLong()).toString());
    }
    if (ids) {
      results.put("id", ZoomFunction.minZoom(14, info.get("id").asString()));
    }
    if (sources) {
      results.put("sources", info.get("sources").asList().stream().map(d -> {
        String recordId = d.get("recordId").asString();
        if (recordId == null) {
          recordId = d.get("recordid").asString();
        }
        return d.get("dataset").asString() + (recordId == null ? "" : (":" + recordId));
      }).sorted().distinct().collect(Collectors.joining(",")));
    }
    return results;
  }

  private void processAdministrativeBoundary(AvroParquetFeature sourceFeature, FeatureCollector features) {
    Struct struct = sourceFeature.getStruct();
    features.line(sourceFeature.getSourceLayer())
      .setMinZoom(adminLevelMinZoom(struct.get("adminlevel").asInt()))
      .setMinPixelSize(0)
      .setAttr("adminLevel", struct.get("adminlevel").asInt())
      .setAttr("maritime", struct.get("maritime").asBoolean())
      .putAttrs(getCommonTags(struct));
  }


  private static int adminLevelMinZoom(int adminLevel) {
    return adminLevel < 4 ? 0 :
      adminLevel <= 4 ? 4 :
      adminLevel <= 6 ? 9 :
      adminLevel <= 8 ? 11 : 12;
  }
}
