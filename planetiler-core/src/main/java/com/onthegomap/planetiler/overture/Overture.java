package com.onthegomap.planetiler.overture;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


public class Overture implements Profile {

  private final boolean connectors;
  private final boolean metadata;
  private final PlanetilerConfig config;

  Overture(PlanetilerConfig config) {
    this.config = config;
    this.connectors = config.arguments().getBoolean("connectors", "include connectors", true);
    this.metadata =
      config.arguments().getBoolean("metadata", "include element metadata (version, update time)", false);
  }

  public static void main(String[] args) throws Exception {
    var base = Path.of("data", "sources", "overture");
    var arguments = Arguments.fromEnvOrArgs(args);
    var sample = arguments.getBoolean("sample", "only download smallest file from parquet source", false);
    var release = arguments.getString("release", "overture release", "2023-07-26-alpha.0");

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
        case "admins/administrativeBoundary" -> processOvertureFeature(
          (OvertureSchema.AdministrativeBoundary) null, sourceFeature, features);
        case "admins/locality" -> processOvertureFeature(
          (OvertureSchema.Locality) null, sourceFeature, features);
        case "buildings/building" -> processOvertureFeature(
          (OvertureSchema.Building) null, sourceFeature, features);
        case "places/place" -> processOvertureFeature(
          (OvertureSchema.Place) null, sourceFeature, features);
        case "transportation/connector" -> processOvertureFeature(
          (OvertureSchema.Connector) null, sourceFeature, features);
        case "transportation/segment" -> processOvertureFeature(
          (OvertureSchema.Segment) null, sourceFeature, features);
      }
    }
  }

  @Override
  public List<VectorTile.Feature> postProcessLayerFeatures(String layer, int zoom, List<VectorTile.Feature> items)
    throws GeometryException {
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
      <a href="https://www.openstreetmap.org/copyright" target="_blank">&copy; OpenStreetMap contributors</a>
      <a href="https://overturemaps.org/download/overture-july-alpha-release-notes/" target="_blank">&copy; Overture Foundation</a>
      """
      .replaceAll("\n", " ")
      .trim();
  }

  private void processOvertureFeature(OvertureSchema.Segment element, SourceFeature sourceFeature,
    FeatureCollector features) {
    Struct struct = ((AvroParquetFeature) sourceFeature).getStruct();
    var subtype = struct.get("subtype").as(OvertureSchema.SegmentSubType.class);
    var roadClass = struct.get("road", "class").as(OvertureSchema.RoadClass.class);
    if (roadClass == null || subtype == null) {
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
    if (struct.get("road.flags").asList().stream().map(Struct::asString).anyMatch("isLink"::equals)) {
      minzoom = Math.max(minzoom, 9);
    }
    var feature = features.line(sourceFeature.getSourceLayer())
      .setMinZoom(minzoom)
      // TODO road not present
      //      .setAttr("width", struct.get("width").asDouble())
      .setMinPixelSize(0)
      .setAttr("subType", struct.get("subtype").asString())
      .putAttrs(getCommonTags(struct));
    if (connectors) {
      feature.setAttrWithMinzoom("connectors", join(",", struct.get("connectors")), 14);
    }
    Struct road = struct.get("road");
    if (!road.isNull()) {
      List<Struct> names = road.get("roadnames").asList();
      Optional<Struct> fullLengthName = names.stream()
        .filter(d -> d.get("at").isNull())
        .findFirst();
      List<Object> otherNames = names.stream().filter(d -> !d.get("at").isNull()).map(Struct::rawValue).toList();
      feature
        // TODO partial road names ? partial tags
        .putAttrs(fullLengthName.map(Overture::getNames).orElse(Map.of()))
        .setAttr("roadNames", toJsonString(otherNames))
        .setAttr("class", road.get("class").asString())
        .setAttr("flags", road.get("flags").asJson())
        .setAttr("lanes", road.get("lanes").asJson())
        .setAttr("surface", road.get("surface").asJson())
        .setAttr("restrictions", road.get("restrictions").asJson());
    }
  }

  private void processOvertureFeature(OvertureSchema.Connector element, SourceFeature sourceFeature,
    FeatureCollector features) {
    if (connectors) {
      Struct struct = ((AvroParquetFeature) sourceFeature).getStruct();
      features.point(sourceFeature.getSourceLayer())
        .setMinZoom(14)
        .putAttrs(getCommonTags(struct));
    }
  }

  private void processOvertureFeature(OvertureSchema.Place element, SourceFeature sourceFeature,
    FeatureCollector features) {
    Struct struct = ((AvroParquetFeature) sourceFeature).getStruct();
    features.point(sourceFeature.getSourceLayer())
      .setMinZoom(14)
      .setAttr("emails", join(",", struct.get("emails")))
      .setAttr("phones", join(",", struct.get("phones")))
      .setAttr("socials", join(",", struct.get("socials")))
      .setAttr("websites", join(",", struct.get("websites")))
      .setAttr("confidence", struct.get("confidence").asDouble())
      .setAttr("brand.wikidata", struct.get("brand", "wikidata").asString())
      .putAttrs(getNames("brand", struct.get("brand", "names")))
      .setAttr("addresses", struct.get("addresses").asJson())
      .setAttr("categories.main", struct.get("categories", "main").asString())
      .setAttr("categories.alternate", join(",", struct.get("categories", "alternate")))
      .putAttrs(getCommonTags(struct))
      .putAttrs(getNames(struct.get("names")));
  }

  private static String toJsonString(List<?> list) {
    try {
      return list == null || list.isEmpty() ? null : OvertureSchema.mapper.writeValueAsString(list);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static String toJsonString(Object obj) {
    try {
      return obj == null ? null : OvertureSchema.mapper.writeValueAsString(obj);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static String join(String sep, Collection<String> items) {
    return items == null || items.isEmpty() ? null : String.join(sep, items);
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

  private void processOvertureFeature(OvertureSchema.Building element, SourceFeature sourceFeature,
    FeatureCollector features) {
    if (sourceFeature.canBePolygon()) {
      Struct struct = ((AvroParquetFeature) sourceFeature).getStruct();
      var commonTags = getCommonTags(struct);
      commonTags.put("class", struct.get("class").asString());
      commonTags.put("height", struct.get("height").asDouble());
      commonTags.put("numFloors", struct.get("numfloors").asInt());
      features.polygon(sourceFeature.getSourceLayer())
        .setMinZoom(14)
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

  private void processOvertureFeature(OvertureSchema.Locality element, SourceFeature sourceFeature,
    FeatureCollector features) {
    if (sourceFeature.canBePolygon()) {
      Struct struct = ((AvroParquetFeature) sourceFeature).getStruct();
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
        if (first) {
          first = false;
          put(result, "name", value);
        }
        put(result, base + "." + key + "." + name.get("language").asString(), value);
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
    results.put("id", info.get("id").asString());
    results.put("sources", info.get("sources").asList().stream().map(d -> {
      String recordId = d.get("recordId").asString();
      if (recordId == null) {
        recordId = d.get("recordid").asString();
      }
      return d.get("dataset").asString() + (recordId == null ? "" : (":" + recordId));
    }).sorted().distinct().collect(Collectors.joining(",")));
    return results;
  }

  private void processOvertureFeature(OvertureSchema.AdministrativeBoundary element,
    SourceFeature sourceFeature, FeatureCollector features) {
    Struct struct = ((AvroParquetFeature) sourceFeature).getStruct();
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
