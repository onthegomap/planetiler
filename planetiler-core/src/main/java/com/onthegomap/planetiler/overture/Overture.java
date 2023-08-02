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
import java.util.regex.Pattern;


public class Overture implements Profile {

  private final boolean connectors;
  private final boolean metadata;

  Overture(PlanetilerConfig config) {
    // 650mb no connectors, no metadata
    // 1gb connectors, no metadata
    // 1.7gb connectors, metadata
    // 932mb no connectors, metadata
    this.connectors = config.arguments().getBoolean("connectors", "include connectors", false);
    this.metadata =
      config.arguments().getBoolean("metadata", "include element metadata (version, update time, id)", false);
  }

  public static void main(String[] args) throws Exception {
    var base = Path.of("data", "sources", "overture");
    var arguments = Arguments.fromEnvOrArgs(args);
    var sample = arguments.getBoolean("sample", "only download smallest file from parquet source", true);

    var pt = Planetiler.create(arguments)
      .addAvroParquetSource("overture", base)
      .setProfile(planetiler -> new Overture(planetiler.config()))
      .overwriteOutput(Path.of("data", "output.pmtiles"));

    downloadFiles(base, pt, "2023-07-26-alpha.0", sample);

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
        case "admins/administrativeBoundary" -> processOvertureFeature(OvertureSchema.AdministrativeBoundary.parse(
          avroFeature.getRecord()), sourceFeature, features);
        case "admins/locality" -> processOvertureFeature(OvertureSchema.Locality.parse(
          avroFeature.getRecord()), sourceFeature, features);
        case "buildings/building" -> processOvertureFeature(OvertureSchema.Building.parse(
          avroFeature.getRecord()), sourceFeature, features);
        case "places/place" -> processOvertureFeature(OvertureSchema.Place.parse(
          avroFeature.getRecord()), sourceFeature, features);
        case "transportation/connector" -> processOvertureFeature(OvertureSchema.Connector.parse(
          avroFeature.getRecord()), sourceFeature, features);
        case "transportation/segment" -> processOvertureFeature(OvertureSchema.Segment.parse(
          avroFeature.getRecord()), sourceFeature, features);
      }
    }
  }

  @Override
  public List<VectorTile.Feature> postProcessLayerFeatures(String layer, int zoom, List<VectorTile.Feature> items)
    throws GeometryException {
    if (layer.equals("admins/administrativeBoundary")) {
      return FeatureMerge.mergeLineStrings(items, 0, 0.125, 4, true);
    } else if (layer.equals("transportation/segment")) {
      return FeatureMerge.mergeLineStrings(items, 0, 0.125, 4, true);
    }
    return items;
  }

  @Override
  public String name() {
    return "Overture";
  }

  private void processOvertureFeature(OvertureSchema.Segment element, SourceFeature sourceFeature,
    FeatureCollector features) {
    int minzoom = switch (element.subType()) {
      case ROAD -> switch (element.road().roadClass()) {
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
    var feature = features.line(sourceFeature.getSourceLayer())
      .setMinZoom(minzoom)
      .setAttr("width", element.width())
      .setMinPixelSize(0)
      .setAttr("subType", element.subType().toString().toLowerCase())
      .putAttrs(getCommonTags(element.info()));
    if (connectors) {
      feature.setAttr("connectors", join(",", element.connectors()));
    }
    if (element.road() != null) {
      OvertureSchema.Road road = element.road();
      feature
        // TODO partial road names ? partial tags
        .putAttrs(road.roadNames() == null ? Map.of() : getNames(road.roadNames().stream()
          .filter(d -> d.at() == null)
          .findFirst()
          .orElse(null)))
        .setAttr("roadNames",
          toJsonString(
            road.roadNames() == null ? null : road.roadNames().stream().filter(d -> d.at() != null).toList()))
        .setAttr("class", road.roadClass().toString())
        .setAttr("flags", toJsonString(road.flags()))
        .setAttr("lanes", toJsonString(road.lanes()))
        .setAttr("surface", toJsonString(road.surface()))
        .setAttr("restrictions", toJsonString(road.restrictions()));
    }
  }

  private void processOvertureFeature(OvertureSchema.Connector element, SourceFeature sourceFeature,
    FeatureCollector features) {
    if (connectors) {
      features.point(sourceFeature.getSourceLayer())
        .setMinZoom(14)
        .putAttrs(getCommonTags(element.info()));
    }
  }

  private void processOvertureFeature(OvertureSchema.Place element, SourceFeature sourceFeature,
    FeatureCollector features) {
    features.point(sourceFeature.getSourceLayer())
      .setMinZoom(14)
      .setAttr("emails", join(",", element.emails()))
      .setAttr("phones", join(",", element.phones()))
      .setAttr("socials", join(",", element.socials()))
      .setAttr("websites", join(",", element.websites()))
      .setAttr("confidence", element.confidence())
      .setAttr("brand.wikidata", element.brand() == null ? null : element.brand().wikidata())
      .putAttrs(getNames("brand.", element.brand() == null ? null : element.brand().names()))
      .setAttr("addresses", toJsonString(element.addresses()))
      .setAttr("category", element.categories().main())
      .setAttr("category.alternate", join(",", element.categories().alternate()))
      .putAttrs(getCommonTags(element.info()))
      .putAttrs(getNames(element.names()));
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

  private void processOvertureFeature(OvertureSchema.Building element, SourceFeature sourceFeature,
    FeatureCollector features) {
    if (sourceFeature.canBePolygon()) {
      features.polygon(sourceFeature.getSourceLayer())
        .setMinZoom(14)
        .setAttr("class", element.class_() != null ? element.class_().toString() : null)
        .setAttr("height", element.height())
        .setAttr("numFloors", element.numFloors())
        .putAttrs(getCommonTags(element.info()));
      if (element.names() != null) {
        var names = getNames(element.names());
        if (!names.isEmpty()) {
          features.centroidIfConvex(sourceFeature.getSourceLayer())
            .setMinZoom(14)
            .setAttr("class", element.class_() != null ? element.class_().toString() : null)
            .setAttr("height", element.height())
            .setAttr("numFloors", element.numFloors())
            .putAttrs(getNames(element.names()))
            .putAttrs(getCommonTags(element.info()));
        }
      }
    }
  }

  private void processOvertureFeature(OvertureSchema.Locality element, SourceFeature sourceFeature,
    FeatureCollector features) {
    if (sourceFeature.canBePolygon()) {
      features.pointOnSurface(sourceFeature.getSourceLayer())
        .setMinZoom(adminLevelMinZoom(element.adminLevel()))
        .putAttrs(getNames(element.names()))
        .setAttr("adminLevel", element.adminLevel())
        .setAttr("subType", element.subType())
        .setAttr("isoCountryCodeAlpha2", element.isoCountryCodeAlpha2())
        .setAttr("isoSubCountryCode", element.isoSubCountryCode())
        .putAttrs(getCommonTags(element.info()));
    }
  }

  private static Map<String, Object> getNames(OvertureSchema.Names names) {
    return getNames(null, names);
  }

  private static Map<String, Object> getNames(String prefix, OvertureSchema.Names names) {
    if (names == null) {
      return Map.of();
    }
    String base = prefix == null ? "name" : (prefix + ".name");
    Map<String, Object> result = new LinkedHashMap<>();
    if (names.common() != null) {
      if (!names.common().isEmpty()) {
        result.put(base, names.common().get(0).value());
      }
      for (var name : names.common()) {
        put(result, base + ".common." + name.language(), name.value());
      }
    }
    if (names.official() != null) {
      for (var name : names.official()) {
        put(result, base + ".official." + name.language(), name.value());
      }
    }
    if (names.shortName() != null) {
      for (var name : names.shortName()) {
        put(result, base + ".short." + name.language(), name.value());
      }
    }
    if (names.alternate() != null) {
      for (var name : names.alternate()) {
        put(result, base + ".alternate." + name.language(), name.value());
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

  private Map<String, Object> getCommonTags(OvertureSchema.SharedMetadata info) {
    Map<String, Object> results = new HashMap<>(4);
    if (metadata) {
      results.put("id", info.id());
      results.put("version", info.version());
      results.put("updateTime", Instant.ofEpochMilli(info.updateTime()).toString());
    }
    results.put("sources", join(",", info.sources().stream()
      .map(d -> d.dataset() + (d.recordId() != null ? (":" + d.recordId()) : ""))
      .distinct()
      .toList()));
    return results;
  }

  private void processOvertureFeature(OvertureSchema.AdministrativeBoundary element,
    SourceFeature sourceFeature, FeatureCollector features) {
    features.line(sourceFeature.getSourceLayer())
      .setMinZoom(adminLevelMinZoom(element.adminLevel()))
      .setMinPixelSize(0)
      .setAttr("adminLevel", element.adminLevel())
      .setAttr("maritime", element.maritime())
      .putAttrs(getCommonTags(element.info()));
  }


  private static int adminLevelMinZoom(int adminLevel) {
    return adminLevel < 4 ? 0 :
      adminLevel <= 4 ? 4 :
      adminLevel <= 6 ? 9 :
      adminLevel <= 8 ? 11 : 12;
  }
}
