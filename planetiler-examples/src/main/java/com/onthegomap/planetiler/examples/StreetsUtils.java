package com.onthegomap.planetiler.examples;

import com.onthegomap.planetiler.reader.SourceFeature;
import java.util.Arrays;
import java.util.List;
import jnr.ffi.annotations.In;

class RoadwayLanes {
  Integer forward = null;
  Integer backward = null;
}

public class StreetsUtils {
  private static final List<String> memorialTypes = Arrays.asList(
    "war_memorial", "stele", "obelisk", "memorial", "stone"
  );

  public static boolean isMemorial(SourceFeature sourceFeature) {
    return sourceFeature.hasTag("historic", "memorial") && memorialTypes.contains((String) sourceFeature.getTag("memorial"));
  }

  public static boolean isFireHydrant(SourceFeature sourceFeature) {
    return sourceFeature.hasTag("emergency", "fire_hydrant") &&
      (sourceFeature.hasTag("fire_hydrant:type", "pillar") || sourceFeature.getTag("fire_hydrant:type") == null);
  }

  public static boolean isStatue(SourceFeature sourceFeature) {
    return sourceFeature.hasTag("historic", "memorial") && sourceFeature.hasTag("memorial", "statue") ||
      sourceFeature.hasTag("tourism", "artwork") && sourceFeature.hasTag("artwork_type", "statue");
  }

  public static boolean isSculpture(SourceFeature sourceFeature) {
    return sourceFeature.hasTag("tourism", "artwork") && sourceFeature.hasTag("artwork_type", "sculpture") ||
      sourceFeature.hasTag("historic", "memorial") && sourceFeature.hasTag("memorial", "sculpture");
  }

  public static boolean isWindTurbine(SourceFeature sourceFeature) {
    return sourceFeature.hasTag("power", "generator") && sourceFeature.hasTag("generator:source", "wind");
  }

  public static boolean isRailway(SourceFeature sourceFeature) {
    return sourceFeature.hasTag("railway",
      "rail",
      "light_rail",
      "subway",
      "disused",
      "narrow_gauge",
      "tram"
    );
  }

  public static boolean isWater(SourceFeature sourceFeature) {
    return sourceFeature.getSource().equals("water") ||
      sourceFeature.hasTag("natural", "water") ||
      (
        sourceFeature.hasTag("leisure", "swimming_pool") &&
        !sourceFeature.hasTag("location", "indoor", "roof")
      );
  }

  public static boolean isRoadwayOneway(SourceFeature sourceFeature) {
    return sourceFeature.hasTag("oneway", "yes") || sourceFeature.hasTag("junction", "roundabout");
  }

  public static RoadwayLanes getRoadwayLanes(SourceFeature sourceFeature) {
    Integer lanesForward = parseUnsignedInt((String) sourceFeature.getTag("lanes:forward"));
    Integer lanesBackward = parseUnsignedInt((String) sourceFeature.getTag("lanes:backward"));

    return new RoadwayLanes() {{
      forward = lanesForward;
      backward = lanesBackward;
    }};
  }

  public static Double getHeight(SourceFeature sourceFeature) {
    Double height = parseDouble((String) sourceFeature.getTag("height"));
    Double estHeight = parseDouble((String) sourceFeature.getTag("est_height"));

    if (height != null) {
      return height;
    }

    return estHeight;
  }

  public static Double getMinHeight(SourceFeature sourceFeature) {
    return parseDouble((String) sourceFeature.getTag("min_height"));
  }

  public static Double getRoofHeight(SourceFeature sourceFeature) {
    return parseDouble((String) sourceFeature.getTag("roof:height"));
  }

  public static Integer getBuildingLevels(SourceFeature sourceFeature) {
    return parseUnsignedInt((String) sourceFeature.getTag("building:levels"));
  }

  public static Integer getRoofLevels(SourceFeature sourceFeature) {
    return parseUnsignedInt((String) sourceFeature.getTag("roof:levels"));
  }

  public static Double getWidth(SourceFeature sourceFeature) {
    return parseDouble((String) sourceFeature.getTag("width"));
  }

  public static Double getDirection(SourceFeature sourceFeature) {
    return DirectionParser.parse((String) sourceFeature.getTag("direction"));
  }

  public static Double getRoofDirection(SourceFeature sourceFeature) {
    return DirectionParser.parse((String) sourceFeature.getTag("roof:direction"));
  }

  public static Double getAngle(SourceFeature sourceFeature) {
    return parseDouble((String) sourceFeature.getTag("angle"));
  }

  private static Double parseDouble(String value) {
    if (value == null) return null;

    try {
      return Double.parseDouble(value);
    } catch (Exception ex) {
      return null;
    }
  }

  private static Integer parseUnsignedInt(String value) {
    if (value == null) return null;

    try {
      return Math.max(0, (int) Double.parseDouble(value));
    } catch (Exception ex) {
      return null;
    }
  }
}
