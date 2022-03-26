package com.onthegomap.planetiler.custommap.features;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureCollector.Feature;
import com.onthegomap.planetiler.custommap.CustomFeature;
import com.onthegomap.planetiler.custommap.ValueParser;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.util.ZoomFunction;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;


public class WaterArea implements CustomFeature {

  private static final double BUFFER_SIZE = 4.0;

  private static final double LOG4 = Math.log(4);
  private static final double MIN_TILE_PERCENT_TO_SHOW_NAME = 0.01;

  private static Set<String> waterExcludeNames = new HashSet<>(Arrays.asList("river", "canal", "stream"));

  private static Predicate<SourceFeature> renderNameTagLogic = sf -> {
    String water = sf.getString("water");
    return Objects.isNull(water) || !waterExcludeNames.contains(water);
  };

  private static Function<SourceFeature, Integer> renderNameZoomLogic = sf -> {
    try {
      int zoom = (int) (Math.log(MIN_TILE_PERCENT_TO_SHOW_NAME / sf.area()) / LOG4);
      return Math.max(0, Math.min(zoom, 14));
    } catch (GeometryException e) {
      return 14;
    }
  };

  private static List<BiConsumer<SourceFeature, Feature>> attributeProcessors =
    Arrays.asList(
      ValueParser.passBoolAttrIfTrue("intermittent", 0),
      ValueParser.passAttrOnCondition("name", renderNameZoomLogic, renderNameTagLogic)
    );

  @Override
  public boolean includeWhen(SourceFeature sourceFeature) {
    if (sourceFeature.getSource().equals("water_polygons")) {
      return true;
    }
    return sourceFeature.canBePolygon() && sourceFeature.hasTag("natural", "water");
  }

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
    Feature f = features.polygon("water")
      .setBufferPixels(BUFFER_SIZE)
      .setAttrWithMinzoom("natural", "water", 0)
      .setZoomRange(0, 14)
      .setMinPixelSize(2)
      // and also whenever you set a label grid size limit, make sure you increase the buffer size so no
      // label grid squares will be the consistent between adjacent tiles
      .setBufferPixelOverrides(ZoomFunction.maxZoom(12, 32));

    attributeProcessors.forEach(p -> p.accept(sourceFeature, f));
  }
}
