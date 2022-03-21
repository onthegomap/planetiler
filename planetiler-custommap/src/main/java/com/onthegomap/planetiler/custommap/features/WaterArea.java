package com.onthegomap.planetiler.custommap.features;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureCollector.Feature;
import com.onthegomap.planetiler.custommap.CustomFeature;
import com.onthegomap.planetiler.custommap.ValueParser;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.util.ZoomFunction;


public class WaterArea implements CustomFeature {

  double BUFFER_SIZE = 4.0;

  private static Set<String> waterExcludeNames = new HashSet<>(Arrays.asList("river", "canal", "stream"));

  private static Predicate<SourceFeature> renderNameLogic = sf -> {
    String water = sf.getString("water");
    return Objects.isNull(water) || !waterExcludeNames.contains(water);
  };

  private static List<BiConsumer<SourceFeature, Feature>> attributeProcessors =
    Arrays.asList(
      ValueParser.passBoolAttrIfTrue("intermittent", 0),
      ValueParser.passAttrOnCondition("name", 0, renderNameLogic)
    );

  @Override
  public boolean includeWhen(SourceFeature sourceFeature) {
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
