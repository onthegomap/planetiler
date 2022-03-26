package com.onthegomap.planetiler.custommap.features;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureCollector.Feature;
import com.onthegomap.planetiler.custommap.CustomFeature;
import com.onthegomap.planetiler.custommap.ValueParser;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.util.ZoomFunction;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Predicate;


public class Waterway implements CustomFeature {

  private double BUFFER_SIZE = 4.0;

  private static Collection<String> waterwayIncludeNames = Arrays.asList("river", "canal", "stream");

  private static Predicate<SourceFeature> renderNameLogic =
    sf -> waterwayIncludeNames.contains(sf.getString("waterway"));

  private static List<BiConsumer<SourceFeature, Feature>> attributeProcessors =
    Arrays.asList(
      ValueParser.passBoolAttrIfTrue("intermittent", 7),
      ValueParser.passAttrOnCondition("name", 12, renderNameLogic)
    );

  @Override
  public boolean includeWhen(SourceFeature sourceFeature) {
    return sourceFeature.canBeLine() && sourceFeature.hasTag("waterway", "river", "canal", "stream");
  }

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
    Feature f = features.line("water")
      .setBufferPixels(BUFFER_SIZE)
      .setAttrWithMinzoom("waterway", sourceFeature.getTag("waterway"), 7)
      .setZoomRange(7, 14)
      // and also whenever you set a label grid size limit, make sure you increase the buffer size so no
      // label grid squares will be the consistent between adjacent tiles
      .setBufferPixelOverrides(ZoomFunction.maxZoom(12, 32));

    attributeProcessors.forEach(p -> p.accept(sourceFeature, f));
  }
}
