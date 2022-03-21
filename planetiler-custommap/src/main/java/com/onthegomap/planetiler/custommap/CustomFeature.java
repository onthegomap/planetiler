package com.onthegomap.planetiler.custommap;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.reader.SourceFeature;

public interface CustomFeature {

  boolean includeWhen(SourceFeature sourceFeature);

  void processFeature(SourceFeature sourceFeature, FeatureCollector features);

}
