package com.onthegomap.flatmap;

import com.graphhopper.reader.ReaderElement;
import com.graphhopper.reader.ReaderRelation;
import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.read.OpenStreetMapReader;
import java.util.List;

public interface Profile {

  default List<OpenStreetMapReader.RelationInfo> preprocessOsmRelation(ReaderRelation relation) {
    return null;
  }

  void processFeature(SourceFeature sourceFeature, FeatureCollector features);

  default void release() {
  }

  default List<VectorTileEncoder.Feature> postProcessLayerFeatures(String layer, int zoom,
    List<VectorTileEncoder.Feature> items) throws GeometryException {
    return items;
  }

  String name();

  default String description() {
    return null;
  }

  default String attribution() {
    return null;
  }

  default String version() {
    return null;
  }

  default boolean isOverlay() {
    return false;
  }

  default boolean caresAboutWikidataTranslation(ReaderElement elem) {
    return true;
  }

  class NullProfile implements Profile {

    @Override
    public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {

    }

    @Override
    public List<VectorTileEncoder.Feature> postProcessLayerFeatures(String layer, int zoom,
      List<VectorTileEncoder.Feature> items) {
      return items;
    }

    @Override
    public String name() {
      return "null";
    }
  }
}
