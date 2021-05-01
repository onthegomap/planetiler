package com.onthegomap.flatmap;

import com.graphhopper.reader.ReaderRelation;
import com.onthegomap.flatmap.reader.OpenStreetMapReader;
import java.util.List;

public interface Profile {

  List<OpenStreetMapReader.RelationInfo> preprocessOsmRelation(ReaderRelation relation);

  void processFeature(SourceFeature sourceFeature, RenderableFeatures features);

  void release();

  List<VectorTileEncoder.Feature> postProcessLayerFeatures(String layer, int zoom,
    List<VectorTileEncoder.Feature> items);

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

  class NullProfile implements Profile {

    @Override
    public List<OpenStreetMapReader.RelationInfo> preprocessOsmRelation(ReaderRelation relation) {
      return null;
    }

    @Override
    public void processFeature(SourceFeature sourceFeature, RenderableFeatures features) {

    }

    @Override
    public void release() {

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
