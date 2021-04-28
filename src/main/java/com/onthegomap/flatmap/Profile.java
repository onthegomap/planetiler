package com.onthegomap.flatmap;

import com.graphhopper.reader.ReaderRelation;
import com.onthegomap.flatmap.VectorTileEncoder.VectorTileFeature;
import com.onthegomap.flatmap.reader.OpenStreetMapReader.RelationInfo;
import java.util.List;

public interface Profile {

  List<RelationInfo> preprocessOsmRelation(ReaderRelation relation);

  void processFeature(SourceFeature sourceFeature, RenderableFeatures features);

  void release();

  List<VectorTileFeature> postProcessLayerFeatures(String layer, int zoom, List<VectorTileFeature> items);

  class NullProfile implements Profile {

    @Override
    public List<RelationInfo> preprocessOsmRelation(ReaderRelation relation) {
      return null;
    }

    @Override
    public void processFeature(SourceFeature sourceFeature, RenderableFeatures features) {

    }

    @Override
    public void release() {

    }

    @Override
    public List<VectorTileFeature> postProcessLayerFeatures(String layer, int zoom,
      List<VectorTileFeature> items) {
      return items;
    }
  }
}
