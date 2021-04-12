package com.onthegomap.flatmap;

import com.graphhopper.reader.ReaderRelation;
import com.onthegomap.flatmap.reader.OpenStreetMapReader.RelationInfo;
import java.util.List;

public interface Profile {

  List<RelationInfo> preprocessOsmRelation(ReaderRelation relation);

  void processFeature(SourceFeature sourceFeature, RenderableFeatures features);

  void release();

}
