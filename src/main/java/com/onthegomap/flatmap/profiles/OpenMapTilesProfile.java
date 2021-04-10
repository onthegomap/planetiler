package com.onthegomap.flatmap.profiles;


import com.graphhopper.reader.ReaderRelation;
import com.onthegomap.flatmap.Profile;
import com.onthegomap.flatmap.RenderableFeature;
import com.onthegomap.flatmap.SourceFeature;
import com.onthegomap.flatmap.reader.OpenStreetMapReader.RelationInfo;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenMapTilesProfile implements Profile {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpenMapTilesProfile.class);

  @Override
  public void release() {
  }

  @Override
  public List<RelationInfo> preprocessOsmRelation(ReaderRelation relation) {
    return null;
  }

  @Override
  public void processFeature(SourceFeature sourceFeature, RenderableFeature features) {

  }
}
