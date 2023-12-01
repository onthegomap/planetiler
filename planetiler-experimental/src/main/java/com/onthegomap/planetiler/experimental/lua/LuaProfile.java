package com.onthegomap.planetiler.experimental.lua;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmRelationInfo;
import java.util.List;
import java.util.function.Consumer;
import org.luaj.vm2.LuaInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link Profile} that delegates to a lua script.
 */
@SuppressWarnings("java:S1168")
public class LuaProfile implements Profile {

  private static final Logger LOGGER = LoggerFactory.getLogger(LuaProfile.class);
  private final LuaEnvironment.PlanetilerNamespace planetiler;

  public LuaProfile(LuaEnvironment env) {
    this.planetiler = env.planetiler;
    if (planetiler.process_feature == null) {
      LOGGER.warn("Missing function planetiler.process_feature");
    }
  }

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
    if (planetiler.process_feature != null) {
      planetiler.process_feature.call(LuaConversions.toLua(sourceFeature), LuaConversions.toLua(features));
    }
  }

  @Override
  public List<VectorTile.Feature> postProcessLayerFeatures(String layer, int zoom, List<VectorTile.Feature> items) {
    if (planetiler.post_process != null) {
      return LuaConversions.toJavaList(
        planetiler.post_process.call(LuaConversions.toLua(layer), LuaConversions.toLua(zoom),
          LuaConversions.toLua(items)),
        VectorTile.Feature.class);
    }
    return null;
  }

  @Override
  public boolean caresAboutSource(String name) {
    return planetiler.cares_about_source == null || planetiler.cares_about_source.call(name).toboolean();
  }

  @Override
  public boolean caresAboutWikidataTranslation(OsmElement elem) {
    return planetiler.cares_about_wikidata_translation != null &&
      planetiler.cares_about_wikidata_translation.call(LuaConversions.toLua(elem)).toboolean();
  }

  @Override
  public String name() {
    return planetiler.output.name;
  }

  @Override
  public String description() {
    return planetiler.output.description;
  }

  @Override
  public String attribution() {
    return planetiler.output.attribution;
  }

  @Override
  public String version() {
    return planetiler.output.version;
  }


  @Override
  public boolean isOverlay() {
    return planetiler.output.is_overlay;
  }

  @Override
  public void finish(String sourceName, FeatureCollector.Factory featureCollectors,
    Consumer<FeatureCollector.Feature> next) {
    if (planetiler.finish != null) {
      planetiler.finish.call(LuaConversions.toLua(sourceName), LuaConversions.toLua(featureCollectors),
        LuaConversions.consumerToLua(next, FeatureCollector.Feature.class));
    }
  }

  @Override
  public long estimateIntermediateDiskBytes(long osmFileSize) {
    return planetiler.estimate_intermediate_disk_bytes == null ? 0 :
      planetiler.estimate_intermediate_disk_bytes.call(LuaInteger.valueOf(osmFileSize)).tolong();
  }

  @Override
  public long estimateOutputBytes(long osmFileSize) {
    return planetiler.estimate_output_bytes == null ? 0 :
      planetiler.estimate_output_bytes.call(LuaInteger.valueOf(osmFileSize)).tolong();
  }

  @Override
  public long estimateRamRequired(long osmFileSize) {
    return planetiler.estimate_ram_required == null ? 0 :
      planetiler.estimate_ram_required.call(LuaInteger.valueOf(osmFileSize)).tolong();
  }

  @Override
  public void preprocessOsmNode(OsmElement.Node node) {
    if (planetiler.preprocess_osm_node != null) {
      planetiler.preprocess_osm_node.call(LuaConversions.toLua(node));
    }
  }

  @Override
  public void preprocessOsmWay(OsmElement.Way way) {
    if (planetiler.preprocess_osm_way != null) {
      planetiler.preprocess_osm_way.call(LuaConversions.toLua(way));
    }
  }

  @Override
  public List<OsmRelationInfo> preprocessOsmRelation(OsmElement.Relation relation) {
    if (planetiler.preprocess_osm_relation == null) {
      return null;
    }
    return LuaConversions.toJavaList(planetiler.preprocess_osm_relation.call(LuaConversions.toLua(relation)),
      OsmRelationInfo.class);
  }

  @Override
  public void release() {
    if (planetiler.release != null) {
      planetiler.release.call();
    }
  }
}
