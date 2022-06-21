package com.onthegomap.planetiler.examples;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmSourceFeature;
import java.nio.file.Path;

public class OsmQaTiles implements Profile {

  public static void main(String[] args) throws Exception {
    run(Arguments.fromArgsOrConfigFile(args));
  }

  static void run(Arguments inArgs) throws Exception {
    int zoom = inArgs.getInteger("zoom", "zoom level to generate tiles at", 12);
    var args = inArgs.orElse(Arguments.of(
      "minzoom", zoom,
      "maxzoom", zoom
    ));
    String area = args.getString("area", "geofabrik area to download", "monaco");
    Planetiler.create(args)
      .setProfile(new OsmQaTiles())
      .addOsmSource("osm", Path.of("data", "sources", area + ".osm.pbf"), "geofabrik:" + area)
      .overwriteOutput("mbtiles", Path.of("data", "qa.mbtiles"))
      .run();
  }

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
    if (!sourceFeature.tags().isEmpty() && sourceFeature instanceof OsmSourceFeature osmFeature) {
      var feature = sourceFeature.canBePolygon() ? features.polygon("osm") :
        sourceFeature.canBeLine() ? features.line("osm") :
        sourceFeature.isPoint() ? features.point("osm") :
        null;
      if (feature != null) {
        var element = osmFeature.originalElement();
        feature
          .setMinPixelSize(0)
          .setPixelTolerance(0)
          .setBufferPixels(0)
          // TODO:
          // @type	node
          // @id	4688642320
          // @version	1
          // @changeset	46134597
          // @uid	...
          // @user	...
          // @timestamp	1487245087
          .setAttr("@id", sourceFeature.id());
        for (var entry : sourceFeature.tags().entrySet()) {
          feature.setAttr(entry.getKey(), entry.getValue());
        }
        if (element instanceof OsmElement.Node) {
          feature.setAttr("@type", "node");
        } else if (element instanceof OsmElement.Way) {
          feature.setAttr("@type", "way");
        } else if (element instanceof OsmElement.Relation) {
          feature.setAttr("@type", "relation");
        }
      }
    }
  }

  @Override
  public String name() {
    return "osm qa";
  }
}
