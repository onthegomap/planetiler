import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.reader.SourceFeature;
import java.nio.file.Path;

class Power implements Profile {
  public void processFeature(SourceFeature source, FeatureCollector features) {
    if (source.canBeLine() && source.hasTag("power", "line")) {
      features
        .line("power")
        .setMinZoom(7)
        .inheritAttrFromSource("power")
        .inheritAttrFromSource("voltage")
        .inheritAttrFromSource("cables")
        .inheritAttrFromSource("operator");
    } else if (source.isPoint() && source.hasTag("power", "pole")) {
      features
        .point("power")
        .setMinZoom(13)
        .inheritAttrFromSource("power")
        .inheritAttrFromSource("ref")
        .inheritAttrFromSource("height")
        .inheritAttrFromSource("operator");
    }

  }

  public String name() {
    return "power";
  }

  void main(String[] argv) throws Exception {
    Arguments args = Arguments.fromArgsOrConfigFile(argv);
    var area = args.getString("area", "geofabrik area to download", "massachusetts");
    Planetiler.create(args)
      .setProfile(new Power())
      .addOsmSource("osm", Path.of("data/sources/" + area + ".osm.pbf"), "geofabrik:" + area)
      .overwriteOutput(Path.of("data/buildings.pmtiles"))
      .run();
  }
}
