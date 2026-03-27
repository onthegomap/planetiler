package com.onthegomap.planetiler.examples;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureMerge;
import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.util.Parse;
import java.nio.file.Path;
import java.util.List;

/**
 * Overlay example that emits administrative boundaries with more detail at higher zoom levels.
 * <p>
 * See <a href="https://wiki.openstreetmap.org/wiki/Key:admin_level">OpenStreetMap admin_level documentation</a> for
 * details on administrative boundary levels.
 * <p>
 * It is recommended to try this example using Central America data as a small source with various borders.
 * <p>
 * To run this example:
 * <ol>
 * <li>Download a .osm.pbf extract (see <a href="https://download.geofabrik.de/">Geofabrik download site</a>) (Central
 * America is recommended)</li>
 * <li>Move the file into {@code planetiler-examples/data/sources/}</li>
 * <li>Build the examples: {@code mvn clean package --file standalone.pom.xml }</li>
 * <li>Run this example:
 * {@code java -Xmx6g -Xms6g -cp target/*-with-deps.jar com.onthegomap.planetiler.examples.AdminBordersOverlay --osm-path=./data/sources/central-america.osm.pbf"}</li>
 * <li>Run the demo tileserver: {@code tileserver-gl-light data/admin-borders.mbtiles}</li>
 * <li>View the output at <a href="http://localhost:8080">localhost:8080</a></li>
 * </ol>
 */
public class AdminBordersOverlay implements Profile {

  // Layer name prefixes for administrative boundary features
  private static final String BORDER_LAYER_NAME_PREFIX = "admin_borders-";
  private static final String POLYGON_LABEL_LAYER_NAME_PREFIX = "polygon_area_labels-";

  // Pre-cached admin level configurations to avoid creating instances on each call
  private static final AdminLevelConfig CONFIG_COUNTRY = new AdminLevelConfig("country", "Country", 0, 2, 5);
  private static final AdminLevelConfig CONFIG_REGION = new AdminLevelConfig("region", "Region", 4, 6, 8);
  private static final AdminLevelConfig CONFIG_COUNTY = new AdminLevelConfig("county", "County", 7, 9, 10);
  private static final AdminLevelConfig CONFIG_CITY = new AdminLevelConfig("city", "City", 9, 11, 12);
  private static final AdminLevelConfig CONFIG_CITY_LEVEL_8 = new AdminLevelConfig("city", "City", 11, 12, 13);
  private static final AdminLevelConfig CONFIG_LOCAL = new AdminLevelConfig("local", "Local", 12, 12, 14);

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
    // Filter for administrative boundary features and validate the admin level
    if (sourceFeature.hasTag("boundary", "administrative")) {
      // Admin levels are integers where lower numbers represent higher-level boundaries
      // (e.g., 2 = country, 4 = state/region, 6 = county, 8 = local administration)
      Integer adminLevel = Parse.parseRoundInt(sourceFeature.getTag("admin_level"));
      if (adminLevel == null || adminLevel < 2 || adminLevel > 10) {
        return;
      }

      // Retrieve the configuration for this administrative level
      AdminLevelConfig adminLevelConfig = configForAdminLevel(adminLevel);
      String name = sourceFeature.getString("name", "");

      // Emit boundary line features when the geometry can be represented as a line
      if (sourceFeature.canBeLine()) {
        features.line(BORDER_LAYER_NAME_PREFIX + adminLevelConfig.kind)
          .setAttr("admin_level", adminLevel)
          .setAttr("kind", adminLevelConfig.kind)
          .setAttr("name", name)
          .setAttr("maritime", sourceFeature.getBoolean("maritime"))
          .setZoomRange(adminLevelConfig.borderMinZoom, 14)
          .setMinPixelSize(0);
      }

      // Emit label point features for named polygon areas (e.g., country/region names)
      if (sourceFeature.canBePolygon() && !name.isBlank()) {
        features.pointOnSurface(POLYGON_LABEL_LAYER_NAME_PREFIX + adminLevelConfig.kind)
          .setAttr("name", name)
          .setAttr("admin_level", adminLevel)
          .setAttr("kind", adminLevelConfig.kind)
          .setAttr("label_level", adminLevelConfig.labelLevel)
          .setAttr("label_text", adminLevelConfig.labelLevel + ": " + name)
          .setZoomRange(adminLevelConfig.labelMinZoom, adminLevelConfig.labelMaxZoom)
          .setBufferPixels(64);
      }
    }
  }

  @Override
  public List<VectorTile.Feature> postProcessLayerFeatures(String layer, int zoom, List<VectorTile.Feature> items) {
    // Merge adjacent line strings to simplify the tile and reduce file size
    // See BikeRouteOverlay.postProcessLayerFeatures for details.
    return FeatureMerge.mergeLineStrings(
      items,
      0.5,
      0.1,
      4
    );
  }

  @Override
  public String name() {
    return "Administrative Borders Overlay";
  }

  @Override
  public String description() {
    return "An example overlay showing country, region, and city administrative borders";
  }

  @Override
  public boolean isOverlay() {
    return true;
  }

  @Override
  public String attribution() {
    return """
      <a href="https://www.openstreetmap.org/copyright" target="_blank">&copy; OpenStreetMap contributors</a>
      """.trim();
  }

  // Retrieve the pre-cached admin level configuration for a given administrative level number.
  private static AdminLevelConfig configForAdminLevel(int adminLevel) {
    return switch (adminLevel) {
      case 2 -> CONFIG_COUNTRY;
      case 3, 4 -> CONFIG_REGION;
      case 5, 6 -> CONFIG_COUNTY;
      case 7 -> CONFIG_CITY;
      case 8 -> CONFIG_CITY_LEVEL_8;
      default -> CONFIG_LOCAL;
    };
  }

  private record AdminLevelConfig(String kind, String labelLevel, int borderMinZoom, int labelMinZoom,
    int labelMaxZoom) {}

  public static void main(String[] args) throws Exception {
    run(Arguments.fromArgsOrConfigFile(args));
  }

  static void run(Arguments args) {
    String area = args.getString("area", "geofabrik area to download", "monaco");
    Planetiler.create(args)
      .setProfile(new AdminBordersOverlay())
      .addOsmSource("osm", Path.of("data", "sources", area + ".osm.pbf"), "geofabrik:" + area)
      .overwriteOutput(Path.of("data", "admin-borders.mbtiles"))
      .run();
  }
}
