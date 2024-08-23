package com.onthegomap.planetiler;

import static com.onthegomap.planetiler.TestUtils.assertSubmap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.expression.Expression;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.reader.SimpleFeature;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmRelationInfo;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class ForwardingProfileTests {

  private ForwardingProfile profile = new ForwardingProfile() {};

  @Test
  void testPreprocessOsmNode() {
    var node1 = new OsmElement.Node(1, 2, 3);
    var node2 = new OsmElement.Node(2, 3, 4);
    List<OsmElement.Node> calledWith = new ArrayList<>();
    profile.registerHandler((ForwardingProfile.OsmNodePreprocessor) calledWith::add);
    profile.preprocessOsmNode(node1);
    assertEquals(List.of(node1), calledWith);

    List<OsmElement.Node> calledWith2 = new ArrayList<>();
    profile.registerHandler((ForwardingProfile.OsmNodePreprocessor) calledWith2::add);
    profile.preprocessOsmNode(node2);
    assertEquals(List.of(node1, node2), calledWith);
    assertEquals(List.of(node2), calledWith2);
  }

  @Test
  void testPreprocessOsmWay() {
    var way1 = new OsmElement.Way(1);
    var way2 = new OsmElement.Way(2);
    List<OsmElement.Way> calledWith = new ArrayList<>();
    profile.registerHandler((ForwardingProfile.OsmWayPreprocessor) calledWith::add);
    profile.preprocessOsmWay(way1);
    assertEquals(List.of(way1), calledWith);

    List<OsmElement.Way> calledWith2 = new ArrayList<>();
    profile.registerHandler((ForwardingProfile.OsmWayPreprocessor) calledWith2::add);
    profile.preprocessOsmWay(way2);
    assertEquals(List.of(way1, way2), calledWith);
    assertEquals(List.of(way2), calledWith2);
  }

  @Test
  void testPreprocessOsmRelation() {
    record RelA(@Override long id) implements OsmRelationInfo {}
    record RelB(@Override long id) implements OsmRelationInfo {}
    assertNull(profile.preprocessOsmRelation(new OsmElement.Relation(1)));
    profile.registerHandler((ForwardingProfile.OsmRelationPreprocessor) relation -> List.of(new RelA(relation.id())));
    assertEquals(List.of(new RelA(1)), profile.preprocessOsmRelation(new OsmElement.Relation(1)));
    profile.registerHandler((ForwardingProfile.OsmRelationPreprocessor) relation -> null);
    profile.registerHandler((ForwardingProfile.OsmRelationPreprocessor) relation -> List.of(new RelB(relation.id())));
    assertEquals(List.of(new RelA(1), new RelB(1)), profile.preprocessOsmRelation(new OsmElement.Relation(1)));
  }

  private void testFeatures(List<Map<String, Object>> expected, SourceFeature feature) {
    List<FeatureCollector.Feature> actualList = TestUtils.processSourceFeature(feature, profile);
    assertEquals(expected.size(), actualList.size(), () -> "size: " + actualList);
    for (int i = 0; i < expected.size(); i++) {
      assertSubmap(expected.get(i), TestUtils.toMap(actualList.get(i), 14));
    }
  }

  @Test
  void testProcessFeature() {
    SourceFeature a = SimpleFeature.create(GeoUtils.EMPTY_POINT, Map.of(), "srca", null, 1);
    SourceFeature b = SimpleFeature.create(GeoUtils.EMPTY_POINT, Map.of(), "srcb", null, 1);
    testFeatures(List.of(), a);
    testFeatures(List.of(), b);

    profile.registerSourceHandler(a.getSource(), (elem, features) -> features.point("a"));
    testFeatures(List.of(Map.of(
      "_layer", "a"
    )), a);
    testFeatures(List.of(), b);

    profile.registerSourceHandler(b.getSource(), (elem, features) -> features.point("b"));
    testFeatures(List.of(Map.of(
      "_layer", "a"
    )), a);
    testFeatures(List.of(Map.of(
      "_layer", "b"
    )), b);

    profile.registerSourceHandler(a.getSource(), (elem, features) -> features.point("a2"));
    testFeatures(List.of(Map.of(
      "_layer", "a"
    ), Map.of(
      "_layer", "a2"
    )), a);
    testFeatures(List.of(Map.of(
      "_layer", "b"
    )), b);
  }

  @Test
  void testProcessFeatureWithFilter() {
    SourceFeature a = SimpleFeature.create(GeoUtils.EMPTY_POINT, Map.of("key", "value"), "srca", null, 1);
    SourceFeature b = SimpleFeature.create(GeoUtils.EMPTY_POINT, Map.of(), "srcb", null, 1);

    profile.registerSourceHandler(a.getSource(), new ForwardingProfile.FeatureProcessor() {
      @Override
      public void processFeature(SourceFeature elem, FeatureCollector features) {
        features.point("a");
      }

      @Override
      public Expression filter() {
        return Expression.matchAny("key", "value");
      }
    });
    testFeatures(List.of(Map.of(
      "_layer", "a"
    )), a);
    testFeatures(List.of(), b);
  }

  @Test
  void testFinishHandler() {
    Set<String> finished = new TreeSet<>();
    profile.finish("source", null, null);
    assertEquals(Set.of(), finished);

    profile.registerHandler(
      (ForwardingProfile.FinishHandler) (sourceName, featureCollectors, emit) -> finished.add("1-" + sourceName));
    profile.finish("source", null, null);
    assertEquals(Set.of("1-source"), finished);

    finished.clear();

    profile.registerHandler(
      (ForwardingProfile.FinishHandler) (sourceName, featureCollectors, emit) -> finished.add("2-" + sourceName));
    profile.finish("source2", null, null);
    assertEquals(Set.of("1-source2", "2-source2"), finished);
  }

  @Test
  void testLayerPostProcesser() throws GeometryException {
    VectorTile.Feature feature = new VectorTile.Feature(
      "layer",
      1,
      VectorTile.encodeGeometry(GeoUtils.point(0, 0)),
      Map.of()
    );
    assertEquals(List.of(feature), profile.postProcessLayerFeatures("layer", 0, List.of(feature)));

    // ignore null response
    profile.registerHandler(new ForwardingProfile.LayerPostProcesser() {
      @Override
      public List<VectorTile.Feature> postProcess(int zoom, List<VectorTile.Feature> items) {
        return null;
      }

      @Override
      public String name() {
        return "a";
      }
    });
    assertEquals(List.of(feature), profile.postProcessLayerFeatures("a", 0, List.of(feature)));

    // allow mutations on initial input
    profile.registerHandler(new ForwardingProfile.LayerPostProcesser() {
      @Override
      public List<VectorTile.Feature> postProcess(int zoom, List<VectorTile.Feature> items) {
        items.set(0, items.getFirst());
        return null;
      }

      @Override
      public String name() {
        return "a";
      }
    });
    assertEquals(List.of(feature), profile.postProcessLayerFeatures("a", 0, List.of(feature)));

    // empty list removes
    profile.registerHandler(new ForwardingProfile.LayerPostProcesser() {
      @Override
      public List<VectorTile.Feature> postProcess(int zoom, List<VectorTile.Feature> items) {
        return List.of();
      }

      @Override
      public String name() {
        return "a";
      }
    });
    assertEquals(List.of(), profile.postProcessLayerFeatures("a", 0, List.of(feature)));
    // doesn't touch elements in another layer
    assertEquals(List.of(feature), profile.postProcessLayerFeatures("b", 0, List.of(feature)));

    // allow mutations on subsequent input
    profile.registerHandler(new ForwardingProfile.LayerPostProcesser() {
      @Override
      public List<VectorTile.Feature> postProcess(int zoom, List<VectorTile.Feature> items) {
        items.add(null);
        items.removeLast();
        return items;
      }

      @Override
      public String name() {
        return "a";
      }
    });
    assertEquals(List.of(), profile.postProcessLayerFeatures("a", 0, List.of(feature)));
    assertEquals(List.of(), profile.postProcessLayerFeatures("a", 0, new ArrayList<>(List.of(feature))));

    // 2 handlers for same layer run one after another
    var skip1 = new ForwardingProfile.LayerPostProcesser() {
      @Override
      public List<VectorTile.Feature> postProcess(int zoom, List<VectorTile.Feature> items) {
        return items.stream().skip(1).toList();
      }

      @Override
      public String name() {
        return "b";
      }
    };
    profile.registerHandler(skip1);
    profile.registerHandler(skip1);
    profile.registerHandler(new ForwardingProfile.LayerPostProcesser() {
      @Override
      public List<VectorTile.Feature> postProcess(int zoom, List<VectorTile.Feature> items) {
        return null; // ensure that returning null after initial post-processors run keeps the postprocessed result
      }

      @Override
      public String name() {
        return "b";
      }
    });
    assertEquals(List.of(feature, feature),
      profile.postProcessLayerFeatures("b", 0, List.of(feature, feature, feature, feature)));
    assertEquals(List.of(feature, feature, feature, feature),
      profile.postProcessLayerFeatures("c", 0, List.of(feature, feature, feature, feature)));
  }

  @Test
  void testTilePostProcesser() throws GeometryException {
    VectorTile.Feature feature = new VectorTile.Feature(
      "layer",
      1,
      VectorTile.encodeGeometry(GeoUtils.point(0, 0)),
      Map.of()
    );
    assertEquals(Map.of("layer", List.of(feature)), profile.postProcessTileFeatures(TileCoord.ofXYZ(0, 0, 0), Map.of(
      "layer", List.of(feature)
    )));

    // ignore null response
    profile.registerHandler((ForwardingProfile.TilePostProcessor) (tileCoord, layers) -> null);
    assertEquals(Map.of("a", List.of(feature)),
      profile.postProcessTileFeatures(TileCoord.ofXYZ(0, 0, 0), Map.of("a", List.of(feature))));

    // allow mutation on initial input
    profile.registerHandler((ForwardingProfile.TilePostProcessor) (tileCoord, layers) -> {
      if (layers.containsKey("a")) {
        var list = layers.get("a");
        var item = list.getFirst();
        list.set(0, item);
        layers.put("a", list);
      }
      return layers;
    });
    assertEquals(Map.of("a", List.of(feature)),
      profile.postProcessTileFeatures(TileCoord.ofXYZ(0, 0, 0), Map.of("a", List.of(feature))));
    assertEquals(Map.of("a", List.of(feature)),
      profile.postProcessTileFeatures(TileCoord.ofXYZ(0, 0, 0),
        new HashMap<>(Map.of("a", new ArrayList<>(List.of(feature))))));

    // empty map removes
    profile.registerHandler((ForwardingProfile.TilePostProcessor) (tileCoord, layers) -> Map.of());
    assertEquals(Map.of(),
      profile.postProcessTileFeatures(TileCoord.ofXYZ(0, 0, 0), Map.of("a", List.of(feature))));

    // allow mutation on subsequent inputs
    profile.registerHandler((ForwardingProfile.TilePostProcessor) (tileCoord, layers) -> {
      layers.put("a", List.of());
      layers.remove("a");
      return layers;
    });
    assertEquals(Map.of(),
      profile.postProcessTileFeatures(TileCoord.ofXYZ(0, 0, 0), Map.of("a", List.of(feature))));

    // also touches elements in another layer
    assertEquals(Map.of(),
      profile.postProcessTileFeatures(TileCoord.ofXYZ(0, 0, 0), Map.of("b", List.of(feature))));
  }

  @Test
  void testStackedTilePostProcessors() throws GeometryException {
    VectorTile.Feature feature = new VectorTile.Feature(
      "layer",
      1,
      VectorTile.encodeGeometry(GeoUtils.point(0, 0)),
      Map.of()
    );
    var skip1 = new ForwardingProfile.TilePostProcessor() {
      @Override
      public Map<String, List<VectorTile.Feature>> postProcessTile(TileCoord tileCoord,
        Map<String, List<VectorTile.Feature>> layers) {
        Map<String, List<VectorTile.Feature>> result = new HashMap<>();
        for (var key : layers.keySet()) {
          result.put(key, layers.get(key).stream().skip(1).toList());
        }
        return result;
      }
    };
    profile.registerHandler(skip1);
    profile.registerHandler(skip1);
    profile.registerHandler((ForwardingProfile.TilePostProcessor) (tileCoord, layers) -> {
      // ensure that returning null after initial post-processors run keeps the postprocessed result
      return null;
    });
    assertEquals(Map.of("b", List.of(feature, feature)),
      profile.postProcessTileFeatures(TileCoord.ofXYZ(0, 0, 0),
        Map.of("b", List.of(feature, feature, feature, feature))));
    assertEquals(Map.of("c", List.of(feature, feature)),
      profile.postProcessTileFeatures(TileCoord.ofXYZ(0, 0, 0),
        Map.of("c", List.of(feature, feature, feature, feature))));
  }


  @Test
  void testCaresAboutSource() {
    profile.registerSourceHandler("a", (x, y) -> {
    });
    assertTrue(profile.caresAboutSource("a"));
    assertFalse(profile.caresAboutSource("b"));

    profile.registerSourceHandler("b", (x, y) -> {
    });
    assertTrue(profile.caresAboutSource("a"));
    assertTrue(profile.caresAboutSource("b"));
    assertFalse(profile.caresAboutSource("c"));

    class C implements ForwardingProfile.Handler, ForwardingProfile.FeatureProcessor {

      @Override
      public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {}

      @Override
      public Expression filter() {
        return Expression.matchSource("c");
      }
    }
    profile.registerHandler(new C());
    assertTrue(profile.caresAboutSource("a"));
    assertTrue(profile.caresAboutSource("b"));
    assertTrue(profile.caresAboutSource("c"));
    assertFalse(profile.caresAboutSource("d"));

    profile.registerFeatureHandler((x, y) -> {
    });
    assertTrue(profile.caresAboutSource("d"));
    assertTrue(profile.caresAboutSource("e"));
  }

  @Test
  void registerAnySourceFeatureHandler() {
    SourceFeature a = SimpleFeature.create(GeoUtils.EMPTY_POINT, Map.of(), "srca", null, 1);
    SourceFeature b = SimpleFeature.create(GeoUtils.EMPTY_POINT, Map.of(), "srcb", null, 1);
    testFeatures(List.of(), a);
    testFeatures(List.of(), b);

    profile.registerFeatureHandler((elem, features) -> features.point("a"));
    testFeatures(List.of(Map.of(
      "_layer", "a"
    )), a);
    testFeatures(List.of(Map.of(
      "_layer", "a"
    )), b);
  }

  @Test
  void registerHandlerWithFilter() {
    SourceFeature a = SimpleFeature.create(GeoUtils.EMPTY_POINT, Map.of("key", "value"), "srca", null, 1);
    SourceFeature b = SimpleFeature.create(GeoUtils.EMPTY_POINT, Map.of(), "srcb", null, 1);
    testFeatures(List.of(), a);
    testFeatures(List.of(), b);

    profile.registerFeatureHandler(new ForwardingProfile.FeatureProcessor() {
      @Override
      public void processFeature(SourceFeature elem, FeatureCollector features) {
        features.point("a");
      }

      @Override
      public Expression filter() {
        return Expression.matchAny("key", "value");
      }
    });
    testFeatures(List.of(Map.of(
      "_layer", "a"
    )), a);
    testFeatures(List.of(), b);
  }

  @Test
  void registerHandlerTwice() {
    SourceFeature a = SimpleFeature.create(GeoUtils.EMPTY_POINT, Map.of(), "source", null, 1);
    testFeatures(List.of(), a);

    ForwardingProfile.FeatureProcessor processor = (elem, features) -> features.point("a");
    profile.registerHandler(processor);
    profile.registerSourceHandler("source", processor);
    testFeatures(List.of(Map.of(
      "_layer", "a"
    )), a);
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "--only-layers=water",
    "--exclude-layers=land",
    "--exclude-layers=land --only-layers=water,land",
  })
  void testLayerCliArgFilter(String args) {
    profile = new ForwardingProfile(PlanetilerConfig.from(Arguments.fromArgs(args.split(" ")))) {};
    record Processor(String name) implements ForwardingProfile.HandlerForLayer, ForwardingProfile.FeatureProcessor {

      @Override
      public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
        features.point(name);
      }
    }

    SourceFeature a = SimpleFeature.create(GeoUtils.EMPTY_POINT, Map.of("key", "value"), "source", "source layer", 1);
    profile.registerHandler(new Processor("water"));
    profile.registerHandler(new Processor("land"));
    testFeatures(List.of(Map.of(
      "_layer", "water"
    )), a);
  }

  @ParameterizedTest
  @CsvSource({
    "'--only-layers=water', water",
    "'--exclude-layers=land,transportation,transportation_name', water",
    // transportation excluded but transportation_name NOT => transportation will be processed
    "'--exclude-layers=land,transportation', water transportation_name transportation",
    "'--exclude-layers=land,transportation_name', water transportation",
    "'--exclude-layers=land --only-layers=water,land', water",
    // transportation excluded but transportation_name NOT => transportation will be processed
    "'--exclude-layers=transportation --only-layers=water,transportation,transportation_name', water transportation_name transportation",
    "'--exclude-layers=transportation_name --only-layers=water,transportation,transportation_name', water transportation",
    "'--exclude-layers=transportation,transportation_name --only-layers=water,transportation,transportation_name', water",
    // transportation excluded but transportation_name NOT => transportation will be processed
    "'--exclude-layers=transportation --only-layers=water,transportation_name', water transportation_name transportation",
    "'--exclude-layers=transportation_name --only-layers=water,transportation', water transportation",
  })
  void testLayerWithDepsCliArgFilter(String args, String expectedLayers) {
    profile = new ForwardingProfile(PlanetilerConfig.from(Arguments.fromArgs(args.split(" ")))) {
      @Override
      public Map<String, List<String>> dependsOnLayer() {
        return Map.of("transportation_name", List.of("transportation"));
      }
    };
    record Processor(String name) implements ForwardingProfile.HandlerForLayer, ForwardingProfile.FeatureProcessor {

      @Override
      public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
        features.point(name);
      }
    }

    SourceFeature a = SimpleFeature.create(GeoUtils.EMPTY_POINT, Map.of("key", "value"), "source", "source layer", 1);
    profile.registerHandler(new Processor("water"));
    profile.registerHandler(new Processor("transportation"));
    profile.registerHandler(new Processor("transportation_name"));
    profile.registerHandler(new Processor("land"));
    // profiles like OpenMapTiles will try to add "transportation" once again to cover for dependency
    profile.registerHandler(new Processor("transportation"));

    List<Map<String, Object>> expected = new ArrayList<>();
    for (var expectedLayer : expectedLayers.split(" ")) {
      expected.add(Map.of("_layer", expectedLayer));
    }

    testFeatures(expected, a);
  }
}
