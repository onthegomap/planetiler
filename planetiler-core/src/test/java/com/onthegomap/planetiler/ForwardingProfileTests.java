package com.onthegomap.planetiler;

import static com.onthegomap.planetiler.TestUtils.assertSubmap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SimpleFeature;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmRelationInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

public class ForwardingProfileTests {

  private final ForwardingProfile profile =
      new ForwardingProfile() {
        @Override
        public String name() {
          return "test";
        }
      };

  @Test
  public void testPreprocessOsmNode() {
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
  public void testPreprocessOsmWay() {
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
  public void testPreprocessOsmRelation() {
    record RelA(@Override long id) implements OsmRelationInfo {}
    record RelB(@Override long id) implements OsmRelationInfo {}
    assertNull(profile.preprocessOsmRelation(new OsmElement.Relation(1)));
    profile.registerHandler(
        (ForwardingProfile.OsmRelationPreprocessor) relation -> List.of(new RelA(relation.id())));
    assertEquals(List.of(new RelA(1)), profile.preprocessOsmRelation(new OsmElement.Relation(1)));
    profile.registerHandler((ForwardingProfile.OsmRelationPreprocessor) relation -> null);
    profile.registerHandler(
        (ForwardingProfile.OsmRelationPreprocessor) relation -> List.of(new RelB(relation.id())));
    assertEquals(
        List.of(new RelA(1), new RelB(1)),
        profile.preprocessOsmRelation(new OsmElement.Relation(1)));
  }

  private void testFeatures(List<Map<String, Object>> expected, SourceFeature feature) {
    List<FeatureCollector.Feature> actualList = TestUtils.processSourceFeature(feature, profile);
    assertEquals(expected.size(), actualList.size(), () -> "size: " + actualList);
    for (int i = 0; i < expected.size(); i++) {
      assertSubmap(expected.get(i), TestUtils.toMap(actualList.get(i), 14));
    }
  }

  @Test
  public void testProcessFeature() {
    SourceFeature a = SimpleFeature.create(GeoUtils.EMPTY_POINT, Map.of(), "srca", null, 1);
    SourceFeature b = SimpleFeature.create(GeoUtils.EMPTY_POINT, Map.of(), "srcb", null, 1);
    testFeatures(List.of(), a);
    testFeatures(List.of(), b);

    profile.registerSourceHandler(a.getSource(), (elem, features) -> features.point("a"));
    testFeatures(List.of(Map.of("_layer", "a")), a);
    testFeatures(List.of(), b);

    profile.registerSourceHandler(b.getSource(), (elem, features) -> features.point("b"));
    testFeatures(List.of(Map.of("_layer", "a")), a);
    testFeatures(List.of(Map.of("_layer", "b")), b);

    profile.registerSourceHandler(a.getSource(), (elem, features) -> features.point("a2"));
    testFeatures(List.of(Map.of("_layer", "a"), Map.of("_layer", "a2")), a);
    testFeatures(List.of(Map.of("_layer", "b")), b);
  }

  @Test
  public void testFinishHandler() {
    Set<String> finished = new TreeSet<>();
    profile.finish("source", null, null);
    assertEquals(Set.of(), finished);

    profile.registerHandler(
        (ForwardingProfile.FinishHandler)
            (sourceName, featureCollectors, emit) -> finished.add("1-" + sourceName));
    profile.finish("source", null, null);
    assertEquals(Set.of("1-source"), finished);

    finished.clear();

    profile.registerHandler(
        (ForwardingProfile.FinishHandler)
            (sourceName, featureCollectors, emit) -> finished.add("2-" + sourceName));
    profile.finish("source2", null, null);
    assertEquals(Set.of("1-source2", "2-source2"), finished);
  }

  @Test
  public void testFeaturePostProcessor() throws GeometryException {
    VectorTile.Feature feature =
        new VectorTile.Feature(
            "layer", 1, VectorTile.encodeGeometry(GeoUtils.point(0, 0)), Map.of());
    assertEquals(List.of(feature), profile.postProcessLayerFeatures("layer", 0, List.of(feature)));

    // ignore null response
    profile.registerHandler(
        new ForwardingProfile.FeaturePostProcessor() {
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

    // empty list removes
    profile.registerHandler(
        new ForwardingProfile.FeaturePostProcessor() {
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

    // 2 handlers for same layer run one after another
    var skip1 =
        new ForwardingProfile.FeaturePostProcessor() {
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
    profile.registerHandler(
        new ForwardingProfile.FeaturePostProcessor() {
          @Override
          public List<VectorTile.Feature> postProcess(int zoom, List<VectorTile.Feature> items) {
            return null; // ensure that returning null after initial post-processors run keeps the
                         // postprocessed result
          }

          @Override
          public String name() {
            return "b";
          }
        });
    assertEquals(
        List.of(feature, feature),
        profile.postProcessLayerFeatures("b", 0, List.of(feature, feature, feature, feature)));
    assertEquals(
        List.of(feature, feature, feature, feature),
        profile.postProcessLayerFeatures("c", 0, List.of(feature, feature, feature, feature)));
  }
}
