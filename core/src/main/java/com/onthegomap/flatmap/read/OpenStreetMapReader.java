package com.onthegomap.flatmap.read;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.ObjectIntHashMap;
import com.graphhopper.coll.GHIntObjectHashMap;
import com.graphhopper.coll.GHLongHashSet;
import com.graphhopper.coll.GHLongObjectHashMap;
import com.graphhopper.coll.GHObjectIntHashMap;
import com.graphhopper.reader.ReaderElement;
import com.graphhopper.reader.ReaderElementUtils;
import com.graphhopper.reader.ReaderNode;
import com.graphhopper.reader.ReaderRelation;
import com.graphhopper.reader.ReaderWay;
import com.onthegomap.flatmap.CommonParams;
import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.MemoryEstimator;
import com.onthegomap.flatmap.Profile;
import com.onthegomap.flatmap.SourceFeature;
import com.onthegomap.flatmap.collections.FeatureGroup;
import com.onthegomap.flatmap.collections.FeatureSort;
import com.onthegomap.flatmap.collections.LongLongMap;
import com.onthegomap.flatmap.collections.LongLongMultimap;
import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.monitoring.Counter;
import com.onthegomap.flatmap.monitoring.ProgressLoggers;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.render.FeatureRenderer;
import com.onthegomap.flatmap.worker.Topology;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.jetbrains.annotations.Nullable;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateList;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import org.locationtech.jts.geom.impl.PackedCoordinateSequence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenStreetMapReader implements Closeable, MemoryEstimator.HasEstimate {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpenStreetMapReader.class);

  private final OsmSource osmInputFile;
  private final Stats stats;
  private final LongLongMap nodeDb;
  private final Counter.Readable PASS1_NODES = Counter.newSingleThreadCounter();
  private final Counter.Readable PASS1_WAYS = Counter.newSingleThreadCounter();
  private final Counter.Readable PASS1_RELATIONS = Counter.newSingleThreadCounter();
  private final Profile profile;
  private final String name;

  // need a few large objects to process ways in relations, should be small enough to keep in memory
  // for routes (750k rels 40m ways) and boundaries (650k rels, 8m ways)
  // need to store route info to use later when processing ways
  // <~500mb
  private GHLongObjectHashMap<RelationInfo> relationInfo = new GHLongObjectHashMap<>();
  private final AtomicLong relationInfoSizes = new AtomicLong(0);
  // ~800mb, ~1.6GB when sorting
  private LongLongMultimap wayToRelations = LongLongMultimap.newSparseUnorderedMultimap();
  // for multipolygons need to store way info (20m ways, 800m nodes) to use when processing relations (4.5m)
  // ~300mb
  private LongHashSet waysInMultipolygon = new GHLongHashSet();
  // ~7GB
  private LongLongMultimap multipolygonWayGeometries = LongLongMultimap.newDensedOrderedMultimap();

  public OpenStreetMapReader(OsmSource osmInputFile, LongLongMap nodeDb, Profile profile, Stats stats) {
    this("osm", osmInputFile, nodeDb, profile, stats);
  }

  public OpenStreetMapReader(String name, OsmSource osmInputFile, LongLongMap nodeDb, Profile profile,
    Stats stats) {
    this.name = name;
    this.osmInputFile = osmInputFile;
    this.nodeDb = nodeDb;
    this.stats = stats;
    this.profile = profile;
    stats.monitorInMemoryObject("osm_relations", this);
    stats.counter("osm_pass1_elements_processed", "type", () -> Map.of(
      "nodes", PASS1_NODES,
      "ways", PASS1_WAYS,
      "relations", PASS1_RELATIONS
    ));
  }

  public void pass1(CommonParams config) {
    var timer = stats.startTimer(name + "_pass1");
    String pbfParsePrefix = "pbfpass1";
    var topology = Topology.start(pbfParsePrefix, stats)
      .fromGenerator("pbf", osmInputFile.read("pbfpass1", config.threads() - 1))
      .addBuffer("reader_queue", 50_000, 10_000)
      .sinkToConsumer("process", 1, this::processPass1);

    var loggers = new ProgressLoggers("osm_pass1")
      .addRateCounter("nodes", PASS1_NODES)
      .addFileSize(nodeDb::fileSize)
      .addRateCounter("ways", PASS1_WAYS)
      .addRateCounter("rels", PASS1_RELATIONS)
      .addProcessStats()
      .addInMemoryObject("hppc", this)
      .addThreadPoolStats("parse", pbfParsePrefix)
      .addTopologyStats(topology);
    topology.awaitAndLog(loggers, config.logInterval());
    timer.stop();
  }

  long nodesInWays = 0;

  void processPass1(ReaderElement readerElement) {
    if (readerElement instanceof ReaderNode node) {
      PASS1_NODES.inc();
      nodeDb.put(node.getId(), GeoUtils.encodeFlatLocation(node.getLon(), node.getLat()));
    } else if (readerElement instanceof ReaderWay way) {
      PASS1_WAYS.inc();
      nodesInWays += way.getNodes().size();
    } else if (readerElement instanceof ReaderRelation rel) {
      PASS1_RELATIONS.inc();
      List<RelationInfo> infos = profile.preprocessOsmRelation(rel);
      if (infos != null) {
        for (RelationInfo info : infos) {
          relationInfo.put(rel.getId(), info);
          relationInfoSizes.addAndGet(info.estimateMemoryUsageBytes());
          for (ReaderRelation.Member member : rel.getMembers()) {
            int type = member.getType();
            if (type == ReaderRelation.Member.WAY || type == ReaderRelation.Member.RELATION) {
              wayToRelations.put(member.getRef(), new RelationMembership(member.getRole(), rel.getId()).encode());
            }
          }
        }
      }
      if (rel.hasTag("type", "multipolygon")) {
        for (ReaderRelation.Member member : rel.getMembers()) {
          if (member.getType() == ReaderRelation.Member.WAY) {
            waysInMultipolygon.add(member.getRef());
          }
        }
      }
    }
  }

  public void pass2(FeatureGroup writer, CommonParams config) {
    var timer = stats.startTimer(name + "_pass2");
    int readerThreads = Math.max(config.threads() / 4, 1);
    int processThreads = config.threads() - 1;
    Counter.MultiThreadCounter nodesProcessed = Counter.newMultiThreadCounter();
    Counter.MultiThreadCounter waysProcessed = Counter.newMultiThreadCounter();
    Counter.MultiThreadCounter relsProcessed = Counter.newMultiThreadCounter();
    stats.counter("osm_pass2_elements_processed", "type", () -> Map.of(
      "nodes", nodesProcessed,
      "ways", waysProcessed,
      "relations", relsProcessed
    ));

    CountDownLatch waysDone = new CountDownLatch(processThreads);

    String parseThreadPrefix = "pbfpass2";
    var topology = Topology.start("osm_pass2", stats)
      .fromGenerator("pbf", osmInputFile.read(parseThreadPrefix, readerThreads))
      .addBuffer("reader_queue", 50_000, 1_000)
      .<FeatureSort.Entry>addWorker("process", processThreads, (prev, next) -> {
        Counter nodes = nodesProcessed.counterForThread();
        Counter ways = waysProcessed.counterForThread();
        Counter rels = relsProcessed.counterForThread();

        ReaderElement readerElement;
        var featureCollectors = new FeatureCollector.Factory(config, stats);
        NodeLocationProvider nodeCache = newNodeGeometryCache();
        FeatureRenderer renderer = getFeatureRenderer(writer, config, next);
        while ((readerElement = prev.get()) != null) {
          SourceFeature feature = null;
          if (readerElement instanceof ReaderNode node) {
            nodes.inc();
            feature = processNodePass2(node);
          } else if (readerElement instanceof ReaderWay way) {
            ways.inc();
            feature = processWayPass2(nodeCache, way);
          } else if (readerElement instanceof ReaderRelation rel) {
            // ensure all ways finished processing before we start relations
            if (waysDone.getCount() > 0) {
              waysDone.countDown();
              waysDone.await();
            }
            rels.inc();
            feature = processRelationPass2(rel, nodeCache);
          }
          if (feature != null) {
            FeatureCollector features = featureCollectors.get(feature);
            profile.processFeature(feature, features);
            for (FeatureCollector.Feature renderable : features) {
              renderer.accept(renderable);
            }
          }
          nodeCache.reset();
        }

        // just in case a worker skipped over all relations
        waysDone.countDown();
      }).addBuffer("feature_queue", 50_000, 1_000)
      .sinkToConsumer("write", 1, writer);

    var logger = new ProgressLoggers("osm_pass2")
      .addRatePercentCounter("nodes", PASS1_NODES.get(), nodesProcessed)
      .addFileSize(nodeDb::fileSize)
      .addRatePercentCounter("ways", PASS1_WAYS.get(), waysProcessed)
      .addRatePercentCounter("rels", PASS1_RELATIONS.get(), relsProcessed)
      .addRateCounter("features", () -> writer.sorter().size())
      .addFileSize(writer::getStorageSize)
      .addProcessStats()
      .addInMemoryObject("hppc", this)
      .addThreadPoolStats("parse", parseThreadPrefix)
      .addTopologyStats(topology);

    topology.awaitAndLog(logger, config.logInterval());

    profile.finish(name,
      new FeatureCollector.Factory(config, stats),
      getFeatureRenderer(writer, config, writer));
    timer.stop();
  }

  private FeatureRenderer getFeatureRenderer(FeatureGroup writer, CommonParams config,
    Consumer<FeatureSort.Entry> next) {
    var encoder = writer.newRenderedFeatureEncoder();
    return new FeatureRenderer(
      config,
      rendered -> next.accept(encoder.apply(rendered)),
      stats
    );
  }

  SourceFeature processRelationPass2(ReaderRelation rel, NodeLocationProvider nodeCache) {
    if (rel.hasTag("type", "multipolygon")) {
      List<RelationMember<RelationInfo>> parentRelations = getRelationMembership(rel.getId());
      return new MultipolygonSourceFeature(rel, nodeCache, parentRelations);
    } else {
      return null;
    }
  }

  SourceFeature processWayPass2(NodeLocationProvider nodeCache, ReaderWay way) {
    LongArrayList nodes = way.getNodes();
    if (waysInMultipolygon.contains(way.getId())) {
      synchronized (multipolygonWayGeometries) {
        multipolygonWayGeometries.putAll(way.getId(), nodes);
      }
    }
    boolean closed = nodes.size() > 1 && nodes.get(0) == nodes.get(nodes.size() - 1);
    String area = way.getTag("area");
    List<RelationMember<RelationInfo>> rels = getRelationMembership(way.getId());
    return new WaySourceFeature(way, closed, area, nodeCache, rels);
  }

  @Nullable
  private List<RelationMember<RelationInfo>> getRelationMembership(long id) {
    LongArrayList relationIds = wayToRelations.get(id);
    List<RelationMember<RelationInfo>> rels = null;
    if (!relationIds.isEmpty()) {
      rels = new ArrayList<>(relationIds.size());
      for (int r = 0; r < relationIds.size(); r++) {
        long encoded = relationIds.get(r);
        RelationMembership parsed = RelationMembership.parse(encoded);
        RelationInfo rel = relationInfo.get(parsed.relationId);
        if (rel != null) {
          rels.add(new RelationMember<>(parsed.role, rel));
        }
      }
    }
    return rels;
  }

  SourceFeature processNodePass2(ReaderNode node) {
    return new NodeSourceFeature(node);
  }

  @Override
  public long estimateMemoryUsageBytes() {
    long size = 0;
    size += MemoryEstimator.size(waysInMultipolygon);
    size += MemoryEstimator.size(multipolygonWayGeometries);
    size += MemoryEstimator.size(wayToRelations);
    size += MemoryEstimator.sizeWithoutValues(relationInfo);
    size += MemoryEstimator.sizeWithoutValues(roleIdsReverse);
    size += MemoryEstimator.sizeWithoutKeys(roleIds);
    size += roleSizes.get();
    size += relationInfoSizes.get();
    return size;
  }


  @Override
  public void close() throws IOException {
    multipolygonWayGeometries = null;
    wayToRelations = null;
    waysInMultipolygon = null;
    relationInfo = null;
    roleIds.release();
    roleIdsReverse.release();
    nodeDb.close();
  }

  public static record RelationMember<T extends RelationInfo>(String role, T relation) {}

  private static final ObjectIntHashMap<String> roleIds = new GHObjectIntHashMap<>();
  private static final IntObjectHashMap<String> roleIdsReverse = new GHIntObjectHashMap<>();
  private static final AtomicLong roleSizes = new AtomicLong(0);
  private static final int ROLE_BITS = 16;
  private static final int MAX_ROLES = (1 << ROLE_BITS) - 10;
  private static final int ROLE_SHIFT = 64 - ROLE_BITS;
  private static final int ROLE_MASK = (1 << ROLE_BITS) - 1;
  private static final long NOT_ROLE_MASK = (1L << ROLE_SHIFT) - 1L;

  private record RelationMembership(String role, long relationId) {

    public static RelationMembership parse(long encoded) {
      int role = (int) ((encoded >>> ROLE_SHIFT) & ROLE_MASK);
      return new RelationMembership(roleIdsReverse.get(role), encoded & NOT_ROLE_MASK);
    }

    public long encode() {
      int roleId = roleIds.getOrDefault(role, -1);
      if (roleId == -1) {
        roleSizes.addAndGet(MemoryEstimator.size(role));
        roleId = roleIds.size() + 1;
        roleIds.put(role, roleId);
        roleIdsReverse.put(roleId, role);
        if (roleId > MAX_ROLES) {
          throw new IllegalStateException("Too many roles to encode: " + role);
        }
      }
      return relationId | ((long) roleId << ROLE_SHIFT);
    }
  }

  public interface RelationInfo extends MemoryEstimator.HasEstimate {

    long id();

    @Override
    default long estimateMemoryUsageBytes() {
      return 0;
    }
  }

  private abstract class ProxyFeature extends SourceFeature {

    final boolean polygon;
    final boolean line;
    final boolean point;

    public ProxyFeature(ReaderElement elem, boolean point, boolean line, boolean polygon,
      List<RelationMember<RelationInfo>> relationInfo) {
      super(ReaderElementUtils.getProperties(elem), name, null, relationInfo, elem.getId());
      this.point = point;
      this.line = line;
      this.polygon = polygon;
    }

    private Geometry latLonGeom;

    @Override
    public Geometry latLonGeometry() throws GeometryException {
      return latLonGeom != null ? latLonGeom : (latLonGeom = GeoUtils.worldToLatLonCoords(worldGeometry()));
    }

    private Geometry worldGeom;

    @Override
    public Geometry worldGeometry() throws GeometryException {
      return worldGeom != null ? worldGeom : (worldGeom = computeWorldGeometry());
    }

    protected abstract Geometry computeWorldGeometry() throws GeometryException;

    @Override
    public boolean isPoint() {
      return point;
    }

    @Override
    public boolean canBeLine() {
      return line;
    }

    @Override
    public boolean canBePolygon() {
      return polygon;
    }
  }

  private class NodeSourceFeature extends ProxyFeature {

    private final double lon;
    private final double lat;

    NodeSourceFeature(ReaderNode node) {
      super(node, true, false, false, null);
      this.lon = node.getLon();
      this.lat = node.getLat();
    }

    @Override
    protected Geometry computeWorldGeometry() {
      return GeoUtils.point(
        GeoUtils.getWorldX(lon),
        GeoUtils.getWorldY(lat)
      );
    }

    @Override
    public boolean isPoint() {
      return true;
    }

    @Override
    public String toString() {
      return "OsmNode[" + id() + ']';
    }
  }

  private class WaySourceFeature extends ProxyFeature {

    private final NodeLocationProvider nodeCache;
    private final LongArrayList nodeIds;

    public WaySourceFeature(ReaderWay way, boolean closed, String area, NodeLocationProvider nodeCache,
      List<RelationMember<RelationInfo>> relationInfo) {
      super(way, false,
        (!closed || !"yes".equals(area)) && way.getNodes().size() >= 2,
        (closed && !"no".equals(area)) && way.getNodes().size() >= 4,
        relationInfo
      );
      this.nodeIds = way.getNodes();
      this.nodeCache = nodeCache;
    }

    @Override
    protected Geometry computeLine() throws GeometryException {
      try {
        CoordinateSequence coords = nodeCache.getWayGeometry(nodeIds);
        return GeoUtils.JTS_FACTORY.createLineString(coords);
      } catch (IllegalArgumentException e) {
        throw new GeometryException("osm_invalid_line", "Error building line for way " + id() + ": " + e);
      }
    }

    @Override
    protected Geometry computePolygon() throws GeometryException {
      try {
        CoordinateSequence coords = nodeCache.getWayGeometry(nodeIds);
        return GeoUtils.JTS_FACTORY.createPolygon(coords);
      } catch (IllegalArgumentException e) {
        throw new GeometryException("osm_invalid_polygon", "Error building polygon for way " + id() + ": " + e);
      }
    }

    @Override
    protected Geometry computeWorldGeometry() throws GeometryException {
      return canBePolygon() ? polygon() : line();
    }

    @Override
    public String toString() {
      return "OsmWay[" + id() + ']';
    }
  }

  private class MultipolygonSourceFeature extends ProxyFeature {

    private final ReaderRelation relation;
    private final NodeLocationProvider nodeCache;

    public MultipolygonSourceFeature(ReaderRelation relation, NodeLocationProvider nodeCache,
      List<RelationMember<RelationInfo>> parentRelations) {
      super(relation, false, false, true, parentRelations);
      this.relation = relation;
      this.nodeCache = nodeCache;
    }

    @Override
    protected Geometry computeWorldGeometry() throws GeometryException {
      List<LongArrayList> rings = new ArrayList<>(relation.getMembers().size());
      for (ReaderRelation.Member member : relation.getMembers()) {
        String role = member.getRole();
        LongArrayList poly = multipolygonWayGeometries.get(member.getRef());
        if (member.getType() == ReaderRelation.Member.WAY) {
          if (poly != null && !poly.isEmpty()) {
            rings.add(poly);
          } else {
            LOGGER.warn("Missing " + role + " OsmWay[" + member.getRef() + "] for multipolygon " + this);
          }
        }
      }
      return OsmMultipolygon.build(rings, nodeCache, id());
    }

    @Override
    public String toString() {
      return "OsmRelation[" + id() + ']';
    }
  }

  NodeLocationProvider newNodeGeometryCache() {
    return new NodeGeometryCache();
  }

  public interface NodeLocationProvider {

    default CoordinateSequence getWayGeometry(LongArrayList nodeIds) {
      CoordinateList coordList = new CoordinateList();
      for (var cursor : nodeIds) {
        coordList.add(getCoordinate(cursor.value));
      }
      return new CoordinateArraySequence(coordList.toCoordinateArray());
    }

    Coordinate getCoordinate(long id);

    default void reset() {
    }
  }

  private class NodeGeometryCache implements NodeLocationProvider {

    @Override
    public Coordinate getCoordinate(long id) {
      long encoded = nodeDb.get(id);
      if (encoded == LongLongMap.MISSING_VALUE) {
        throw new IllegalArgumentException("Missing location for node: " + id);
      }
      return new CoordinateXY(GeoUtils.decodeWorldX(encoded), GeoUtils.decodeWorldY(encoded));
    }

    @Override
    public CoordinateSequence getWayGeometry(LongArrayList nodeIds) {
      int num = nodeIds.size();
      CoordinateSequence seq = new PackedCoordinateSequence.Double(nodeIds.size(), 2, 0);

      for (int i = 0; i < num; i++) {
        long encoded = nodeDb.get(nodeIds.get(i));
        if (encoded == LongLongMap.MISSING_VALUE) {
          throw new IllegalArgumentException("Missing location for node: " + nodeIds.get(i));
        }
        seq.setOrdinate(i, 0, GeoUtils.decodeWorldX(encoded));
        seq.setOrdinate(i, 1, GeoUtils.decodeWorldY(encoded));
      }
      return seq;
    }
  }

  public void reset() {
  }
}
