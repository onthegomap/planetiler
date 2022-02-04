package com.onthegomap.planetiler.reader.osm;

import static com.onthegomap.planetiler.util.MemoryEstimator.estimateSize;
import static com.onthegomap.planetiler.util.MemoryEstimator.estimateSizeWithoutKeys;
import static com.onthegomap.planetiler.util.MemoryEstimator.estimateSizeWithoutValues;

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
import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.collection.LongLongMap;
import com.onthegomap.planetiler.collection.LongLongMultimap;
import com.onthegomap.planetiler.collection.SortableFeature;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.render.FeatureRenderer;
import com.onthegomap.planetiler.stats.Counter;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.MemoryEstimator;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateList;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import org.locationtech.jts.geom.impl.PackedCoordinateSequence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility that constructs {@link SourceFeature SourceFeatures} from the raw nodes, ways, and relations contained in an
 * {@link OsmInputFile}.
 */
public class OsmReader implements Closeable, MemoryEstimator.HasEstimate {

  private static final Logger LOGGER = LoggerFactory.getLogger(OsmReader.class);
  private static final int ROLE_BITS = 16;
  private static final int MAX_ROLES = (1 << ROLE_BITS) - 10;
  private static final int ROLE_SHIFT = 64 - ROLE_BITS;
  private static final int ROLE_MASK = (1 << ROLE_BITS) - 1;
  private static final long NOT_ROLE_MASK = (1L << ROLE_SHIFT) - 1L;
  private final OsmSource osmInputFile;
  private final Stats stats;
  private final LongLongMap nodeLocationDb;
  private final Counter.Readable PASS1_NODES = Counter.newSingleThreadCounter();
  private final Counter.Readable PASS1_WAYS = Counter.newSingleThreadCounter();
  private final Counter.Readable PASS1_RELATIONS = Counter.newSingleThreadCounter();
  private final Profile profile;
  private final String name;
  private final AtomicLong relationInfoSizes = new AtomicLong(0);
  // need a few large objects to process ways in relations, should be small enough to keep in memory
  // for routes (750k rels 40m ways) and boundaries (650k rels, 8m ways)
  // need to store route info to use later when processing ways
  // <~500mb
  private GHLongObjectHashMap<OsmRelationInfo> relationInfo = new GHLongObjectHashMap<>();
  // ~800mb, ~1.6GB when sorting
  private LongLongMultimap wayToRelations = LongLongMultimap.newSparseUnorderedMultimap();
  // for multipolygons need to store way info (20m ways, 800m nodes) to use when processing relations (4.5m)
  // ~300mb
  private LongHashSet waysInMultipolygon = new GHLongHashSet();
  // ~7GB
  private LongLongMultimap multipolygonWayGeometries = LongLongMultimap.newDensedOrderedMultimap();
  // keep track of data needed to encode/decode role strings into a long
  private final ObjectIntHashMap<String> roleIds = new GHObjectIntHashMap<>();
  private final IntObjectHashMap<String> roleIdsReverse = new GHIntObjectHashMap<>();
  private final AtomicLong roleSizes = new AtomicLong(0);

  /**
   * Constructs a new {@code OsmReader} from
   *
   * @param name           ID for this reader to use in stats and logs
   * @param osmInputFile   the file to read raw nodes, ways, and relations from
   * @param nodeLocationDb store that will temporarily hold node locations (encoded as a long) between passes to
   *                       reconstruct way geometries
   * @param profile        logic that defines what map features to emit for each source feature
   * @param stats          to keep track of counters and timings
   */
  public OsmReader(String name, OsmSource osmInputFile, LongLongMap nodeLocationDb, Profile profile,
    Stats stats) {
    this.name = name;
    this.osmInputFile = osmInputFile;
    this.nodeLocationDb = nodeLocationDb;
    this.stats = stats;
    this.profile = profile;
    stats.monitorInMemoryObject("osm_relations", this);
    stats.counter("osm_pass1_elements_processed", "type", () -> Map.of(
      "nodes", PASS1_NODES,
      "ways", PASS1_WAYS,
      "relations", PASS1_RELATIONS
    ));
  }

  /**
   * Pre-processes all OSM elements before {@link #pass2(FeatureGroup, PlanetilerConfig)} is used to emit map features.
   * <p>
   * Stores node locations for pass2 to use to reconstruct way geometries.
   * <p>
   * Also stores the result of {@link Profile#preprocessOsmRelation(OsmElement.Relation)} so that pass2 can know the
   * relevant relations that a way belongs to.
   *
   * @param config user-provided arguments to control the number of threads, and log interval
   */
  public void pass1(PlanetilerConfig config) {
    var timer = stats.startStage("osm_pass1");
    String pbfParsePrefix = "pbfpass1";
    int parseThreads = Math.max(1, config.threads() - 2);
    var pipeline = WorkerPipeline.start("osm_pass1", stats)
      .fromGenerator("pbf", osmInputFile.read("pbfpass1", parseThreads))
      .addBuffer("reader_queue", 50_000, 10_000)
      .sinkToConsumer("process", 1, this::processPass1Element);

    var loggers = ProgressLoggers.create()
      .addRateCounter("nodes", PASS1_NODES, true)
      .addFileSizeAndRam(nodeLocationDb)
      .addRateCounter("ways", PASS1_WAYS, true)
      .addRateCounter("rels", PASS1_RELATIONS, true)
      .newLine()
      .addProcessStats()
      .addInMemoryObject("hppc", this)
      .newLine()
      .addThreadPoolStats("parse", pbfParsePrefix + "-pool")
      .addPipelineStats(pipeline);
    pipeline.awaitAndLog(loggers, config.logInterval());
    timer.stop();
  }

  void processPass1Element(ReaderElement readerElement) {
    // only a single thread calls this with elements ordered by ID, so it's safe to manipulate these
    // shared data structures which are not thread safe
    if (readerElement.getId() < 0) {
      throw new IllegalArgumentException("Negative OSM element IDs not supported: " + readerElement);
    }
    if (readerElement instanceof ReaderNode node) {
      PASS1_NODES.inc();
      try {
        profile.preprocessOsmNode(OsmElement.fromGraphopper(node));
      } catch (Exception e) {
        LOGGER.error("Error preprocessing OSM node " + node.getId(), e);
      }
      // TODO allow limiting node storage to only ones that profile cares about
      nodeLocationDb.put(node.getId(), GeoUtils.encodeFlatLocation(node.getLon(), node.getLat()));
    } else if (readerElement instanceof ReaderWay way) {
      PASS1_WAYS.inc();
      try {
        profile.preprocessOsmWay(OsmElement.fromGraphopper(way));
      } catch (Exception e) {
        LOGGER.error("Error preprocessing OSM way " + way.getId(), e);
      }
    } else if (readerElement instanceof ReaderRelation rel) {
      PASS1_RELATIONS.inc();
      // don't leak graphhopper classes out through public API
      OsmElement.Relation osmRelation = OsmElement.fromGraphopper(rel);
      try {
        List<OsmRelationInfo> infos = profile.preprocessOsmRelation(osmRelation);
        if (infos != null) {
          for (OsmRelationInfo info : infos) {
            relationInfo.put(rel.getId(), info);
            relationInfoSizes.addAndGet(info.estimateMemoryUsageBytes());
            for (ReaderRelation.Member member : rel.getMembers()) {
              int type = member.getType();
              // TODO handle nodes in relations and super-relations
              if (type == ReaderRelation.Member.WAY) {
                wayToRelations.put(member.getRef(), encodeRelationMembership(member.getRole(), rel.getId()));
              }
            }
          }
        }
      } catch (Exception e) {
        LOGGER.error("Error preprocessing OSM relation " + rel.getId(), e);
      }
      // TODO allow limiting multipolygon storage to only ones that profile cares about
      if (isMultipolygon(rel)) {
        for (ReaderRelation.Member member : rel.getMembers()) {
          if (member.getType() == ReaderRelation.Member.WAY) {
            waysInMultipolygon.add(member.getRef());
          }
        }
      }
    }
  }

  private static boolean isMultipolygon(ReaderRelation relation) {
    return relation.hasTag("type", "multipolygon", "boundary", "land_area");
  }

  /**
   * Constructs geometries from OSM elements and emits map features as defined by the {@link Profile}.
   *
   * @param writer consumer that will store finished features
   * @param config user-provided arguments to control the number of threads, and log interval
   */
  public void pass2(FeatureGroup writer, PlanetilerConfig config) {
    var timer = stats.startStage("osm_pass2");
    int threads = config.threads();
    int readerThreads = Math.max(threads / 4, 1);
    int processThreads = threads - (threads >= 4 ? 1 : 0);
    Counter.MultiThreadCounter nodesProcessed = Counter.newMultiThreadCounter();
    Counter.MultiThreadCounter waysProcessed = Counter.newMultiThreadCounter();
    Counter.MultiThreadCounter relsProcessed = Counter.newMultiThreadCounter();
    stats.counter("osm_pass2_elements_processed", "type", () -> Map.of(
      "nodes", nodesProcessed,
      "ways", waysProcessed,
      "relations", relsProcessed
    ));

    // since multiple threads process OSM elements, and we must process all ways before processing any relations,
    // use a count down latch to wait for all threads to finish processing ways
    CountDownLatch waysDone = new CountDownLatch(processThreads);

    String parseThreadPrefix = "pbfpass2";
    var pipeline = WorkerPipeline.start("osm_pass2", stats)
      .fromGenerator("pbf", osmInputFile.read(parseThreadPrefix, readerThreads))
      // TODO should use an adaptive batch size to better utilize lots of cpus:
      //   - make queue size proportional to cores
      //   - much larger batches when processing points
      //   - slightly larger batches when processing ways
      //   - 1_000 is probably fine for relations
      .addBuffer("reader_queue", 50_000, 1_000)
      .<SortableFeature>addWorker("process", processThreads, (prev, next) -> {
        // avoid contention trying to get the thread-local counters by getting them once when thread starts
        Counter nodes = nodesProcessed.counterForThread();
        Counter ways = waysProcessed.counterForThread();
        Counter rels = relsProcessed.counterForThread();

        ReaderElement readerElement;
        var featureCollectors = new FeatureCollector.Factory(config, stats);
        NodeLocationProvider nodeLocations = newNodeLocationProvider();
        FeatureRenderer renderer = createFeatureRenderer(writer, config, next);
        while ((readerElement = prev.get()) != null) {
          SourceFeature feature = null;
          if (readerElement instanceof ReaderNode node) {
            nodes.inc();
            feature = processNodePass2(node);
          } else if (readerElement instanceof ReaderWay way) {
            ways.inc();
            feature = processWayPass2(way, nodeLocations);
          } else if (readerElement instanceof ReaderRelation rel) {
            // ensure all ways finished processing before we start relations
            if (waysDone.getCount() > 0) {
              waysDone.countDown();
              waysDone.await();
            }
            rels.inc();
            feature = processRelationPass2(rel, nodeLocations);
          }
          // render features specified by profile and hand them off to next step that will
          // write them intermediate storage
          if (feature != null) {
            FeatureCollector features = featureCollectors.get(feature);
            try {
              profile.processFeature(feature, features);
              for (FeatureCollector.Feature renderable : features) {
                renderer.accept(renderable);
              }
            } catch (Exception e) {
              String type = switch (readerElement.getType()) {
                case ReaderElement.NODE -> "node";
                case ReaderElement.WAY -> "way";
                case ReaderElement.RELATION -> "relation";
                default -> "element";
              };
              LOGGER.error("Error processing OSM " + type + " " + readerElement.getId(), e);
            }
          }
        }

        // just in case a worker skipped over all relations
        waysDone.countDown();
      }).addBuffer("feature_queue", 50_000, 1_000)
      // FeatureGroup writes need to be single-threaded
      .sinkToConsumer("write", 1, writer);

    var logger = ProgressLoggers.create()
      .addRatePercentCounter("nodes", PASS1_NODES.get(), nodesProcessed)
      .addFileSizeAndRam(nodeLocationDb)
      .addRatePercentCounter("ways", PASS1_WAYS.get(), waysProcessed)
      .addRatePercentCounter("rels", PASS1_RELATIONS.get(), relsProcessed)
      .addRateCounter("features", writer::numFeaturesWritten)
      .addFileSize(writer)
      .newLine()
      .addProcessStats()
      .addInMemoryObject("hppc", this)
      .newLine()
      .addThreadPoolStats("parse", parseThreadPrefix + "-pool")
      .addPipelineStats(pipeline);

    pipeline.awaitAndLog(logger, config.logInterval());

    try {
      profile.finish(name,
        new FeatureCollector.Factory(config, stats),
        createFeatureRenderer(writer, config, writer));
    } catch (Exception e) {
      LOGGER.error("Error calling profile.finish", e);
    }
    timer.stop();
  }

  private FeatureRenderer createFeatureRenderer(FeatureGroup writer, PlanetilerConfig config,
    Consumer<SortableFeature> next) {
    var encoder = writer.newRenderedFeatureEncoder();
    return new FeatureRenderer(
      config,
      rendered -> next.accept(encoder.apply(rendered)),
      stats
    );
  }

  SourceFeature processNodePass2(ReaderNode node) {
    // nodes are simple because they already contain their location
    return new NodeSourceFeature(node);
  }

  SourceFeature processWayPass2(ReaderWay way, NodeLocationProvider nodeLocations) {
    // ways contain an ordered list of node IDs, so we need to join that with node locations
    // from pass1 to reconstruct the geometry.
    LongArrayList nodes = way.getNodes();
    if (waysInMultipolygon.contains(way.getId())) {
      // if this is part of a multipolygon, store the node IDs for this way ID so that when
      // we get to the multipolygon we can go from way IDs -> node IDs -> node locations.
      synchronized (this) { // multiple threads may update this concurrently
        multipolygonWayGeometries.putAll(way.getId(), nodes);
      }
    }
    boolean closed = nodes.size() > 1 && nodes.get(0) == nodes.get(nodes.size() - 1);
    // area tag used to differentiate between whether a closed way should be treated as a polygon or linestring
    String area = way.getTag("area");
    List<RelationMember<OsmRelationInfo>> rels = getRelationMembershipForWay(way.getId());
    return new WaySourceFeature(way, closed, area, nodeLocations, rels);
  }

  SourceFeature processRelationPass2(ReaderRelation rel, NodeLocationProvider nodeLocations) {
    // Relation info gets used during way processing, except multipolygons which we have to process after we've
    // stored all the node IDs for each way.
    if (isMultipolygon(rel)) {
      List<RelationMember<OsmRelationInfo>> parentRelations = getRelationMembershipForWay(rel.getId());
      return new MultipolygonSourceFeature(rel, nodeLocations, parentRelations);
    } else {
      return null;
    }
  }

  private List<RelationMember<OsmRelationInfo>> getRelationMembershipForWay(long wayId) {
    LongArrayList relationIds = wayToRelations.get(wayId);
    List<RelationMember<OsmRelationInfo>> rels = null;
    if (!relationIds.isEmpty()) {
      rels = new ArrayList<>(relationIds.size());
      for (int r = 0; r < relationIds.size(); r++) {
        long encoded = relationIds.get(r);
        // encoded ID uses the upper few bits of the long to encode the role
        RelationMembership parsed = decodeRelationMembership(encoded);
        OsmRelationInfo rel = relationInfo.get(parsed.relationId);
        if (rel != null) {
          rels.add(new RelationMember<>(parsed.role, rel));
        }
      }
    }
    return rels;
  }

  @Override
  public long estimateMemoryUsageBytes() {
    long size = 0;
    size += estimateSize(waysInMultipolygon);
    size += estimateSize(multipolygonWayGeometries);
    size += estimateSize(wayToRelations);
    size += estimateSizeWithoutValues(relationInfo);
    size += MemoryEstimator.estimateSizeWithoutValues(roleIdsReverse);
    size += estimateSizeWithoutKeys(roleIds);
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
    nodeLocationDb.close();
    roleIds.release();
    roleIdsReverse.release();
  }

  NodeLocationProvider newNodeLocationProvider() {
    return new NodeDbLocationProvider();
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
  }

  /**
   * Member of a relation extracted from OSM input data.
   *
   * @param <T>      type of the user-defined class storing information about the relation
   * @param role     "role" of the relation member
   * @param relation user-provided data about the relation from pass1
   */
  public static record RelationMember<T extends OsmRelationInfo>(String role, T relation) {}

  /** Raw relation membership data that gets encoded/decoded into a long. */
  private record RelationMembership(String role, long relationId) {}

  /** Returns the role and relation ID packed into a long. */
  private RelationMembership decodeRelationMembership(long encoded) {
    int role = (int) ((encoded >>> ROLE_SHIFT) & ROLE_MASK);
    return new RelationMembership(roleIdsReverse.get(role), encoded & NOT_ROLE_MASK);
  }

  /** Packs a string role and relation into a compact long for storage. */
  private long encodeRelationMembership(String role, long relationId) {
    int roleId = roleIds.getOrDefault(role, -1);
    if (roleId == -1) {
      roleSizes.addAndGet(estimateSize(role));
      roleId = roleIds.size() + 1;
      roleIds.put(role, roleId);
      roleIdsReverse.put(roleId, role);
      if (roleId > MAX_ROLES) {
        throw new IllegalStateException("Too many roles to encode: " + role);
      }
    }
    return relationId | ((long) roleId << ROLE_SHIFT);
  }

  /**
   * A source feature generated from OSM elements. Inferring the geometry can be expensive, so each sublass is
   * constructed with the inputs necessary to create the geometry, but the geometry is constructed lazily on read.
   */
  private abstract class OsmFeature extends SourceFeature {

    private final boolean polygon;
    private final boolean line;
    private final boolean point;
    private Geometry latLonGeom;
    private Geometry worldGeom;

    public OsmFeature(ReaderElement elem, boolean point, boolean line, boolean polygon,
      List<RelationMember<OsmRelationInfo>> relationInfo) {
      super(ReaderElementUtils.getTags(elem), name, null, relationInfo, elem.getId());
      this.point = point;
      this.line = line;
      this.polygon = polygon;
    }

    @Override
    public Geometry latLonGeometry() throws GeometryException {
      return latLonGeom != null ? latLonGeom : (latLonGeom = GeoUtils.worldToLatLonCoords(worldGeometry()));
    }

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

  /** A {@link Point} created from an OSM node. */
  private class NodeSourceFeature extends OsmFeature {

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

  /** Returns {@code true} if a way can be interpreted as a line. */
  public static boolean canBeLine(boolean closed, String area, int points) {
    return (!closed || !"yes".equals(area)) && points >= 2;
  }

  /** Returns {@code true} if a way can be interpreted as a polygon. */
  public static boolean canBePolygon(boolean closed, String area, int points) {
    return (closed && !"no".equals(area)) && points >= 4;
  }

  /**
   * A {@link LineString} or {@link Polygon} created from an OSM way.
   * <p>
   * Unclosed rings are always interpreted as linestrings. Closed rings can be interpreted as either a polygon or a
   * linestring unless {@code area=yes} tag prevents them from being a linestring or {@code area=no} tag prevents them
   * from being a polygon.
   */
  private class WaySourceFeature extends OsmFeature {

    private final NodeLocationProvider nodeLocations;
    private final LongArrayList nodeIds;

    public WaySourceFeature(ReaderWay way, boolean closed, String area, NodeLocationProvider nodeLocations,
      List<RelationMember<OsmRelationInfo>> relationInfo) {
      super(way, false,
        OsmReader.canBeLine(closed, area, way.getNodes().size()),
        OsmReader.canBePolygon(closed, area, way.getNodes().size()),
        relationInfo
      );
      this.nodeIds = way.getNodes();
      this.nodeLocations = nodeLocations;
    }

    @Override
    protected Geometry computeLine() throws GeometryException {
      try {
        CoordinateSequence coords = nodeLocations.getWayGeometry(nodeIds);
        return GeoUtils.JTS_FACTORY.createLineString(coords);
      } catch (IllegalArgumentException e) {
        throw new GeometryException("osm_invalid_line", "Error building line for way " + id() + ": " + e);
      }
    }

    @Override
    protected Geometry computePolygon() throws GeometryException {
      try {
        CoordinateSequence coords = nodeLocations.getWayGeometry(nodeIds);
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

  /**
   * A {@link MultiPolygon} created from an OSM relation where {@code type=multipolygon}.
   * <p>
   * Delegates complex reconstruction work to {@link OsmMultipolygon}.
   */
  private class MultipolygonSourceFeature extends OsmFeature {

    private final ReaderRelation relation;
    private final NodeLocationProvider nodeLocations;

    public MultipolygonSourceFeature(ReaderRelation relation, NodeLocationProvider nodeLocations,
      List<RelationMember<OsmRelationInfo>> parentRelations) {
      super(relation, false, false, true, parentRelations);
      this.relation = relation;
      this.nodeLocations = nodeLocations;
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
          } else if (relation.hasTag("type", "multipolygon")) {
            // boundary and land_area relations might not be complete for extracts, but multipolygons should be
            LOGGER.warn(
              "Missing " + role + " OsmWay[" + member.getRef() + "] for " + relation.getTag("type") + " " + this);
          }
        }
      }
      return OsmMultipolygon.build(rings, nodeLocations, id());
    }

    @Override
    public String toString() {
      return "OsmRelation[" + id() + ']';
    }
  }

  /**
   * A thin layer on top of {@link LongLongMap} that decodes node locations stored as {@code long} values.
   */
  private class NodeDbLocationProvider implements NodeLocationProvider {

    @Override
    public Coordinate getCoordinate(long id) {
      long encoded = nodeLocationDb.get(id);
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
        long encoded = nodeLocationDb.get(nodeIds.get(i));
        if (encoded == LongLongMap.MISSING_VALUE) {
          throw new IllegalArgumentException("Missing location for node: " + nodeIds.get(i));
        }
        seq.setOrdinate(i, 0, GeoUtils.decodeWorldX(encoded));
        seq.setOrdinate(i, 1, GeoUtils.decodeWorldY(encoded));
      }
      return seq;
    }
  }
}
