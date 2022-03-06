package com.onthegomap.planetiler.reader.osm;

import static com.onthegomap.planetiler.util.MemoryEstimator.estimateSize;
import static com.onthegomap.planetiler.worker.Worker.joinFutures;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongObjectHashMap;
import com.carrotsearch.hppc.ObjectIntHashMap;
import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.collection.Hppc;
import com.onthegomap.planetiler.collection.LongLongMap;
import com.onthegomap.planetiler.collection.LongLongMultimap;
import com.onthegomap.planetiler.collection.SortableFeature;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.render.FeatureRenderer;
import com.onthegomap.planetiler.stats.Counter;
import com.onthegomap.planetiler.stats.ProcessInfo;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.Format;
import com.onthegomap.planetiler.util.MemoryEstimator;
import com.onthegomap.planetiler.worker.Distributor;
import com.onthegomap.planetiler.worker.WeightedHandoffQueue;
import com.onthegomap.planetiler.worker.WorkQueue;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
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
 * Utility that constructs {@link SourceFeature SourceFeatures} from the raw nodes, ways, and
 * relations contained in an {@link OsmInputFile}.
 */
public class OsmReader implements Closeable, MemoryEstimator.HasEstimate {

  private static final Logger LOGGER = LoggerFactory.getLogger(OsmReader.class);
  private static final Format FORMAT = Format.defaultInstance();
  private static final int ROLE_BITS = 16;
  private static final int MAX_ROLES = (1 << ROLE_BITS) - 10;
  private static final int ROLE_SHIFT = 64 - ROLE_BITS;
  private static final int ROLE_MASK = (1 << ROLE_BITS) - 1;
  private static final long NOT_ROLE_MASK = (1L << ROLE_SHIFT) - 1L;
  private final OsmBlockSource osmBlockSource;
  private final Stats stats;
  private final LongLongMap nodeLocationDb;
  private final Counter.Readable PASS1_BLOCKS = Counter.newSingleThreadCounter();
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
  private LongObjectHashMap<OsmRelationInfo> relationInfo = Hppc.newLongObjectHashMap();
  // ~800mb, ~1.6GB when sorting
  private LongLongMultimap wayToRelations = LongLongMultimap.newSparseUnorderedMultimap();
  // for multipolygons need to store way info (20m ways, 800m nodes) to use when processing
  // relations (4.5m)
  // ~300mb
  private LongHashSet waysInMultipolygon = new LongHashSet();
  // ~7GB
  private LongLongMultimap multipolygonWayGeometries = LongLongMultimap.newDensedOrderedMultimap();
  // keep track of data needed to encode/decode role strings into a long
  private final ObjectIntHashMap<String> roleIds = new ObjectIntHashMap<>();
  private final IntObjectHashMap<String> roleIdsReverse = new IntObjectHashMap<>();
  private final AtomicLong roleSizes = new AtomicLong(0);

  /**
   * Constructs a new {@code OsmReader} from
   *
   * @param name ID for this reader to use in stats and logs
   * @param osmSourceProvider the file to read raw nodes, ways, and relations from
   * @param nodeLocationDb store that will temporarily hold node locations (encoded as a long)
   *     between passes to reconstruct way geometries
   * @param profile logic that defines what map features to emit for each source feature
   * @param stats to keep track of counters and timings
   */
  public OsmReader(
      String name,
      Supplier<OsmBlockSource> osmSourceProvider,
      LongLongMap nodeLocationDb,
      Profile profile,
      Stats stats) {
    this.name = name;
    this.osmBlockSource = osmSourceProvider.get();
    this.nodeLocationDb = nodeLocationDb;
    this.stats = stats;
    this.profile = profile;
    stats.monitorInMemoryObject("osm_relations", this);
    stats.counter(
        "osm_pass1_elements_processed",
        "type",
        () ->
            Map.of(
                "blocks", PASS1_BLOCKS,
                "nodes", PASS1_NODES,
                "ways", PASS1_WAYS,
                "relations", PASS1_RELATIONS));
  }

  /**
   * Pre-processes all OSM elements before {@link #pass2(FeatureGroup, PlanetilerConfig)} is used to
   * emit map features.
   *
   * <p>Stores node locations for pass2 to use to reconstruct way geometries.
   *
   * <p>Also stores the result of {@link Profile#preprocessOsmRelation(OsmElement.Relation)} so that
   * pass2 can know the relevant relations that a way belongs to.
   *
   * @param config user-provided arguments to control the number of threads, and log interval
   */
  public void pass1(PlanetilerConfig config) {
    record BlockWithResult(OsmBlockSource.Block block, WeightedHandoffQueue<OsmElement> result) {}
    var timer = stats.startStage("osm_pass1");
    int parseThreads = Math.max(1, config.threads() - 2);
    int pendingBlocks = parseThreads * 2;
    // Each worker will hand off finished elements to the single process thread. A
    // Future<List<OsmElement>> would result
    // in too much memory usage/GC so use a WeightedHandoffQueue instead which will fill up with
    // lightweight objects
    // like nodes without any tags, but limit the number of pending heavy entities like relations
    int handoffQueueBatches =
        Math.max(10, (int) (100d * ProcessInfo.getMaxMemoryBytes() / 100_000_000_000d));
    var parsedBatches =
        new WorkQueue<WeightedHandoffQueue<OsmElement>>("elements", pendingBlocks, 1, stats);
    var pipeline = WorkerPipeline.start("osm_pass1", stats);
    var readBranch =
        pipeline
            .<BlockWithResult>fromGenerator(
                "read",
                next -> {
                  osmBlockSource.forEachBlock(
                      (block) -> {
                        WeightedHandoffQueue<OsmElement> result =
                            new WeightedHandoffQueue<>(handoffQueueBatches, 10_000);
                        parsedBatches.accept(result);
                        next.accept(new BlockWithResult(block, result));
                      });
                  parsedBatches.close();
                })
            .addBuffer("pbf_blocks", pendingBlocks)
            .sinkToConsumer(
                "parse",
                parseThreads,
                block -> {
                  List<OsmElement> result = new ArrayList<>();
                  boolean nodesDone = false, waysDone = false;
                  for (var element : block.block.decodeElements()) {
                    if (element instanceof OsmElement.Node node) {
                      // pre-compute encoded location in worker threads since it is fairly expensive
                      // and should be done in parallel
                      node.encodedLocation();
                      if (nodesDone) {
                        throw new IllegalArgumentException(
                            "Input file must be sorted with nodes first, then ways, then relations."
                                + " Encountered node "
                                + node.id()
                                + " after a way or relation");
                      }
                    } else if (element instanceof OsmElement.Way way) {
                      nodesDone = true;
                      if (waysDone) {
                        throw new IllegalArgumentException(
                            "Input file must be sorted with nodes first, then ways, then relations."
                                + " Encountered way "
                                + way.id()
                                + " after a relation");
                      }
                    } else if (element instanceof OsmElement.Relation) {
                      nodesDone = waysDone = true;
                    }
                    block.result.accept(element, element.cost());
                  }
                  block.result.close();
                });

    var processBranch =
        pipeline.readFromQueue(parsedBatches).sinkToConsumer("process", 1, this::processPass1Block);

    var loggers =
        ProgressLoggers.create()
            .addRateCounter("nodes", PASS1_NODES, true)
            .addFileSizeAndRam(nodeLocationDb)
            .addRateCounter("ways", PASS1_WAYS, true)
            .addRateCounter("rels", PASS1_RELATIONS, true)
            .addRateCounter("blocks", PASS1_BLOCKS)
            .newLine()
            .addProcessStats()
            .addInMemoryObject("hppc", this)
            .newLine()
            .addPipelineStats(readBranch)
            .addPipelineStats(processBranch);

    loggers.awaitAndLog(joinFutures(readBranch.done(), processBranch.done()), config.logInterval());

    LOGGER.debug(
        "processed "
            + "blocks:"
            + FORMAT.integer(PASS1_BLOCKS.get())
            + " nodes:"
            + FORMAT.integer(PASS1_NODES.get())
            + " ways:"
            + FORMAT.integer(PASS1_WAYS.get())
            + " relations:"
            + FORMAT.integer(PASS1_RELATIONS.get()));
    timer.stop();
  }

  void processPass1Block(Iterable<? extends OsmElement> elements) {
    int nodes = 0, ways = 0, relations = 0;
    for (OsmElement element : elements) {
      // only a single thread calls this with elements ordered by ID, so it's safe to manipulate
      // these
      // shared data structures which are not thread safe
      if (element.id() < 0) {
        throw new IllegalArgumentException("Negative OSM element IDs not supported: " + element);
      }
      if (element instanceof OsmElement.Node node) {
        nodes++;
        try {
          profile.preprocessOsmNode(node);
        } catch (Exception e) {
          LOGGER.error("Error preprocessing OSM node " + node.id(), e);
        }
        // TODO allow limiting node storage to only ones that profile cares about
        nodeLocationDb.put(node.id(), node.encodedLocation());
      } else if (element instanceof OsmElement.Way way) {
        ways++;
        try {
          profile.preprocessOsmWay(way);
        } catch (Exception e) {
          LOGGER.error("Error preprocessing OSM way " + way.id(), e);
        }
      } else if (element instanceof OsmElement.Relation relation) {
        relations++;
        try {
          List<OsmRelationInfo> infos = profile.preprocessOsmRelation(relation);
          if (infos != null) {
            for (OsmRelationInfo info : infos) {
              relationInfo.put(relation.id(), info);
              relationInfoSizes.addAndGet(info.estimateMemoryUsageBytes());
              for (var member : relation.members()) {
                var type = member.type();
                // TODO handle nodes in relations and super-relations
                if (type == OsmElement.Type.WAY) {
                  wayToRelations.put(
                      member.ref(), encodeRelationMembership(member.role(), relation.id()));
                }
              }
            }
          }
        } catch (Exception e) {
          LOGGER.error("Error preprocessing OSM relation " + relation.id(), e);
        }
        // TODO allow limiting multipolygon storage to only ones that profile cares about
        if (isMultipolygon(relation)) {
          for (var member : relation.members()) {
            if (member.type() == OsmElement.Type.WAY) {
              waysInMultipolygon.add(member.ref());
            }
          }
        }
      }
    }
    PASS1_BLOCKS.inc();
    PASS1_NODES.incBy(nodes);
    PASS1_WAYS.incBy(ways);
    PASS1_RELATIONS.incBy(relations);
  }

  private static boolean isMultipolygon(OsmElement.Relation relation) {
    return relation.hasTag("type", "multipolygon", "boundary", "land_area");
  }

  /**
   * Constructs geometries from OSM elements and emits map features as defined by the {@link
   * Profile}.
   *
   * @param writer consumer that will store finished features
   * @param config user-provided arguments to control the number of threads, and log interval
   */
  public void pass2(FeatureGroup writer, PlanetilerConfig config) {
    var timer = stats.startStage("osm_pass2");
    int threads = config.threads();
    int processThreads = Math.max(threads < 4 ? threads : (threads - 1), 1);
    Counter.MultiThreadCounter blocksProcessed = Counter.newMultiThreadCounter();
    Counter.MultiThreadCounter nodesProcessed = Counter.newMultiThreadCounter();
    Counter.MultiThreadCounter waysProcessed = Counter.newMultiThreadCounter();
    Counter.MultiThreadCounter relsProcessed = Counter.newMultiThreadCounter();
    stats.counter(
        "osm_pass2_elements_processed",
        "type",
        () ->
            Map.of(
                "blocks", blocksProcessed,
                "nodes", nodesProcessed,
                "ways", waysProcessed,
                "relations", relsProcessed));

    // since multiple threads process OSM elements, and we must process all ways before processing
    // any relations,
    // use a count down latch to wait for all threads to finish processing ways
    CountDownLatch waitForWays = new CountDownLatch(processThreads);
    // Use a Distributor to keep all worker threads busy when processing the final blocks of
    // relations by offloading
    // items to threads that are done reading blocks
    Distributor<OsmElement.Relation> relationDistributor = Distributor.createWithCapacity(1_000);

    var pipeline =
        WorkerPipeline.start("osm_pass2", stats)
            .fromGenerator("read", osmBlockSource::forEachBlock)
            .addBuffer("pbf_blocks", 100)
            .<SortableFeature>addWorker(
                "process",
                processThreads,
                (prev, next) -> {
                  // avoid contention trying to get the thread-local counters by getting them once
                  // when thread starts
                  Counter blocks = blocksProcessed.counterForThread();
                  Counter nodes = nodesProcessed.counterForThread();
                  Counter ways = waysProcessed.counterForThread();
                  Counter rels = relsProcessed.counterForThread();

                  var waysDone = false;
                  var featureCollectors = new FeatureCollector.Factory(config, stats);
                  final NodeLocationProvider nodeLocations = newNodeLocationProvider();
                  FeatureRenderer renderer = createFeatureRenderer(writer, config, next);
                  var relationHandler =
                      relationDistributor.forThread(
                          relation -> {
                            var feature = processRelationPass2(relation, nodeLocations);
                            if (feature != null) {
                              render(featureCollectors, renderer, relation, feature);
                            }
                            rels.inc();
                          });

                  for (var block : prev) {
                    int blockNodes = 0, blockWays = 0;
                    for (var element : block.decodeElements()) {
                      SourceFeature feature = null;
                      if (element instanceof OsmElement.Node node) {
                        blockNodes++;
                        feature = processNodePass2(node);
                      } else if (element instanceof OsmElement.Way way) {
                        blockWays++;
                        feature = processWayPass2(way, nodeLocations);
                      } else if (element instanceof OsmElement.Relation relation) {
                        // ensure all ways finished processing before we start relations
                        if (!waysDone && waitForWays.getCount() > 0) {
                          waitForWays.countDown();
                          waitForWays.await();
                          waysDone = true;
                        }
                        relationHandler.accept(relation);
                      }
                      // render features specified by profile and hand them off to next step that
                      // will
                      // write them intermediate storage
                      if (feature != null) {
                        render(featureCollectors, renderer, element, feature);
                      }
                    }

                    blocks.inc();
                    nodes.incBy(blockNodes);
                    ways.incBy(blockWays);
                  }
                  // just in case a worker skipped over all relations
                  waitForWays.countDown();

                  // do work for other threads that are still processing blocks of relations
                  relationHandler.close();
                })
            .addBuffer("feature_queue", 50_000, 1_000)
            // FeatureGroup writes need to be single-threaded
            .sinkToConsumer("write", 1, writer);

    var logger =
        ProgressLoggers.create()
            .addRatePercentCounter("nodes", PASS1_NODES.get(), nodesProcessed, true)
            .addFileSizeAndRam(nodeLocationDb)
            .addRatePercentCounter("ways", PASS1_WAYS.get(), waysProcessed, true)
            .addRatePercentCounter("rels", PASS1_RELATIONS.get(), relsProcessed, true)
            .addRateCounter("features", writer::numFeaturesWritten)
            .addFileSize(writer)
            .addRatePercentCounter("blocks", PASS1_BLOCKS.get(), blocksProcessed, false)
            .newLine()
            .addProcessStats()
            .addInMemoryObject("hppc", this)
            .newLine()
            .addPipelineStats(pipeline);

    pipeline.awaitAndLog(logger, config.logInterval());

    LOGGER.debug(
        "processed "
            + "blocks:"
            + FORMAT.integer(blocksProcessed.get())
            + " nodes:"
            + FORMAT.integer(nodesProcessed.get())
            + " ways:"
            + FORMAT.integer(waysProcessed.get())
            + " relations:"
            + FORMAT.integer(relsProcessed.get()));

    timer.stop();

    try {
      profile.finish(
          name,
          new FeatureCollector.Factory(config, stats),
          createFeatureRenderer(writer, config, writer));
    } catch (Exception e) {
      LOGGER.error("Error calling profile.finish", e);
    }
  }

  private void render(
      FeatureCollector.Factory featureCollectors,
      FeatureRenderer renderer,
      OsmElement element,
      SourceFeature feature) {
    FeatureCollector features = featureCollectors.get(feature);
    try {
      profile.processFeature(feature, features);
      for (FeatureCollector.Feature renderable : features) {
        renderer.accept(renderable);
      }
    } catch (Exception e) {
      String type = element.getClass().getSimpleName();
      LOGGER.error("Error processing OSM " + type + " " + element.id(), e);
    }
  }

  private FeatureRenderer createFeatureRenderer(
      FeatureGroup writer, PlanetilerConfig config, Consumer<SortableFeature> next) {
    var encoder = writer.newRenderedFeatureEncoder();
    return new FeatureRenderer(config, rendered -> next.accept(encoder.apply(rendered)), stats);
  }

  SourceFeature processNodePass2(OsmElement.Node node) {
    // nodes are simple because they already contain their location
    return new NodeSourceFeature(node);
  }

  SourceFeature processWayPass2(OsmElement.Way way, NodeLocationProvider nodeLocations) {
    // ways contain an ordered list of node IDs, so we need to join that with node locations
    // from pass1 to reconstruct the geometry.
    LongArrayList nodes = way.nodes();
    if (waysInMultipolygon.contains(way.id())) {
      // if this is part of a multipolygon, store the node IDs for this way ID so that when
      // we get to the multipolygon we can go from way IDs -> node IDs -> node locations.
      synchronized (this) { // multiple threads may update this concurrently
        multipolygonWayGeometries.putAll(way.id(), nodes);
      }
    }
    boolean closed = nodes.size() > 1 && nodes.get(0) == nodes.get(nodes.size() - 1);
    // area tag used to differentiate between whether a closed way should be treated as a polygon or
    // linestring
    String area = way.getString("area");
    List<RelationMember<OsmRelationInfo>> rels = getRelationMembershipForWay(way.id());
    return new WaySourceFeature(way, closed, area, nodeLocations, rels);
  }

  SourceFeature processRelationPass2(OsmElement.Relation rel, NodeLocationProvider nodeLocations) {
    // Relation info gets used during way processing, except multipolygons which we have to process
    // after we've
    // stored all the node IDs for each way.
    if (isMultipolygon(rel)) {
      List<RelationMember<OsmRelationInfo>> parentRelations = getRelationMembershipForWay(rel.id());
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
    size += estimateSize(relationInfo);
    size += estimateSize(roleIdsReverse);
    size += estimateSize(roleIds);
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
    osmBlockSource.close();
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
   * @param <T> type of the user-defined class storing information about the relation
   * @param role "role" of the relation member
   * @param relation user-provided data about the relation from pass1
   */
  public record RelationMember<T extends OsmRelationInfo>(String role, T relation) {}

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
   * A source feature generated from OSM elements. Inferring the geometry can be expensive, so each
   * sublass is constructed with the inputs necessary to create the geometry, but the geometry is
   * constructed lazily on read.
   */
  private abstract class OsmFeature extends SourceFeature {

    private final boolean polygon;
    private final boolean line;
    private final boolean point;
    private Geometry latLonGeom;
    private Geometry worldGeom;

    public OsmFeature(
        OsmElement elem,
        boolean point,
        boolean line,
        boolean polygon,
        List<RelationMember<OsmRelationInfo>> relationInfo) {
      super(elem.tags(), name, null, relationInfo, elem.id());
      this.point = point;
      this.line = line;
      this.polygon = polygon;
    }

    @Override
    public Geometry latLonGeometry() throws GeometryException {
      return latLonGeom != null
          ? latLonGeom
          : (latLonGeom = GeoUtils.worldToLatLonCoords(worldGeometry()));
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

    private final long encodedLocation;

    NodeSourceFeature(OsmElement.Node node) {
      super(node, true, false, false, null);
      this.encodedLocation = node.encodedLocation();
    }

    @Override
    protected Geometry computeWorldGeometry() {
      return GeoUtils.point(
          GeoUtils.decodeWorldX(encodedLocation), GeoUtils.decodeWorldY(encodedLocation));
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
   *
   * <p>Unclosed rings are always interpreted as linestrings. Closed rings can be interpreted as
   * either a polygon or a linestring unless {@code area=yes} tag prevents them from being a
   * linestring or {@code area=no} tag prevents them from being a polygon.
   */
  private class WaySourceFeature extends OsmFeature {

    private final NodeLocationProvider nodeLocations;
    private final LongArrayList nodeIds;

    public WaySourceFeature(
        OsmElement.Way way,
        boolean closed,
        String area,
        NodeLocationProvider nodeLocations,
        List<RelationMember<OsmRelationInfo>> relationInfo) {
      super(
          way,
          false,
          OsmReader.canBeLine(closed, area, way.nodes().size()),
          OsmReader.canBePolygon(closed, area, way.nodes().size()),
          relationInfo);
      this.nodeIds = way.nodes();
      this.nodeLocations = nodeLocations;
    }

    @Override
    protected Geometry computeLine() throws GeometryException {
      try {
        CoordinateSequence coords = nodeLocations.getWayGeometry(nodeIds);
        return GeoUtils.JTS_FACTORY.createLineString(coords);
      } catch (IllegalArgumentException e) {
        throw new GeometryException(
            "osm_invalid_line", "Error building line for way " + id() + ": " + e);
      }
    }

    @Override
    protected Geometry computePolygon() throws GeometryException {
      try {
        CoordinateSequence coords = nodeLocations.getWayGeometry(nodeIds);
        return GeoUtils.JTS_FACTORY.createPolygon(coords);
      } catch (IllegalArgumentException e) {
        throw new GeometryException(
            "osm_invalid_polygon", "Error building polygon for way " + id() + ": " + e);
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
   *
   * <p>Delegates complex reconstruction work to {@link OsmMultipolygon}.
   */
  private class MultipolygonSourceFeature extends OsmFeature {

    private final OsmElement.Relation relation;
    private final NodeLocationProvider nodeLocations;

    public MultipolygonSourceFeature(
        OsmElement.Relation relation,
        NodeLocationProvider nodeLocations,
        List<RelationMember<OsmRelationInfo>> parentRelations) {
      super(relation, false, false, true, parentRelations);
      this.relation = relation;
      this.nodeLocations = nodeLocations;
    }

    @Override
    protected Geometry computeWorldGeometry() throws GeometryException {
      List<LongArrayList> rings = new ArrayList<>(relation.members().size());
      for (OsmElement.Relation.Member member : relation.members()) {
        String role = member.role();
        LongArrayList poly = multipolygonWayGeometries.get(member.ref());
        if (member.type() == OsmElement.Type.WAY) {
          if (poly != null && !poly.isEmpty()) {
            rings.add(poly);
          } else if (relation.hasTag("type", "multipolygon")) {
            // boundary and land_area relations might not be complete for extracts, but
            // multipolygons should be
            LOGGER.warn(
                "Missing "
                    + role
                    + " OsmWay["
                    + member.ref()
                    + "] for "
                    + relation.getTag("type")
                    + " "
                    + this);
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
   * A thin layer on top of {@link LongLongMap} that decodes node locations stored as {@code long}
   * values.
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
