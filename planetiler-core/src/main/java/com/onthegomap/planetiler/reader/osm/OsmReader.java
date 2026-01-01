package com.onthegomap.planetiler.reader.osm;

import static com.onthegomap.planetiler.util.MemoryEstimator.estimateSize;
import static com.onthegomap.planetiler.worker.Worker.joinFutures;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongObjectHashMap;
import com.carrotsearch.hppc.ObjectIntHashMap;
import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.collection.Hppc;
import com.onthegomap.planetiler.collection.LongLongMap;
import com.onthegomap.planetiler.collection.LongLongMultimap;
import com.onthegomap.planetiler.collection.SortableFeature;
import com.onthegomap.planetiler.collection.Storage;
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
import com.onthegomap.planetiler.util.ResourceUsage;
import com.onthegomap.planetiler.worker.Distributor;
import com.onthegomap.planetiler.worker.WeightedHandoffQueue;
import com.onthegomap.planetiler.worker.WorkQueue;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
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
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility that constructs {@link SourceFeature SourceFeatures} from the raw nodes, ways, and relations contained in an
 * {@link OsmInputFile}.
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
  private final Profile profile;
  private final String name;
  private final AtomicLong relationInfoSizes = new AtomicLong(0);
  // need a few large objects to process ways in relations, should be small enough to keep in memory
  // for routes (750k rels 40m ways) and boundaries (650k rels, 8m ways)
  // need to store route info to use later when processing ways
  // <~500mb
  private LongObjectHashMap<OsmRelationInfo> relationInfo = Hppc.newLongObjectHashMap();
  // ~800mb, ~1.6GB when sorting
  private LongLongMultimap.Appendable wayToRelations = LongLongMultimap.newAppendableMultimap();

  // ~20mb or ~40mb while sorting
  private LongLongMultimap.Appendable relationToParentRelations = LongLongMultimap.newAppendableMultimap();

  private final Object wayToRelationsLock = new Object();
  // for multipolygons need to store way info (20m ways, 800m nodes) to use when processing relations (4.5m)
  // ~300mb
  private Roaring64Bitmap waysInMultipolygon = new Roaring64Bitmap();
  private final Object waysInMultipolygonLock = new Object();
  // ~7GB
  private LongLongMultimap.Replaceable multipolygonWayGeometries;
  // for relation_members: track relations that need member processing
  private Roaring64Bitmap relationsForMemberProcessing = new Roaring64Bitmap();
  private final Object relationsForMemberProcessingLock = new Object();
  // for relation_members: track ways that are members of relations we care about
  private Roaring64Bitmap waysInRelationMembers = new Roaring64Bitmap();
  private final Object waysInRelationMembersLock = new Object();
  // for relation_members: store way geometries (node IDs) for member ways
  private LongLongMultimap.Replaceable relationMembersWayGeometries;
  // for relation_members: store way tags for member ways
  private LongObjectHashMap<Map<String, Object>> relationMembersWayTags = Hppc.newLongObjectHashMap();
  private final Object relationMembersWayTagsLock = new Object();
  // for relation_members: track nodes that are members of relations we care about
  private Roaring64Bitmap nodesInRelationMembers = new Roaring64Bitmap();
  private final Object nodesInRelationMembersLock = new Object();
  // for relation_members: store node tags for member nodes
  private LongObjectHashMap<Map<String, Object>> relationMembersNodeTags = Hppc.newLongObjectHashMap();
  private final Object relationMembersNodeTagsLock = new Object();
  // keep track of data needed to encode/decode role strings into a long
  private final ObjectIntHashMap<String> roleIds = new ObjectIntHashMap<>();
  private final IntObjectHashMap<String> roleIdsReverse = new IntObjectHashMap<>();
  private final AtomicLong roleSizes = new AtomicLong(0);
  private final OsmPhaser pass1Phaser = new OsmPhaser(0);

  /**
   * Constructs a new {@code OsmReader} from an {@code osmSourceProvider} that will use {@code nodeLocationDb} as a
   * temporary store for node locations.
   *
   * @param name                   ID for this reader to use in stats and logs
   * @param osmSourceProvider      the file to read raw nodes, ways, and relations from
   * @param nodeLocationDb         store that will temporarily hold node locations (encoded as a long) between passes to
   *                               reconstruct way geometries
   * @param multipolygonGeometries store that will temporarily hold multipolygon way geometries
   * @param profile                logic that defines what map features to emit for each source feature
   * @param stats                  to keep track of counters and timings
   */
  public OsmReader(String name, Supplier<OsmBlockSource> osmSourceProvider, LongLongMap nodeLocationDb,
    LongLongMultimap.Replaceable multipolygonGeometries, Profile profile, Stats stats) {
    this.name = name;
    this.osmBlockSource = osmSourceProvider.get();
    this.nodeLocationDb = nodeLocationDb;
    this.stats = stats;
    this.profile = profile;
    stats.monitorInMemoryObject("osm_relations", this);
    stats.counter("osm_pass1_elements_processed", "type", () -> Map.of(
      "blocks", PASS1_BLOCKS,
      "nodes", pass1Phaser::nodes,
      "ways", pass1Phaser::ways,
      "relations", pass1Phaser::relations
    ));
    this.multipolygonWayGeometries = multipolygonGeometries;
    // Initialize relation members way geometries storage (similar to multipolygons)
    this.relationMembersWayGeometries = LongLongMultimap.newInMemoryReplaceableMultimap();
  }

  /**
   * Alias for {@link #OsmReader(String, Supplier, LongLongMap, LongLongMultimap.Replaceable, Profile, Stats)} that sets
   * the multipolygon geometry multimap to a default in-memory implementation.
   */
  public OsmReader(String name, Supplier<OsmBlockSource> osmSourceProvider, LongLongMap nodeLocationDb, Profile profile,
    Stats stats) {
    this(name, osmSourceProvider, nodeLocationDb, LongLongMultimap.newInMemoryReplaceableMultimap(), profile, stats);
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
    var pipeline = WorkerPipeline.start("osm_pass1", stats);
    CompletableFuture<?> done;

    var loggers = ProgressLoggers.create()
      .addRateCounter("nodes", pass1Phaser::nodes, true)
      .addFileSizeAndRam(nodeLocationDb)
      .addRateCounter("ways", pass1Phaser::ways, true)
      .addRateCounter("rels", pass1Phaser::relations, true)
      .addRateCounter("blocks", PASS1_BLOCKS)
      .newLine()
      .addProcessStats()
      .addInMemoryObject("hppc", this)
      .newLine();
    int threads = config.threads();

    if (nodeLocationDb instanceof LongLongMap.ParallelWrites) {
      // If the node location writer supports parallel writes, then parse, process, and write node locations from worker threads
      int parseThreads = Math.max(1, threads < 8 ? threads : (threads - 1));
      pass1Phaser.registerWorkers(parseThreads);
      var parallelPipeline = pipeline
        .fromGenerator("read", osmBlockSource::forEachBlock)
        .addBuffer("pbf_blocks", parseThreads * 2)
        .sinkTo("process", parseThreads, this::processPass1Blocks);
      loggers.addPipelineStats(parallelPipeline);
      done = parallelPipeline.done();
    } else {
      // If the node location writer requires sequential writes, then the reader hands off the block to workers
      // and a handle that the result will go on to the single-threaded writer, and the writer emits new nodes when
      // they are ready
      int parseThreads = Math.max(1, threads < 8 ? threads : (threads - 2));
      int pendingBlocks = parseThreads * 2;
      // Each worker will hand off finished elements to the single process thread. A Future<List<OsmElement>> would result
      // in too much memory usage/GC so use a WeightedHandoffQueue instead which will fill up with lightweight objects
      // like nodes without any tags, but limit the number of pending heavy entities like relations
      int handoffQueueBatches = Math.max(
        10,
        (int) (100d * ProcessInfo.getMaxMemoryBytes() / 20_000_000_000d)
      );
      record BlockWithResult(OsmBlockSource.Block block, WeightedHandoffQueue<OsmElement> result) {}
      pass1Phaser.registerWorkers(1);
      var parsedBatches = new WorkQueue<WeightedHandoffQueue<OsmElement>>("elements", pendingBlocks, 1, stats);
      var readBranch = pipeline
        .<BlockWithResult>fromGenerator("read", next -> {
          var parsedBatchEnqueuer = parsedBatches.threadLocalWriter();
          osmBlockSource.forEachBlock((block) -> {
            WeightedHandoffQueue<OsmElement> result = new WeightedHandoffQueue<>(handoffQueueBatches, 10_000);
            parsedBatchEnqueuer.accept(result);
            next.accept(new BlockWithResult(block, result));
          });
          parsedBatches.close();
        })
        .addBuffer("pbf_blocks", pendingBlocks)
        .sinkToConsumer("parse", parseThreads, block -> {
          for (var element : block.block.decodeElements()) {
            if (element instanceof OsmElement.Node node) {
              // pre-compute encoded location in worker threads since it is fairly expensive and should be done in parallel
              node.encodedLocation();
            }
            block.result.accept(element, element.cost());
          }
          block.result.close();
        });

      var processBranch = pipeline
        .readFromQueue(parsedBatches)
        .sinkTo("process", 1, this::processPass1Blocks);

      loggers
        .addPipelineStats(readBranch)
        .addPipelineStats(processBranch);
      done = joinFutures(readBranch.done(), processBranch.done());
    }

    loggers.awaitAndLog(done, config.logInterval());

    LOGGER.debug("Processed " + FORMAT.integer(PASS1_BLOCKS.get()) + " blocks:");
    pass1Phaser.printSummary();
    timer.stop();
  }

  void processPass1Blocks(Iterable<? extends Iterable<? extends OsmElement>> blocks) {
    // may be called by multiple threads so need to synchronize access to any shared data structures
    try (
      var nodeWriter = nodeLocationDb.newWriter();
      var phases = pass1Phaser.forWorker()
        .whenWorkerFinishes(OsmPhaser.Phase.NODES, nodeWriter::close)
    ) {
      for (var block : blocks) {
        for (OsmElement element : block) {
          if (element.id() < 0) {
            throw new IllegalArgumentException("Negative OSM element IDs not supported: " + element);
          }
          if (element instanceof OsmElement.Node node) {
            phases.arrive(OsmPhaser.Phase.NODES);
            try {
              profile.preprocessOsmNode(node);
            } catch (Exception e) {
              LOGGER.error("Error preprocessing OSM node " + node.id(), e);
            }
            // TODO allow limiting node storage to only ones that profile cares about
            nodeWriter.put(node.id(), node.encodedLocation());
          } else if (element instanceof OsmElement.Way way) {
            phases.arriveAndWaitForOthers(OsmPhaser.Phase.WAYS);
            try {
              profile.preprocessOsmWay(way);
            } catch (Exception e) {
              LOGGER.error("Error preprocessing OSM way " + way.id(), e);
            }
          } else if (element instanceof OsmElement.Relation relation) {
            phases.arrive(OsmPhaser.Phase.RELATIONS);
            try {
              List<OsmRelationInfo> infos = profile.preprocessOsmRelation(relation);
              if (infos != null && !infos.isEmpty()) {
                synchronized (wayToRelationsLock) {
                  for (OsmRelationInfo info : infos) {
                    relationInfo.put(relation.id(), info);
                    relationInfoSizes.addAndGet(info.estimateMemoryUsageBytes());
                  }
                  for (var member : relation.members()) {
                    var type = member.type();
                    // TODO handle nodes in relations
                    if (type == OsmElement.Type.WAY) {
                      wayToRelations.put(member.ref(), encodeRelationMembership(member.role(), relation.id()));
                    } else if (type == OsmElement.Type.RELATION) {
                      relationToParentRelations.put(member.ref(),
                        encodeRelationMembership(member.role(), relation.id()));
                    }
                  }
                }
              }
            } catch (Exception e) {
              LOGGER.error("Error preprocessing OSM relation " + relation.id(), e);
            }
            // TODO allow limiting multipolygon storage to only ones that profile cares about
            if (isMultipolygon(relation)) {
              synchronized (waysInMultipolygonLock) {
                for (var member : relation.members()) {
                  if (member.type() == OsmElement.Type.WAY) {
                    waysInMultipolygon.add(member.ref());
                  }
                }
              }
            }
            // Track relations that need member processing (relation_members geometry)
            // Check if any of the relation infos is a RelationMembersInfo by class name
            // (can't import directly since it's in a different module)
            List<OsmRelationInfo> infos = relationInfo.get(relation.id()) != null ?
              List.of(relationInfo.get(relation.id())) : null;
            if (infos != null && !infos.isEmpty()) {
              for (OsmRelationInfo info : infos) {
                // Check if this is a RelationMembersInfo by class name
                String className = info.getClass().getName();
                if (className.contains("RelationMembersInfo")) {
                  synchronized (relationsForMemberProcessingLock) {
                    relationsForMemberProcessing.add(relation.id());
                  }
                  // Track member ways and nodes for this relation
                  synchronized (waysInRelationMembersLock) {
                    synchronized (nodesInRelationMembersLock) {
                      for (var member : relation.members()) {
                        if (member.type() == OsmElement.Type.WAY) {
                          waysInRelationMembers.add(member.ref());
                        } else if (member.type() == OsmElement.Type.NODE) {
                          nodesInRelationMembers.add(member.ref());
                        }
                      }
                    }
                  }
                  break;
                }
              }
            }
          }
        }
        PASS1_BLOCKS.inc();
      }
    }
  }

  private static boolean isMultipolygon(OsmElement.Relation relation) {
    return relation.hasTag("type", "multipolygon", "boundary", "land_area") &&
      relation.members().stream().anyMatch(m -> m.type() == OsmElement.Type.WAY);
  }

  /**
   * Constructs geometries from OSM elements and emits map features as defined by the {@link Profile}.
   *
   * @param writer consumer that will store finished features
   * @param config user-provided arguments to control the number of threads, and log interval
   */
  public void pass2(FeatureGroup writer, PlanetilerConfig config) {
    var timer = stats.startStage("osm_pass2");
    int writeThreads = config.featureWriteThreads();
    int processThreads = config.featureProcessThreads();
    Counter.MultiThreadCounter blocksProcessed = Counter.newMultiThreadCounter();
    // track relation count separately because they get enqueued onto the distributor near the end
    Counter.MultiThreadCounter relationsProcessed = Counter.newMultiThreadCounter();
    OsmPhaser pass2Phaser = new OsmPhaser(processThreads);
    stats.counter("osm_pass2_elements_processed", "type", () -> Map.of(
      "blocks", blocksProcessed::get,
      "nodes", pass2Phaser::nodes,
      "ways", pass2Phaser::ways,
      "relations", relationsProcessed
    ));

    // Use a Distributor to keep all worker threads busy when processing the final blocks of relations by offloading
    // items to threads that are done reading blocks
    Distributor<OsmElement.Relation> relationDistributor = Distributor.createWithCapacity(1_000);

    var pipeline = WorkerPipeline.start("osm_pass2", stats)
      .fromGenerator("read", osmBlockSource::forEachBlock)
      .addBuffer("pbf_blocks", Math.max(10, processThreads / 2))
      .<SortableFeature>addWorker("process", processThreads, (prev, next) -> {
        // avoid contention trying to get the thread-local counters by getting them once when thread starts
        Counter blocks = blocksProcessed.counterForThread();
        Counter rels = relationsProcessed.counterForThread();

        var featureCollectors = new FeatureCollector.Factory(config, stats);
        final NodeLocationProvider nodeLocations = newNodeLocationProvider();
        try (var renderer = createFeatureRenderer(writer, config, next)) {
          var phaser = pass2Phaser.forWorker();
          var relationHandler = relationDistributor.forThread(relation -> {
            var feature = processRelationPass2(relation, nodeLocations);
            if (feature != null) {
              render(featureCollectors, renderer, relation, feature);
            }
            rels.inc();
          });
          for (var block : prev) {
            for (var element : block.decodeElements()) {
              SourceFeature feature = null;
              if (element instanceof OsmElement.Node node) {
                phaser.arrive(OsmPhaser.Phase.NODES);
                feature = processNodePass2(node);
              } else if (element instanceof OsmElement.Way way) {
                phaser.arrive(OsmPhaser.Phase.WAYS);
                feature = processWayPass2(way, nodeLocations);
              } else if (element instanceof OsmElement.Relation relation) {
                phaser.arriveAndWaitForOthers(OsmPhaser.Phase.RELATIONS);
                relationHandler.accept(relation);
              }
              // render features specified by profile and hand them off to next step that will
              // write them intermediate storage
              if (feature != null) {
                render(featureCollectors, renderer, element, feature);
              }
            }
            blocks.inc();
          }

          phaser.close();

          // do work for other threads that are still processing blocks of relations
          relationHandler.close();
        }
      }).addBuffer("feature_queue", 50_000, 1_000)
      // FeatureGroup writes need to be single-threaded
      .sinkTo("write", writeThreads, prev -> {
        try (var writerForThread = writer.writerForThread()) {
          for (var item : prev) {
            writerForThread.accept(item);
          }
        }
      });

    var logger = ProgressLoggers.create()
      .addRatePercentCounter("nodes", pass1Phaser.nodes(), pass2Phaser::nodes, true)
      .addFileSizeAndRam(nodeLocationDb)
      .addRatePercentCounter("ways", pass1Phaser.ways(), pass2Phaser::ways, true)
      .addRatePercentCounter("rels", pass1Phaser.relations(), relationsProcessed, true)
      .addRateCounter("features", writer::numFeaturesWritten)
      .addFileSize(writer)
      .addRatePercentCounter("blocks", PASS1_BLOCKS.get(), blocksProcessed, false)
      .newLine()
      .addProcessStats()
      .addInMemoryObject("relInfo", this)
      .addFileSizeAndRam("mpGeoms", multipolygonWayGeometries)
      .newLine()
      .addPipelineStats(pipeline);

    pipeline.awaitAndLog(logger, config.logInterval());

    LOGGER.debug("Processed " + FORMAT.integer(blocksProcessed.get()) + " blocks:");
    pass2Phaser.printSummary();

    timer.stop();

    try (
      var writerForThread = writer.writerForThread();
      var renderer = createFeatureRenderer(writer, config, writerForThread)
    ) {
      profile.finish(name, new FeatureCollector.Factory(config, stats), renderer);
    } catch (Exception e) {
      LOGGER.error("Error calling profile.finish", e);
    }
  }

  /** Estimates the resource requirements for a nodemap but parses the type/storage from strings. */
  public static ResourceUsage estimateNodeLocationUsage(String type, String storage, long osmFileSize, Path path) {
    return estimateNodeLocationUsage(LongLongMap.Type.from(type), Storage.from(storage), osmFileSize, path);
  }

  /** Estimates the resource requirements for a nodemap for a given OSM input file. */
  public static ResourceUsage estimateNodeLocationUsage(LongLongMap.Type type, Storage storage, long osmFileSize,
    Path path) {
    long nodes = estimateNumNodes(osmFileSize);
    long maxNodeId = estimateMaxNodeId(osmFileSize);

    ResourceUsage check = new ResourceUsage("nodemap");

    return switch (type) {
      case NOOP -> check;
      case SPARSE_ARRAY -> check.addMemory(300_000_000L, "sparsearray node location in-memory index")
        .add(path, storage, 9 * nodes, "sparsearray node location cache");
      case SORTED_TABLE -> check.addMemory(300_000_000L, "sortedtable node location in-memory index")
        .add(path, storage, 12 * nodes, "sortedtable node location cache");
      case ARRAY -> check.add(path, storage, 8 * maxNodeId,
        "array node location cache (switch to sparsearray to reduce size)");
    };
  }

  /**
   * Estimates the resource requirements for a multipolygon geometry multimap but parses the type/storage from strings.
   */
  public static ResourceUsage estimateMultipolygonGeometryUsage(String storage, long osmFileSize, Path path) {
    return estimateMultipolygonGeometryUsage(Storage.from(storage), osmFileSize, path);
  }

  /** Estimates the resource requirements for a multipolygon geometry multimap for a given OSM input file. */
  public static ResourceUsage estimateMultipolygonGeometryUsage(Storage storage, long osmFileSize, Path path) {
    // Planet extract (62G) requires about 9.5G for way geometries
    long estimatedSize = (long) (9.5 * osmFileSize / 62);

    return new ResourceUsage("way geometry multipolygon")
      .add(path, storage, estimatedSize, "multipolygon way geometries");
  }

  private static long estimateNumNodes(long osmFileSize) {
    // On 2/14/2022, planet.pbf was 66691979646 bytes with ~7.5b nodes, so scale from there
    return Math.round(7_500_000_000d * (osmFileSize / 66_691_979_646d));
  }

  private static long estimateMaxNodeId(long osmFileSize) {
    // On 2/14/2022, planet.pbf was 66691979646 bytes and max node ID was ~9.5b, so scale from there
    // but don't go less than 9.5b in case it's an extract
    return Math.round(9_500_000_000d * Math.max(1, osmFileSize / 66_691_979_646d));
  }

  private void render(FeatureCollector.Factory featureCollectors, FeatureRenderer renderer, OsmElement element,
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

  private FeatureRenderer createFeatureRenderer(FeatureGroup writer, PlanetilerConfig config,
    Consumer<SortableFeature> next) {
    @SuppressWarnings("java:S2095") // closed by FeatureRenderer
    var encoder = writer.newRenderedFeatureEncoder();
    return new FeatureRenderer(
      config,
      rendered -> next.accept(encoder.apply(rendered)),
      stats,
      encoder
    );
  }

  SourceFeature processNodePass2(OsmElement.Node node) {
    // nodes are simple because they already contain their location
    // Store node tags if this node is a member of a relation we care about
    if (nodesInRelationMembers.contains(node.id())) {
      synchronized (relationMembersNodeTagsLock) {
        relationMembersNodeTags.put(node.id(), node.tags());
      }
    }
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
        multipolygonWayGeometries.replaceValues(way.id(), nodes);
      }
    }
    // Store way geometry and tags if this way is a member of a relation we care about
    if (waysInRelationMembers.contains(way.id())) {
      synchronized (this) {
        relationMembersWayGeometries.replaceValues(way.id(), nodes);
      }
      synchronized (relationMembersWayTagsLock) {
        relationMembersWayTags.put(way.id(), way.tags());
      }
    }
    boolean closed = nodes.size() > 1 && nodes.get(0) == nodes.get(nodes.size() - 1);
    // area tag used to differentiate between whether a closed way should be treated as a polygon or linestring
    String area = way.getString("area");
    List<RelationMember<OsmRelationInfo>> rels = getRelationMembershipForWay(way.id());
    return new WaySourceFeature(way, closed, area, nodeLocations, rels);
  }

  SourceFeature processRelationPass2(OsmElement.Relation rel, NodeLocationProvider nodeLocations) {
    // Relation info gets used during way processing, except multipolygons which we have to process after we've
    // stored all the node IDs for each way.
    if (isMultipolygon(rel)) {
      List<RelationMember<OsmRelationInfo>> parentRelations = getRelationMembershipForWay(rel.id());
      return new MultipolygonSourceFeature(rel, nodeLocations, parentRelations);
    } else if (relationsForMemberProcessing.contains(rel.id())) {
      // This relation needs member processing (relation_members geometry)
      List<RelationMember<OsmRelationInfo>> parentRelations = getRelationMembershipForWay(rel.id());
      RelationMemberDataProvider dataProvider = new RelationMemberDataProvider() {
        @Override
        public LongArrayList getWayGeometry(long wayId) {
          return relationMembersWayGeometries != null ? relationMembersWayGeometries.get(wayId) : null;
        }
        
        @Override
        public Map<String, Object> getWayTags(long wayId) {
          return relationMembersWayTags != null ? relationMembersWayTags.get(wayId) : null;
        }
        
        @Override
        public Map<String, Object> getNodeTags(long nodeId) {
          return relationMembersNodeTags != null ? relationMembersNodeTags.get(nodeId) : null;
        }
        
        @Override
        public org.locationtech.jts.geom.Coordinate getNodeCoordinate(long nodeId) {
          long encoded = nodeLocationDb.get(nodeId);
          if (encoded == LongLongMap.MISSING_VALUE) {
            return null;
          }
          return new org.locationtech.jts.geom.CoordinateXY(
            GeoUtils.decodeWorldX(encoded),
            GeoUtils.decodeWorldY(encoded)
          );
        }
      };
      return new RelationSourceFeature(rel, parentRelations, dataProvider);
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
          rels.add(new RelationMember<>(parsed.role, rel, List.of()));
        }
        LongArrayList parentRelations = relationToParentRelations.get(parsed.relationId);
        if (parentRelations.isEmpty()) {
          continue;
        }
        var visited = new HashSet<Long>();
        visited.add(parsed.relationId);
        for (int p = 0; p < parentRelations.size(); p++) {
          rels.addAll(getRelationInfosForRelationId(parentRelations.get(p), visited, List.of(parsed.relationId())));
        }
      }
    }
    return rels;
  }

  private List<RelationMember<OsmRelationInfo>> getRelationInfosForRelationId(long relationIdAndRole,
    HashSet<Long> visited, List<Long> parentRelationPath) {
    var parsed = decodeRelationMembership(relationIdAndRole);
    if (!visited.add(parsed.relationId)) {
      return List.of();
    }
    LongArrayList parentRelations = relationToParentRelations.get(parsed.relationId);
    List<RelationMember<OsmRelationInfo>> rels = new ArrayList<>(parentRelations.size());
    OsmRelationInfo relation = relationInfo.get(parsed.relationId);
    if (relation != null) {
      rels.add(new RelationMember<>(parsed.role, relation, parentRelationPath));
    }
    for (int p = 0; p < parentRelations.size(); p++) {
      rels.addAll(getRelationInfosForRelationId(parentRelations.get(p), visited,
        Stream.concat(parentRelationPath.stream(), Stream.of(parsed.relationId)).toList()));
    }
    return rels;
  }

  @Override
  public long estimateMemoryUsageBytes() {
    long size = 0;
    size += waysInMultipolygon == null ? 0 : waysInMultipolygon.serializedSizeInBytes();
    // multipolygonWayGeometries is reported separately
    size += waysInRelationMembers == null ? 0 : waysInRelationMembers.serializedSizeInBytes();
    size += nodesInRelationMembers == null ? 0 : nodesInRelationMembers.serializedSizeInBytes();
    size += estimateSize(relationMembersWayTags);
    size += estimateSize(relationMembersNodeTags);
    size += estimateSize(wayToRelations);
    size += estimateSize(relationToParentRelations);
    size += estimateSize(relationInfo);
    size += estimateSize(roleIdsReverse);
    size += estimateSize(roleIds);
    size += roleSizes.get();
    size += relationInfoSizes.get();
    return size;
  }

  @Override
  public void close() throws IOException {
    if (multipolygonWayGeometries != null) {
      multipolygonWayGeometries.close();
      multipolygonWayGeometries = null;
    }
    if (relationMembersWayGeometries != null) {
      relationMembersWayGeometries.close();
      relationMembersWayGeometries = null;
    }
    wayToRelations = null;
    relationToParentRelations = null;
    waysInMultipolygon = null;
    waysInRelationMembers = null;
    nodesInRelationMembers = null;
    relationMembersWayTags = null;
    relationMembersNodeTags = null;
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
   * @param <T>                type of the user-defined class storing information about the relation
   * @param role               "role" of the relation member
   * @param relation           user-provided data about the relation from pass1
   * @param parentRelationPath the sequence of relation IDs that were traversed to get to this relation (if it is a
   *                           super-relation). An empty list means this is a direct parent.
   */
  public record RelationMember<T extends OsmRelationInfo>(String role, T relation, List<Long> parentRelationPath) {

    public RelationMember(String role, T relation) {
      this(role, relation, List.of());
    }

    /** Returns {@code true} if this is from a super-relation that contains a relation this element belongs to. */
    public boolean isSuperRelation() {
      return !parentRelationPath.isEmpty();
    }
  }

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
   * A source feature generated from OSM elements. Inferring the geometry can be expensive, so each subclass is
   * constructed with the inputs necessary to create the geometry, but the geometry is constructed lazily on read.
   */
  private abstract class OsmFeature extends SourceFeature implements OsmSourceFeature {

    private final OsmElement originalElement;
    private final boolean polygon;
    private final boolean line;
    private final boolean point;
    private Geometry latLonGeom;
    private Geometry worldGeom;


    public OsmFeature(OsmElement elem, boolean point, boolean line, boolean polygon,
      List<RelationMember<OsmRelationInfo>> relationInfo) {
      super(elem.tags(), name, null, relationInfo, elem.id());
      this.originalElement = elem;
      this.point = point;
      this.line = line;
      this.polygon = polygon;
    }

    @Override
    public long vectorTileFeatureId(int multiplier) {
      return OsmElement.vectorTileFeatureId(multiplier, id(), originalElement.type());
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

    @Override
    public OsmElement originalElement() {
      return originalElement;
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
        GeoUtils.decodeWorldX(encodedLocation),
        GeoUtils.decodeWorldY(encodedLocation)
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

    public WaySourceFeature(OsmElement.Way way, boolean closed, String area, NodeLocationProvider nodeLocations,
      List<RelationMember<OsmRelationInfo>> relationInfo) {
      super(way, false,
        OsmReader.canBeLine(closed, area, way.nodes().size()),
        OsmReader.canBePolygon(closed, area, way.nodes().size()),
        relationInfo
      );
      this.nodeIds = way.nodes();
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
      return super.canBePolygon() ? polygon() : line();
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

    private final OsmElement.Relation relation;
    private final NodeLocationProvider nodeLocations;

    public MultipolygonSourceFeature(OsmElement.Relation relation, NodeLocationProvider nodeLocations,
      List<RelationMember<OsmRelationInfo>> parentRelations) {
      super(relation, false, false, true, parentRelations);
      this.relation = relation;
      this.nodeLocations = nodeLocations;
    }

    @Override
    protected Geometry computeWorldGeometry() throws GeometryException {
      List<LongArrayList> rings = new ArrayList<>(relation.members().size());
      Set<Long> added = new HashSet<>();
      for (OsmElement.Relation.Member member : relation.members()) {
        String role = member.role();
        LongArrayList poly = multipolygonWayGeometries.get(member.ref());
        if (member.type() == OsmElement.Type.WAY) {
          if (!added.add(member.ref())) {
            // ignore duplicate relation members
            stats.dataError("osm_" + relation.getTag("type") + "_duplicate_member");
          } else if (poly != null && !poly.isEmpty()) {
            rings.add(poly);
          } else {
            // boundary and land_area relations might not be complete for extracts, but multipolygons should be
            stats.dataError("osm_" + relation.getTag("type") + "_missing_way");
            LOGGER.trace(
              "Missing {} OsmWay[{}] for {} {}", role, member.ref(), relation.getTag("type"), this);
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
