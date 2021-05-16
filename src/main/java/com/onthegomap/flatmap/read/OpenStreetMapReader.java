package com.onthegomap.flatmap.read;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongDoubleHashMap;
import com.carrotsearch.hppc.LongHashSet;
import com.graphhopper.coll.GHLongHashSet;
import com.graphhopper.coll.GHLongObjectHashMap;
import com.graphhopper.reader.ReaderElement;
import com.graphhopper.reader.ReaderElementUtils;
import com.graphhopper.reader.ReaderNode;
import com.graphhopper.reader.ReaderRelation;
import com.graphhopper.reader.ReaderWay;
import com.onthegomap.flatmap.CommonParams;
import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.FeatureRenderer;
import com.onthegomap.flatmap.MemoryEstimator;
import com.onthegomap.flatmap.Profile;
import com.onthegomap.flatmap.SourceFeature;
import com.onthegomap.flatmap.collections.FeatureGroup;
import com.onthegomap.flatmap.collections.FeatureSort;
import com.onthegomap.flatmap.collections.LongLongMap;
import com.onthegomap.flatmap.collections.LongLongMultimap;
import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.monitoring.ProgressLoggers;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.worker.Topology;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.impl.PackedCoordinateSequence;

public class OpenStreetMapReader implements Closeable, MemoryEstimator.HasEstimate {

  private final OsmInputFile osmInputFile;
  private final Stats stats;
  private final LongLongMap nodeDb;
  private final AtomicLong TOTAL_NODES = new AtomicLong(0);
  private final AtomicLong TOTAL_WAYS = new AtomicLong(0);
  private final AtomicLong TOTAL_RELATIONS = new AtomicLong(0);
  private final Profile profile;

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

  public OpenStreetMapReader(OsmInputFile osmInputFile, LongLongMap nodeDb, Profile profile, Stats stats) {
    this.osmInputFile = osmInputFile;
    this.nodeDb = nodeDb;
    this.stats = stats;
    this.profile = profile;
  }

  public void pass1(CommonParams config) {
    var topology = Topology.start("osm_pass1", stats)
      .fromGenerator("pbf", osmInputFile.read(config.threads() - 1))
      .addBuffer("reader_queue", 50_000, 10_000)
      .sinkToConsumer("process", 1, (readerElement) -> {
        if (readerElement instanceof ReaderNode node) {
          TOTAL_NODES.incrementAndGet();
          nodeDb.put(node.getId(), GeoUtils.encodeFlatLocation(node.getLon(), node.getLat()));
        } else if (readerElement instanceof ReaderWay) {
          TOTAL_WAYS.incrementAndGet();
        } else if (readerElement instanceof ReaderRelation rel) {
          TOTAL_RELATIONS.incrementAndGet();
          List<RelationInfo> infos = profile.preprocessOsmRelation(rel);
          if (infos != null) {
            for (RelationInfo info : infos) {
              relationInfo.put(rel.getId(), info);
              relationInfoSizes.addAndGet(info.estimateMemoryUsageBytes());
              for (ReaderRelation.Member member : rel.getMembers()) {
                if (member.getType() == ReaderRelation.Member.WAY) {
                  wayToRelations.put(member.getRef(), rel.getId());
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
      });

    var loggers = new ProgressLoggers("osm_pass1")
      .addRateCounter("nodes", TOTAL_NODES)
      .addFileSize(nodeDb::fileSize)
      .addRateCounter("ways", TOTAL_WAYS)
      .addRateCounter("rels", TOTAL_RELATIONS)
      .addProcessStats()
      .addInMemoryObject("hppc", this)
      .addThreadPoolStats("parse", "pool-")
      .addTopologyStats(topology);
    topology.awaitAndLog(loggers, config.logInterval());
  }

  public void pass2(FeatureGroup writer, CommonParams config) {
    int readerThreads = Math.max(config.threads() / 4, 1);
    int processThreads = config.threads() - 1;
    AtomicLong nodesProcessed = new AtomicLong(0);
    AtomicLong waysProcessed = new AtomicLong(0);
    AtomicLong relsProcessed = new AtomicLong(0);
    CountDownLatch waysDone = new CountDownLatch(processThreads);

    var topology = Topology.start("osm_pass2", stats)
      .fromGenerator("pbf", osmInputFile.read(readerThreads))
      .addBuffer("reader_queue", 50_000, 1_000)
      .<FeatureSort.Entry>addWorker("process", processThreads, (prev, next) -> {
        ReaderElement readerElement;
        var featureCollectors = new FeatureCollector.Factory(config);
        NodeGeometryCache nodeCache = new NodeGeometryCache();
        var encoder = writer.newRenderedFeatureEncoder();
        FeatureRenderer renderer = new FeatureRenderer(
          config,
          rendered -> next.accept(encoder.apply(rendered))
        );
        while ((readerElement = prev.get()) != null) {
          SourceFeature feature = null;
          if (readerElement instanceof ReaderNode node) {
            nodesProcessed.incrementAndGet();
            feature = new NodeSourceFeature(node);
          } else if (readerElement instanceof ReaderWay way) {
            waysProcessed.incrementAndGet();
            feature = new WaySourceFeature(way, nodeCache);
          } else if (readerElement instanceof ReaderRelation rel) {
            // ensure all ways finished processing before we start relations
            if (waysDone.getCount() > 0) {
              waysDone.countDown();
              waysDone.await();
            }
            relsProcessed.incrementAndGet();
            if (rel.hasTag("type", "multipolygon")) {
              feature = new MultipolygonSourceFeature(rel);
            }
          }
          if (feature != null) {
            FeatureCollector features = featureCollectors.get(feature);
            profile.processFeature(feature, features);
            for (FeatureCollector.Feature<?> renderable : features) {
              renderer.renderFeature(renderable);
            }
          }
          nodeCache.reset();
        }

        // just in case a worker skipped over all relations
        waysDone.countDown();
      }).addBuffer("feature_queue", 50_000, 1_000)
      .sinkToConsumer("write", 1, writer);

    var logger = new ProgressLoggers("osm_pass2")
      .addRatePercentCounter("nodes", TOTAL_NODES.get(), nodesProcessed)
      .addFileSize(nodeDb::fileSize)
      .addRatePercentCounter("ways", TOTAL_WAYS.get(), waysProcessed)
      .addRatePercentCounter("rels", TOTAL_RELATIONS.get(), relsProcessed)
      .addRateCounter("features", () -> writer.sorter().size())
      .addFileSize(writer::getStorageSize)
      .addProcessStats()
      .addInMemoryObject("hppc", this)
      .addThreadPoolStats("parse", "pool-")
      .addTopologyStats(topology);

    topology.awaitAndLog(logger, config.logInterval());
  }

  @Override
  public long estimateMemoryUsageBytes() {
    long size = 0;
    size += MemoryEstimator.size(waysInMultipolygon);
    size += MemoryEstimator.size(multipolygonWayGeometries);
    size += MemoryEstimator.size(wayToRelations);
    size += MemoryEstimator.sizeWithoutValues(relationInfo);
    size += relationInfoSizes.get();
    return size;
  }


  @Override
  public void close() throws IOException {
    multipolygonWayGeometries = null;
    wayToRelations = null;
    waysInMultipolygon = null;
    relationInfo = null;
    nodeDb.close();
  }

  public static class RelationInfo implements MemoryEstimator.HasEstimate {

    @Override
    public long estimateMemoryUsageBytes() {
      return 0;
    }
  }

  private static abstract class ProxyFeature extends SourceFeature {

    public ProxyFeature(ReaderElement elem) {
      super(ReaderElementUtils.getProperties(elem));
    }

    private Geometry latLonGeom;

    @Override
    public Geometry latLonGeometry() {
      return latLonGeom != null ? latLonGeom : (latLonGeom = GeoUtils.latLonToWorldCoords(worldGeometry()));
    }

    private Geometry worldGeom;

    @Override
    public Geometry worldGeometry() {
      return worldGeom != null ? worldGeom : (worldGeom = computeWorldGeometry());
    }

    protected abstract Geometry computeWorldGeometry();
  }

  private static class NodeSourceFeature extends ProxyFeature {

    private final double lon;
    private final double lat;

    NodeSourceFeature(ReaderNode node) {
      super(node);
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
  }

  private static class WaySourceFeature extends ProxyFeature {

    private final NodeGeometryCache nodeCache;
    private final LongArrayList nodeIds;

    public WaySourceFeature(ReaderWay way, NodeGeometryCache nodeCache) {
      super(way);
      this.nodeIds = way.getNodes();
      this.nodeCache = nodeCache;
    }

    @Override
    protected Geometry computeWorldGeometry() {
      return null;
    }

    @Override
    public boolean isPoint() {
      return false;
    }
  }

  private static class MultipolygonSourceFeature extends ProxyFeature {

    public MultipolygonSourceFeature(ReaderRelation relation) {
      super(relation);
    }

    @Override
    protected Geometry computeWorldGeometry() {
      return null;
    }

    @Override
    public boolean isPoint() {
      return false;
    }
  }

  private class NodeGeometryCache {

    private final LongDoubleHashMap xs = new LongDoubleHashMap();
    private final LongDoubleHashMap ys = new LongDoubleHashMap();

    public CoordinateSequence getWayGeometry(LongArrayList nodeIds) {
      int num = nodeIds.size();
      CoordinateSequence seq = new PackedCoordinateSequence.Double(nodeIds.size(), 2, 0);

      for (int i = 0; i < num; i++) {
        long id = nodeIds.get(i);
        double worldX, worldY;
        worldX = xs.getOrDefault(id, Double.NaN);
        if (Double.isNaN(worldX)) {
          long encoded = nodeDb.get(id);
          xs.put(id, worldX = GeoUtils.decodeWorldX(encoded));
          ys.put(id, worldY = GeoUtils.decodeWorldY(encoded));
        } else {
          worldY = ys.get(id);
        }
        seq.setOrdinate(i, 0, worldX);
        seq.setOrdinate(i, 1, worldY);
      }
      return seq;
    }

    public void reset() {
      xs.clear();
      ys.clear();
    }
  }
}
