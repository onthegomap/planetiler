package com.onthegomap.planetiler.collection;

import static com.onthegomap.planetiler.util.MutableCollections.makeMutable;

import com.carrotsearch.hppc.LongLongHashMap;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.GeometryType;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.geo.TileOrder;
import com.onthegomap.planetiler.render.RenderedFeature;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.CloseableConsumer;
import com.onthegomap.planetiler.util.CommonStringEncoder;
import com.onthegomap.planetiler.util.DiskBacked;
import com.onthegomap.planetiler.util.LayerAttrStats;
import com.onthegomap.planetiler.worker.Worker;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import net.jcip.annotations.NotThreadSafe;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility that accepts rendered map features in any order and groups them by tile for a reader to iterate through.
 * <p>
 * Only support single-threaded writes and reads.
 * <p>
 * Limitation: layer name and attribute key strings get compressed into a single byte, so only 250 unique values are
 * supported (see {@link CommonStringEncoder})
 */
@NotThreadSafe
public final class FeatureGroup implements Iterable<FeatureGroup.TileFeatures>, DiskBacked {

  public static final int SORT_KEY_BITS = 23;
  public static final int SORT_KEY_MAX = (1 << (SORT_KEY_BITS - 1)) - 1;
  public static final int SORT_KEY_MIN = -(1 << (SORT_KEY_BITS - 1));
  private static final int SORT_KEY_MASK = (1 << SORT_KEY_BITS) - 1;
  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureGroup.class);
  private final FeatureSort sorter;
  private final Profile profile;
  private final CommonStringEncoder.AsByte commonLayerStrings = new CommonStringEncoder.AsByte();
  private final CommonStringEncoder commonValueStrings = new CommonStringEncoder(100_000);
  private final Stats stats;
  private final PlanetilerConfig config;
  private volatile boolean prepared = false;
  private final TileOrder tileOrder;


  FeatureGroup(FeatureSort sorter, TileOrder tileOrder, Profile profile, PlanetilerConfig config, Stats stats) {
    this.sorter = sorter;
    this.tileOrder = tileOrder;
    this.profile = profile;
    this.config = config;
    this.stats = stats;
  }

  /** Returns a feature grouper that stores all feature in-memory. Only suitable for toy use-cases like unit tests. */
  public static FeatureGroup newInMemoryFeatureGroup(TileOrder tileOrder, Profile profile, PlanetilerConfig config,
    Stats stats) {
    return new FeatureGroup(FeatureSort.newInMemory(), tileOrder, profile, config, stats);
  }


  /**
   * Returns a feature grouper that writes all elements to disk in chunks, sorts each chunk, then reads back in order
   * from those chunks. Suitable for making maps up to planet-scale.
   */
  public static FeatureGroup newDiskBackedFeatureGroup(TileOrder tileOrder, Path tempDir, Profile profile,
    PlanetilerConfig config, Stats stats) {
    return new FeatureGroup(
      new ExternalMergeSort(tempDir, config, stats),
      tileOrder, profile, config, stats
    );
  }

  /**
   * Encode key by {@code tile} asc, {@code layer} asc, {@code sortKey} asc with an extra bit to indicate whether the
   * value contains grouping information.
   */
  static long encodeKey(int tile, byte layer, int sortKey, boolean hasGroup) {
    return ((long) tile << 32L) | ((long) (layer & 0xff) << 24L) | (((sortKey - SORT_KEY_MIN) & SORT_KEY_MASK) << 1L) |
      (hasGroup ? 1 : 0);
  }

  static boolean extractHasGroupFromKey(long key) {
    return (key & 1) == 1;
  }

  static int extractTileFromKey(long key) {
    return (int) (key >> 32L);
  }

  static byte extractLayerIdFromKey(long key) {
    return (byte) (key >> 24);
  }

  static int extractSortKeyFromKey(long key) {
    return ((int) ((key >> 1) & SORT_KEY_MASK)) + SORT_KEY_MIN;
  }

  private static RenderedFeature.Group peekAtGroupInfo(byte[] encoded) {
    try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(encoded)) {
      long group = unpacker.unpackLong();
      int limit = unpacker.unpackInt();
      return new RenderedFeature.Group(group, limit);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  static GeometryType decodeGeomType(byte geomTypeAndScale) {
    return GeometryType.valueOf((byte) (geomTypeAndScale & 0b111));
  }

  static int decodeScale(byte geomTypeAndScale) {
    return (geomTypeAndScale & 0xff) >>> 3;
  }

  static byte encodeGeomTypeAndScale(VectorTile.VectorGeometry geometry) {
    assert geometry.geomType().asByte() >= 0 && geometry.geomType().asByte() <= 8;
    assert geometry.scale() >= 0 && geometry.scale() < (1 << 5);
    return (byte) ((geometry.geomType().asByte() & 0xff) | (geometry.scale() << 3));
  }

  public long numFeaturesWritten() {
    return sorter.numFeaturesWritten();
  }

  /** Returns a function for a single thread to use to serialize rendered features. */
  public RenderedFeatureEncoder newRenderedFeatureEncoder() {
    return new RenderedFeatureEncoder() {
      // This method gets called billions of times when generating the planet, so these optimizations make a big difference:
      // 1) Re-use the same buffer packer to avoid allocating and resizing new byte arrays for every feature.
      private final MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
      // 2) Avoid re-encoding values for identical filled geometries (i.e. ocean) by memoizing the encoded values
      // FeatureRenderer ensures that a separate VectorTileEncoder.Feature is used for each zoom level
      private VectorTile.Feature lastFeature = null;
      private byte[] lastEncodedValue = null;

      @Override
      public SortableFeature apply(RenderedFeature feature) {
        var group = feature.group().orElse(null);
        var thisFeature = feature.vectorTileFeature();
        byte[] encodedValue;
        if (group != null) { // don't bother memoizing if group is present
          encodedValue = encodeValue(thisFeature, group, packer);
        } else if (lastFeature == thisFeature) {
          encodedValue = lastEncodedValue;
        } else { // feature changed, memoize new value
          lastFeature = thisFeature;
          lastEncodedValue = encodedValue = encodeValue(feature.vectorTileFeature(), null, packer);
        }

        return new SortableFeature(encodeKey(feature), encodedValue);
      }

      @Override
      public void close() throws IOException {
        packer.close();
      }
    };
  }

  private long encodeKey(RenderedFeature feature) {
    var vectorTileFeature = feature.vectorTileFeature();
    byte encodedLayer = commonLayerStrings.encode(vectorTileFeature.layer());

    return encodeKey(
      this.tileOrder.encode(feature.tile()),
      encodedLayer,
      feature.sortKey(),
      feature.group().isPresent()
    );
  }

  private byte[] encodeValue(VectorTile.Feature vectorTileFeature, RenderedFeature.Group group,
    MessageBufferPacker packer) {
    packer.clear();
    try {
      // hasGroup bit in key will tell consumers whether they need to decode group info from value
      if (group != null) {
        packer.packLong(group.group());
        packer.packInt(group.limit());
      }
      packer.packLong(vectorTileFeature.id());
      packer.packByte(encodeGeomTypeAndScale(vectorTileFeature.geometry()));
      var attrs = vectorTileFeature.tags();
      packer.packMapHeader((int) attrs.values().stream().filter(Objects::nonNull).count());
      for (Map.Entry<String, Object> entry : attrs.entrySet()) {
        Object value = entry.getValue();
        if (value != null) {
          packer.packInt(commonValueStrings.encode(entry.getKey()));
          packer.packValue(switch (value) {
            case String string -> ValueFactory.newString(string);
            case Integer integer -> ValueFactory.newInteger(integer.longValue());
            case Long longValue -> ValueFactory.newInteger(longValue);
            case Float floatValue -> ValueFactory.newFloat(floatValue);
            case Double doubleValue -> ValueFactory.newFloat(doubleValue);
            case Boolean booleanValue -> ValueFactory.newBoolean(booleanValue);
            case Object other -> ValueFactory.newString(other.toString());
          });
        }
      }
      // Use the same binary format for encoding geometries in output vector tiles. Benchmarking showed
      // it was faster and smaller for encoding/decoding intermediate geometries than alternatives like WKB.
      int[] commands = vectorTileFeature.geometry().commands();
      packer.packArrayHeader(commands.length);
      for (int command : commands) {
        packer.packInt(command);
      }
      packer.close();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    return packer.toByteArray();
  }

  /** Returns a new feature writer that can be used for a single thread. */
  public CloseableConsumer<SortableFeature> writerForThread() {
    return sorter.writerForThread();
  }

  @Override
  public Iterator<TileFeatures> iterator() {
    prepare();
    return groupIntoTiles(sorter.iterator());
  }

  /**
   * Reads temp features using {@code threads} parallel threads and merges into a sorted list.
   *
   * @param threads The number of parallel read threads to spawn
   * @return a {@link Reader} with a handle to the new read threads that were spawned, and in {@link Iterable} that can
   *         be used to iterate over the results.
   */
  public Reader parallelIterator(int threads) {
    prepare();
    var parIter = sorter.parallelIterator(stats, threads);
    return new Reader(parIter.reader(), () -> groupIntoTiles(parIter.iterator()));
  }

  private Iterator<TileFeatures> groupIntoTiles(Iterator<SortableFeature> entries) {
    // entries are sorted by tile ID, so group consecutive entries in same tile into tiles
    if (!entries.hasNext()) {
      return Collections.emptyIterator();
    }

    /*
     * Features from sorter are ordered by tile, so iterate through features as long as
     * they are in the same tile and return that group.
     */
    SortableFeature firstFeature = entries.next();
    return new Iterator<>() {
      private SortableFeature lastFeature = firstFeature;
      private int lastTileId = extractTileFromKey(firstFeature.key());

      @Override
      public boolean hasNext() {
        return lastFeature != null;
      }

      @Override
      public TileFeatures next() {
        TileFeatures result = new TileFeatures(lastTileId);
        result.add(lastFeature);
        int lastTile = lastTileId;

        while (entries.hasNext()) {
          SortableFeature next = entries.next();
          lastFeature = next;
          lastTileId = extractTileFromKey(lastFeature.key());
          if (lastTile != lastTileId) {
            return result;
          }
          result.add(next);
        }
        lastFeature = null;
        return result;
      }
    };
  }

  @Override
  public long diskUsageBytes() {
    return sorter.diskUsageBytes();
  }

  /** Sorts features to prepare for grouping after all features have been written. */
  public void prepare() {
    if (!prepared) {
      synchronized (this) {
        if (!prepared) {
          sorter.sort();
          prepared = true;
        }
      }
    }
  }

  public int chunksToRead() {
    return sorter.chunksToRead();
  }

  public interface RenderedFeatureEncoder extends Function<RenderedFeature, SortableFeature>, Closeable {}

  public record Reader(Worker readWorker, Iterable<TileFeatures> result) {}

  /** Features contained in a single tile. */
  public class TileFeatures {

    private final TileCoord tileCoord;
    private final List<SortableFeature> entries = new ArrayList<>();
    private final AtomicLong numFeaturesProcessed = new AtomicLong(0);
    private LongLongHashMap counts = null;
    private byte lastLayer = Byte.MAX_VALUE;

    private TileFeatures(int lastTileId) {
      this.tileCoord = tileOrder.decode(lastTileId);
    }

    private static void unscaleAndRemovePointsOutsideBuffer(List<VectorTile.Feature> features, double maxPointBuffer) {
      boolean checkPoints = maxPointBuffer <= 256 && maxPointBuffer >= -128;
      for (int i = 0; i < features.size(); i++) {
        var feature = features.get(i);
        if (feature != null) {
          VectorTile.VectorGeometry geometry = feature.geometry();
          var orig = geometry;
          if (geometry.scale() != 0) {
            geometry = geometry.unscale();
          }
          if (checkPoints && geometry.geomType() == GeometryType.POINT && !geometry.isEmpty()) {
            geometry = geometry.filterPointsOutsideBuffer(maxPointBuffer);
          }
          if (geometry.isEmpty()) {
            features.set(i, null);
          } else if (geometry != orig) {
            features.set(i, feature.copyWithNewGeometry(geometry));
          }
        }
      }
    }

    /** Returns the number of features read including features discarded from being over the limit in a group. */
    public long getNumFeaturesProcessed() {
      return numFeaturesProcessed.get();
    }

    /** Returns the number of features to output, excluding features discarded from being over the limit in a group. */
    public long getNumFeaturesToEmit() {
      return entries.size();
    }

    public TileCoord tileCoord() {
      return tileCoord;
    }

    /**
     * Returns true if {@code other} contains features with identical layer, geometry, and attributes, as this tile -
     * even if the tiles have separate coordinates.
     * <p>
     * Used as an optimization to avoid re-encoding the same ocean tiles over and over again.
     */
    public boolean hasSameContents(TileFeatures other) {
      if (other == null || other.entries.size() != entries.size()) {
        return false;
      }
      for (int i = 0; i < entries.size(); i++) {
        SortableFeature a = entries.get(i);
        SortableFeature b = other.entries.get(i);
        long layerA = extractLayerIdFromKey(a.key());
        long layerB = extractLayerIdFromKey(b.key());
        if (layerA != layerB || !Arrays.equals(a.value(), b.value())) {
          return false;
        }
      }
      return true;
    }


    private VectorTile.Feature decodeVectorTileFeature(SortableFeature entry) {
      try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(entry.value())) {
        long group;
        if (extractHasGroupFromKey(entry.key())) {
          group = unpacker.unpackLong();
          unpacker.unpackInt(); // groupLimit - features over the limit were already discarded
        } else {
          group = VectorTile.Feature.NO_GROUP;
        }
        long id = unpacker.unpackLong();
        byte geomTypeAndScale = unpacker.unpackByte();
        GeometryType geomType = decodeGeomType(geomTypeAndScale);
        int scale = decodeScale(geomTypeAndScale);
        int mapSize = unpacker.unpackMapHeader();
        Map<String, Object> attrs = HashMap.newHashMap(mapSize);
        for (int i = 0; i < mapSize; i++) {
          String key = commonValueStrings.decode(unpacker.unpackInt());
          Value v = unpacker.unpackValue();
          if (v.isStringValue()) {
            attrs.put(key, v.asStringValue().asString());
          } else if (v.isIntegerValue()) {
            attrs.put(key, v.asIntegerValue().toLong());
          } else if (v.isFloatValue()) {
            attrs.put(key, v.asFloatValue().toDouble());
          } else if (v.isBooleanValue()) {
            attrs.put(key, v.asBooleanValue().getBoolean());
          }
        }
        int commandSize = unpacker.unpackArrayHeader();
        int[] commands = new int[commandSize];
        for (int i = 0; i < commandSize; i++) {
          commands[i] = unpacker.unpackInt();
        }
        String layer = commonLayerStrings.decode(extractLayerIdFromKey(entry.key()));
        return new VectorTile.Feature(
          layer,
          id,
          new VectorTile.VectorGeometry(commands, geomType, scale),
          attrs,
          group
        );
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }

    public VectorTile getVectorTile() {
      return getVectorTile(null);
    }

    public VectorTile getVectorTile(LayerAttrStats.Updater layerStats) {
      VectorTile tile = new VectorTile();
      if (layerStats != null) {
        tile.trackLayerStats(layerStats.forZoom(tileCoord.z()));
      }
      List<VectorTile.Feature> items = new ArrayList<>();
      String currentLayer = null;
      Map<String, List<VectorTile.Feature>> layerFeatures = new TreeMap<>();
      for (SortableFeature entry : entries) {
        var feature = decodeVectorTileFeature(entry);
        String layer = feature.layer();

        if (currentLayer == null) {
          currentLayer = layer;
          layerFeatures.put(currentLayer, items);
        } else if (!currentLayer.equals(layer)) {
          currentLayer = layer;
          items = new ArrayList<>();
          layerFeatures.put(layer, items);
        }

        items.add(feature);
      }
      // first post-process entire tile by invoking postProcessTileFeatures to allow for post-processing that combines
      // features across different layers, infers new layers, or removes layers
      try {
        var initialFeatures = layerFeatures;
        layerFeatures = profile.postProcessTileFeatures(tileCoord, layerFeatures);
        if (layerFeatures == null) {
          layerFeatures = initialFeatures;
        }
      } catch (Throwable e) { // NOSONAR - OK to catch Throwable since we re-throw Errors
        handlePostProcessFailure(e, "entire tile");
      }
      // then let profiles post-process each layer in isolation with postProcessLayerFeatures
      for (var entry : layerFeatures.entrySet()) {
        postProcessAndAddLayerFeatures(tile, entry.getKey(), entry.getValue());
      }
      return tile;
    }

    private void postProcessAndAddLayerFeatures(VectorTile encoder, String layer,
      List<VectorTile.Feature> features) {
      if (features == null || features.isEmpty()) {
        return;
      }
      try {
        List<VectorTile.Feature> postProcessed = makeMutable(profile
          .postProcessLayerFeatures(layer, tileCoord.z(), makeMutable(features)));
        features = postProcessed == null ? features : postProcessed;
        // lines are stored using a higher precision so that rounding does not
        // introduce artificial intersections between endpoints to confuse line merging,
        // so we have to reduce the precision here, now that line merging is done.
        unscaleAndRemovePointsOutsideBuffer(features, config.maxPointBuffer());
        // also remove points more than --max-point-buffer pixels outside the tile if the
        // user has requested a narrower buffer than the profile provides by default
      } catch (Throwable e) { // NOSONAR - OK to catch Throwable since we re-throw Errors
        handlePostProcessFailure(e, layer);
      }
      encoder.addLayerFeatures(layer, features);
    }

    private void handlePostProcessFailure(Throwable e, String entity) {
      // failures in tile post-processing happen very late so err on the side of caution and
      // log failures, only throwing when it's a fatal error
      if (e instanceof GeometryException geoe) {
        geoe.log(stats, "postprocess_layer",
          "Caught error postprocessing features for " + entity + " on " + tileCoord, config.logJtsExceptions());
      } else if (e instanceof Error err) {
        LOGGER.error("Caught fatal error postprocessing features {} {}", entity, tileCoord, e);
        throw err;
      } else {
        LOGGER.error("Caught error postprocessing features {} {}", entity, tileCoord, e);
      }
    }

    void add(SortableFeature entry) {
      numFeaturesProcessed.incrementAndGet();
      long key = entry.key();
      if (extractHasGroupFromKey(key)) {
        byte thisLayer = extractLayerIdFromKey(key);
        if (counts == null) {
          counts = Hppc.newLongLongHashMap();
          lastLayer = thisLayer;
        } else if (thisLayer != lastLayer) {
          lastLayer = thisLayer;
          counts.clear();
        }
        var groupInfo = peekAtGroupInfo(entry.value());
        long old = counts.getOrDefault(groupInfo.group(), 0);
        if (groupInfo.limit() > 0 && old >= groupInfo.limit()) {
          // discard if there are to many features in this group already
          return;
        }
        counts.put(groupInfo.group(), old + 1);
      }
      entries.add(entry);
    }

    @Override
    public String toString() {
      return "TileFeatures{" +
        "tile=" + tileCoord +
        ", num entries=" + entries.size() +
        '}';
    }
  }
}
