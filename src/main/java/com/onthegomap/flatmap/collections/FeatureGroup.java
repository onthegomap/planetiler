package com.onthegomap.flatmap.collections;

import com.carrotsearch.hppc.LongLongHashMap;
import com.graphhopper.coll.GHLongLongHashMap;
import com.onthegomap.flatmap.GeometryType;
import com.onthegomap.flatmap.LayerStats;
import com.onthegomap.flatmap.Profile;
import com.onthegomap.flatmap.VectorTileEncoder;
import com.onthegomap.flatmap.geo.TileCoord;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.render.RenderedFeature;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FeatureGroup implements Consumer<FeatureSort.Entry>, Iterable<FeatureGroup.TileFeatures> {

  public static final int Z_ORDER_BITS = 23;
  public static final int Z_ORDER_MAX = (1 << (Z_ORDER_BITS - 1)) - 1;
  public static final int Z_ORDER_MIN = -(1 << (Z_ORDER_BITS - 1));
  private static final int Z_ORDER_MASK = (1 << Z_ORDER_BITS) - 1;
  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureGroup.class);
  private final FeatureSort sorter;
  private final Profile profile;
  private final CommonStringEncoder commonStrings;
  private final Stats stats;

  public FeatureGroup(FeatureSort sorter, Profile profile, CommonStringEncoder commonStrings, Stats stats) {
    this.sorter = sorter;
    this.profile = profile;
    this.commonStrings = commonStrings;
    this.stats = stats;
  }

  private final LayerStats layerStats = new LayerStats();

  public LayerStats layerStats() {
    return layerStats;
  }

  public FeatureGroup(FeatureSort sorter, Profile profile, Stats stats) {
    this(sorter, profile, new CommonStringEncoder(), stats);
  }

  static long encodeSortKey(int tile, byte layer, int zOrder, boolean hasGroup) {
    zOrder = -zOrder - 1;
    return ((long) tile << 32L) | ((long) (layer & 0xff) << 24L) | (((zOrder - Z_ORDER_MIN) & Z_ORDER_MASK) << 1L) | (
      hasGroup ? 1 : 0);
  }

  static boolean extractHasGroupFromSortKey(long sortKey) {
    return (sortKey & 1) == 1;
  }

  static int extractTileFromSortKey(long sortKey) {
    return (int) (sortKey >> 32L);
  }

  static byte extractLayerIdFromSortKey(long sortKey) {
    return (byte) (sortKey >> 24);
  }

  static int extractZorderFromKey(long sortKey) {
    return Z_ORDER_MAX - ((int) ((sortKey >> 1) & Z_ORDER_MASK));
  }

  private static RenderedFeature.Group decodeGroupInfo(byte[] encoded) {
    try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(encoded)) {
      long group = unpacker.unpackLong();
      int limit = unpacker.unpackInt();
      return new RenderedFeature.Group(group, limit);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public long numFeatures() {
    return sorter.size();
  }

  public Function<RenderedFeature, FeatureSort.Entry> newRenderedFeatureEncoder() {
    /*
     * Optimization: Re-use the same buffer packer to avoid allocating and resizing new byte arrays for every feature.
     */
    var packer = MessagePack.newDefaultBufferPacker();

    /*
     * Optimization: Avoid re-encoding values for identical fill geometries (ie. in the ocean) by memoizing based on
     * the input vector tile feature. FeatureRenderer ensures that all fill vector tile features use the same instance
     * within a zoom level (and filled tiles are ordered by z, x, y).
     */
    return new Function<>() {
      private VectorTileEncoder.Feature lastFeature = null;
      private byte[] lastEncodedValue = null;

      @Override
      public FeatureSort.Entry apply(RenderedFeature feature) {
        layerStats.accept(feature);
        var group = feature.group();
        var thisFeature = feature.vectorTileFeature();
        byte[] encodedValue;
        if (group.isEmpty()) { // don't bother memoizing if group is present
          encodedValue = encodeValue(thisFeature, group, packer);
        } else if (lastFeature == thisFeature) {
          encodedValue = lastEncodedValue;
        } else { // feature changed, memoize new value
          lastFeature = thisFeature;
          lastEncodedValue = encodedValue = encodeValue(feature.vectorTileFeature(), feature.group(), packer);
        }

        return new FeatureSort.Entry(encodeSortKey(feature), encodedValue);
      }
    };
  }

  private long encodeSortKey(RenderedFeature feature) {
    var vectorTileFeature = feature.vectorTileFeature();
    commonStrings.encode(vectorTileFeature.layer());
    return encodeSortKey(
      feature.tile().encoded(),
      commonStrings.encode(vectorTileFeature.layer()),
      feature.zOrder(),
      feature.group().isPresent()
    );
  }

  private byte[] encodeValue(VectorTileEncoder.Feature vectorTileFeature, Optional<RenderedFeature.Group> group,
    MessageBufferPacker packer) {
    packer.clear();
    try {
      var groupInfoOption = group;
      if (groupInfoOption.isPresent()) {
        var groupInfo = groupInfoOption.get();
        packer.packLong(groupInfo.group());
        packer.packInt(groupInfo.limit());
      }
      packer.packLong(vectorTileFeature.id());
      packer.packByte(vectorTileFeature.geometry().geomType().asByte());
      var attrs = vectorTileFeature.attrs();
      packer.packMapHeader((int) attrs.values().stream().filter(Objects::nonNull).count());
      for (Map.Entry<String, Object> entry : attrs.entrySet()) {
        if (entry.getValue() != null) {
          packer.packByte(commonStrings.encode(entry.getKey()));
          Object value = entry.getValue();
          if (value instanceof String) {
            packer.packValue(ValueFactory.newString((String) value));
          } else if (value instanceof Integer) {
            packer.packValue(ValueFactory.newInteger(((Integer) value).longValue()));
          } else if (value instanceof Long) {
            packer.packValue(ValueFactory.newInteger((Long) value));
          } else if (value instanceof Float) {
            packer.packValue(ValueFactory.newFloat((Float) value));
          } else if (value instanceof Double) {
            packer.packValue(ValueFactory.newFloat((Double) value));
          } else if (value instanceof Boolean) {
            packer.packValue(ValueFactory.newBoolean((Boolean) value));
          } else {
            packer.packValue(ValueFactory.newString(value.toString()));
          }
        }
      }
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

  private VectorTileEncoder.Feature decodeVectorTileFeature(FeatureSort.Entry entry) {
    try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(entry.value())) {
      long group;
      if (extractHasGroupFromSortKey(entry.sortKey())) {
        group = unpacker.unpackLong(); // group
        unpacker.unpackInt(); // groupLimit
      } else {
        group = VectorTileEncoder.Feature.NO_GROUP;
      }
      long id = unpacker.unpackLong();
      byte geomType = unpacker.unpackByte();
      int mapSize = unpacker.unpackMapHeader();
      Map<String, Object> attrs = new HashMap<>(mapSize);
      for (int i = 0; i < mapSize; i++) {
        String key = commonStrings.decode(unpacker.unpackByte());
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
      return new VectorTileEncoder.Feature(
        commonStrings.decode(extractLayerIdFromSortKey(entry.sortKey())),
        id,
        new VectorTileEncoder.VectorGeometry(commands, GeometryType.valueOf(geomType)),
        attrs,
        group
      );
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void accept(FeatureSort.Entry entry) {
    sorter.add(entry);
  }

  @Override
  public Iterator<TileFeatures> iterator() {
    Iterator<FeatureSort.Entry> entries = sorter.iterator();
    if (!entries.hasNext()) {
      return Collections.emptyIterator();
    }
    FeatureSort.Entry firstFeature = entries.next();
    return new Iterator<>() {
      private FeatureSort.Entry lastFeature = firstFeature;
      private int lastTileId = extractTileFromSortKey(firstFeature.sortKey());

      @Override
      public boolean hasNext() {
        return lastFeature != null;
      }

      @Override
      public TileFeatures next() {
        TileFeatures result = new TileFeatures(lastTileId);
        result.accept(lastFeature);
        int lastTile = lastTileId;

        while (entries.hasNext()) {
          FeatureSort.Entry next = entries.next();
          lastFeature = next;
          lastTileId = extractTileFromSortKey(lastFeature.sortKey());
          if (lastTile != lastTileId) {
            return result;
          }
          result.accept(next);
        }
        lastFeature = null;
        return result;
      }
    };
  }

  public long getStorageSize() {
    return sorter.getStorageSize();
  }

  public FeatureSort sorter() {
    return sorter;
  }

  public Profile profile() {
    return profile;
  }

  public CommonStringEncoder commonStrings() {
    return commonStrings;
  }

  public Stats stats() {
    return stats;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null || obj.getClass() != this.getClass()) {
      return false;
    }
    var that = (FeatureGroup) obj;
    return Objects.equals(this.sorter, that.sorter) &&
      Objects.equals(this.profile, that.profile) &&
      Objects.equals(this.commonStrings, that.commonStrings) &&
      Objects.equals(this.stats, that.stats);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sorter, profile, commonStrings, stats);
  }

  @Override
  public String toString() {
    return "FeatureGroup[" +
      "sorter=" + sorter + ", " +
      "profile=" + profile + ", " +
      "commonStrings=" + commonStrings + ", " +
      "stats=" + stats + ']';
  }


  public class TileFeatures implements Consumer<FeatureSort.Entry> {

    private final TileCoord tile;
    private final List<FeatureSort.Entry> entries = new ArrayList<>();

    private LongLongHashMap counts = null;
    private byte layer = Byte.MAX_VALUE;

    public TileFeatures(int tile) {
      this.tile = TileCoord.decode(tile);
    }

    public long getNumFeatures() {
      return entries.size();
    }

    public TileCoord coord() {
      return tile;
    }

    public boolean hasSameContents(TileFeatures other) {
      if (other == null || other.entries.size() != entries.size()) {
        return false;
      }
      for (int i = 0; i < entries.size(); i++) {
        FeatureSort.Entry a = entries.get(i);
        FeatureSort.Entry b = other.entries.get(i);
        long layerA = extractLayerIdFromSortKey(a.sortKey());
        long layerB = extractLayerIdFromSortKey(b.sortKey());
        if (layerA != layerB || !Arrays.equals(a.value(), b.value())) {
          return false;
        }
      }
      return true;
    }

    public VectorTileEncoder getTile() {
      VectorTileEncoder encoder = new VectorTileEncoder();
      List<VectorTileEncoder.Feature> items = new ArrayList<>(entries.size());
      String currentLayer = null;
      for (int index = entries.size() - 1; index >= 0; index--) {
        FeatureSort.Entry entry = entries.get(index);

        var feature = decodeVectorTileFeature(entry);
        String layer = feature.layer();

        if (currentLayer == null) {
          currentLayer = layer;
        } else if (!currentLayer.equals(layer)) {
          encoder.addLayerFeatures(
            currentLayer,
            profile.postProcessLayerFeatures(currentLayer, tile.z(), items)
          );
          currentLayer = layer;
          items.clear();
        }

        items.add(feature);
      }
      encoder.addLayerFeatures(
        currentLayer,
        profile.postProcessLayerFeatures(currentLayer, tile.z(), items)
      );
      return encoder;
    }

    @Override
    public void accept(FeatureSort.Entry entry) {
      long sortKey = entry.sortKey();
      if (extractHasGroupFromSortKey(sortKey)) {
        byte thisLayer = extractLayerIdFromSortKey(sortKey);
        if (counts == null) {
          counts = new GHLongLongHashMap();
          layer = thisLayer;
        } else if (thisLayer != layer) {
          layer = thisLayer;
          counts.clear();
        }
        var groupInfo = decodeGroupInfo(entry.value());
        long old = counts.getOrDefault(groupInfo.group(), 0);
        if (groupInfo.limit() > 0 && old >= groupInfo.limit()) {
          return;
        }
        counts.put(groupInfo.group(), old + 1);
      }
      entries.add(entry);
    }

    @Override
    public String toString() {
      return "TileFeatures{" +
        "tile=" + tile +
        ", num entries=" + entries.size() +
        '}';
    }
  }
}
