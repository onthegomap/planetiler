package com.onthegomap.flatmap.collections;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongLongHashMap;
import com.graphhopper.coll.GHLongLongHashMap;
import com.onthegomap.flatmap.LayerFeature;
import com.onthegomap.flatmap.Profile;
import com.onthegomap.flatmap.VectorTileEncoder;
import com.onthegomap.flatmap.VectorTileEncoder.VectorTileFeature;
import com.onthegomap.flatmap.collections.MergeSortFeatureMap.TileFeatures;
import com.onthegomap.flatmap.geo.TileCoord;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import org.jetbrains.annotations.NotNull;
import org.locationtech.jts.geom.Geometry;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public record MergeSortFeatureMap(MergeSort mergeSort, Profile profile, CommonStringEncoder commonStrings)
  implements Consumer<MergeSort.Entry>, Iterable<TileFeatures> {

  private static final Logger LOGGER = LoggerFactory.getLogger(MergeSortFeatureMap.class);

  public MergeSortFeatureMap(MergeSort mergeSort, Profile profile) {
    this(mergeSort, profile, new CommonStringEncoder());
  }

  @Override
  public void accept(MergeSort.Entry entry) {
    mergeSort.add(entry);
  }

  @Override
  public Iterator<TileFeatures> iterator() {
    Iterator<MergeSort.Entry> entries = mergeSort.iterator();
    if (!entries.hasNext()) {
      return Collections.emptyIterator();
    }
    MergeSort.Entry firstFeature = entries.next();
    byte[] firstData = firstFeature.value();
    long firstSort = firstFeature.sortKey();
    return new Iterator<>() {
      private byte[] last = firstData;
      private long lastSortKey = firstSort;
      private int lastTileId = FeatureMapKey.extractTileFromKey(firstSort);

      @Override
      public boolean hasNext() {
        return last != null;
      }

      @Override
      public TileFeatures next() {
        TileFeatures result = new TileFeatures(lastTileId);
        result.add(lastSortKey, last);
        int lastTile = lastTileId;
        while (entries.hasNext()) {
          MergeSort.Entry next = entries.next();
          last = next.value();
          lastSortKey = next.sortKey();
          lastTileId = FeatureMapKey.extractTileFromKey(lastSortKey);
          if (lastTile != lastTileId) {
            return result;
          }
          result.add(next.sortKey(), last);
        }
        return result;
      }
    };
  }

  public long getStorageSize() {
    return mergeSort.getStorageSize();
  }

  public class TileFeatures implements Consumer<MergeSort.Entry> {

    private final TileCoord tile;
    private final LongArrayList sortKeys = new LongArrayList();
    private final List<byte[]> entries = new ArrayList<>();

    private LongLongHashMap counts = null;
    private byte layer = Byte.MAX_VALUE;

    public TileFeatures(int tile) {
      this.tile = TileCoord.decode(tile);
    }

    public long getNumFeatures() {
      return 0;
    }

    public TileCoord coord() {
      return tile;
    }

    public boolean hasSameContents(TileFeatures other) {
      if (other == null || other.entries.size() != entries.size()) {
        return false;
      }
      for (int i = 0; i < entries.size(); i++) {
        byte[] a = entries.get(i);
        byte[] b = other.entries.get(i);
        if (!Arrays.equals(a, b)) {
          return false;
        }
      }
      return true;
    }

    public VectorTileEncoder getTile() {
      VectorTileEncoder encoder = new VectorTileEncoder();
      List<VectorTileFeature> items = new ArrayList<>(entries.size());
      String currentLayer = null;
      for (int index = entries.size() - 1; index >= 0; index--) {
        byte[] entry = entries.get(index);
        long sortKey = sortKeys.get(index);

        FeatureMapKey key = FeatureMapKey.decode(sortKey);
        FeatureMapValue value = FeatureMapValue.decode(entry, key.hasGroup(), commonStrings);
        String layer = commonStrings.decode(key.layer);

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

        items.add(LayerFeature.of(key, value));
      }
      encoder.addLayerFeatures(
        currentLayer,
        profile.postProcessLayerFeatures(currentLayer, tile.z(), items)
      );
      return encoder;
    }

    public TileFeatures add(long sortKey, byte[] entry) {
      if (FeatureMapKey.extractHasGroupFromKey(sortKey)) {
        byte thisLayer = FeatureMapKey.extractLayerIdFromKey(sortKey);
        if (counts == null) {
          counts = new GHLongLongHashMap();
          layer = thisLayer;
        } else if (thisLayer != layer) {
          layer = thisLayer;
          counts.clear();
        }
        var groupInfo = FeatureMapValue.decodeGroupInfo(entry);
        long old = counts.getOrDefault(groupInfo.group, 0);
        if (old >= groupInfo.limit && groupInfo.limit > 0) {
          return this;
        }
        counts.put(groupInfo.group, old + 1);
      }
      sortKeys.add(sortKey);
      entries.add(entry);
      return this;
    }

    @Override
    public void accept(MergeSort.Entry renderedFeature) {
      add(renderedFeature.sortKey(), renderedFeature.value());
    }

    @Override
    public String toString() {
      return "TileFeatures{" +
        "tile=" + tile +
        ", sortKeys=" + sortKeys +
        ", entries=" + entries +
        '}';
    }
  }

  private static final ThreadLocal<MessageBufferPacker> messagePackers = ThreadLocal
    .withInitial(MessagePack::newDefaultBufferPacker);

  public record FeatureMapKey(long encoded, TileCoord tile, byte layer, int zOrder, boolean hasGroup) implements
    Comparable<FeatureMapKey> {

    private static final int Z_ORDER_MASK = (1 << 23) - 1;
    public static final int Z_ORDER_MAX = (1 << 22) - 1;
    public static final int Z_ORDER_MIN = -(1 << 22);
    public static final int Z_ORDER_BITS = 23;

    public static FeatureMapKey of(int tile, byte layer, int zOrder, boolean hasGroup) {
      return new FeatureMapKey(encode(tile, layer, zOrder, hasGroup), TileCoord.decode(tile), layer, zOrder, hasGroup);
    }

    public static FeatureMapKey decode(long encoded) {
      return of(
        extractTileFromKey(encoded),
        extractLayerIdFromKey(encoded),
        extractZorderFromKey(encoded),
        extractHasGroupFromKey(encoded)
      );
    }

    public static long encode(int tile, byte layer, int zOrder, boolean hasGroup) {
      return ((long) tile << 32L) | ((long) (layer & 0xff) << 24L) | (((zOrder - Z_ORDER_MIN) & Z_ORDER_MASK) << 1L) | (
        hasGroup ? 1 : 0);
    }

    public static boolean extractHasGroupFromKey(long sortKey) {
      return (sortKey & 1) == 1;
    }

    public static int extractTileFromKey(long sortKey) {
      return (int) (sortKey >> 32L);
    }

    public static byte extractLayerIdFromKey(long sortKey) {
      return (byte) (sortKey >> 24);
    }

    public static int extractZorderFromKey(long sortKey) {
      return ((int) ((sortKey >> 1) & Z_ORDER_MASK) + Z_ORDER_MIN);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      FeatureMapKey that = (FeatureMapKey) o;

      return encoded == that.encoded;
    }

    @Override
    public int hashCode() {
      return (int) (encoded ^ (encoded >>> 32));
    }

    @Override
    public int compareTo(@NotNull FeatureMapKey o) {
      return Long.compare(encoded, o.encoded);
    }
  }

  public static record FeatureMapValue(
    long featureId,
    Map<String, Object> attrs,
    int[] commands,
    byte geomType,
    boolean hasGrouping,
    int groupLimit,
    long group
  ) {

    public static record GroupInfo(long group, int limit) {

    }

    public static GroupInfo decodeGroupInfo(byte[] encoded) {
      try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(encoded)) {
        long group = unpacker.unpackLong();
        int limit = unpacker.unpackInt();
        return new GroupInfo(group, limit);
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }

    public static FeatureMapValue from(
      long featureId,
      Map<String, Object> attrs,
      Geometry geom,
      boolean hasGrouping,
      int groupLimit
    ) {
      long group = geom.getUserData() instanceof Long longValue ? longValue : 0;
      byte geomType = (byte) VectorTileEncoder.toGeomType(geom).getNumber();
      int[] commands = VectorTileEncoder.getCommands(geom);
      return new FeatureMapValue(
        featureId,
        attrs,
        commands,
        geomType,
        hasGrouping,
        groupLimit,
        group
      );
    }

    public static FeatureMapValue decode(byte[] encoded, boolean hasGroup, CommonStringEncoder commonStrings) {
      try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(encoded)) {
        long group = 0;
        int groupLimit = -1;
        if (hasGroup) {
          group = unpacker.unpackLong();
          groupLimit = unpacker.unpackInt();
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
        return new FeatureMapValue(id, attrs, commands, geomType, hasGroup, groupLimit, group);
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }

    public byte[] encode(CommonStringEncoder commonStrings) {
      MessageBufferPacker packer = messagePackers.get();
      packer.clear();
      try {
        if (hasGrouping) {
          packer.packLong(group);
          packer.packInt(groupLimit);
        }
        packer.packLong(featureId);
        packer.packByte(geomType);
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
  }
}
