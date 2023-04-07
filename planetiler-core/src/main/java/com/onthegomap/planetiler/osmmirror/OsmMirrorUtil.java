package com.onthegomap.planetiler.osmmirror;

import com.carrotsearch.hppc.LongArrayList;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;

public class OsmMirrorUtil {

  public static byte[] encodeTags(Map<String, Object> tags) {
    if (tags.isEmpty()) {
      return null;
    }
    try (var msgPack = MessagePack.newDefaultBufferPacker()) {
      packTags(tags, msgPack);
      return msgPack.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void packTags(Map<String, Object> tags, MessagePacker msgPack) throws IOException {
    msgPack.packMapHeader(tags.size());
    for (var entry : tags.entrySet()) {
      msgPack.packString(entry.getKey());
      if (entry.getValue() == null) {
        msgPack.packNil();
      } else {
        msgPack.packString(entry.getValue().toString());
      }
    }
  }

  public static Map<String, Object> parseTags(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return Map.of();
    }
    try (var msgPack = MessagePack.newDefaultUnpacker(bytes)) {
      return unpackTags(msgPack);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Map<String, Object> unpackTags(MessageUnpacker msgPack) throws IOException {
    if (!msgPack.hasNext()) {
      return Map.of();
    }
    int length = msgPack.unpackMapHeader();
    Map<String, Object> result = new HashMap<>(length);
    for (int i = 0; i < length; i++) {
      result.put(
        msgPack.unpackString(),
        msgPack.unpackString()
      );
    }
    return result;
  }

  public static byte[] encode(OsmElement.Node node, boolean id) {
    try (var messagePack = MessagePack.newDefaultBufferPacker()) {
      pack(messagePack, node, id);
      return messagePack.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static OsmElement.Node decodeNode(byte[] bytes) {
    try (var messagePack = MessagePack.newDefaultUnpacker(bytes)) {
      return unpackNode(messagePack);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static byte[] encode(OsmElement.Way way, boolean id) {
    try (var messagePack = MessagePack.newDefaultBufferPacker()) {
      pack(messagePack, way, id);
      return messagePack.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static OsmElement.Way decodeWay(byte[] bytes) {
    try (var messagePack = MessagePack.newDefaultUnpacker(bytes)) {
      return unpackWay(messagePack);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static byte[] encode(OsmElement.Relation relation, boolean id) {
    try (var messagePack = MessagePack.newDefaultBufferPacker()) {
      pack(messagePack, relation, id);
      return messagePack.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static OsmElement.Relation decodeRelation(byte[] bytes) {
    try (var messagePack = MessagePack.newDefaultUnpacker(bytes)) {
      return unpackRelation(messagePack);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  static void pack(MessagePacker messagePack, OsmElement.Node node, boolean id) throws IOException {
    if (id) {
      messagePack.packLong(node.id());
    }
    messagePack.packInt(node.version());
    messagePack.packLong(node.encodedLocation());
    packTags(node.tags(), messagePack);
  }

  private static OsmElement.Node unpackNode(MessageUnpacker messagePack) throws IOException {
    long id = messagePack.unpackLong();
    return unpackNode(messagePack, id);
  }

  private static OsmElement.Node unpackNode(MessageUnpacker messagePack, long id) throws IOException {
    int version = messagePack.unpackInt();
    long location = messagePack.unpackLong();
    Map<String, Object> tags = unpackTags(messagePack);
    return new OsmElement.Node(id, tags, location, OsmElement.Info.forVersion(version));
  }

  private static int encodeZigZag32(final int n) {
    // Note:  the right-shift must be arithmetic
    return (n << 1) ^ (n >> 31);
  }

  private static long encodeZigZag64(final long n) {
    // Note:  the right-shift must be arithmetic
    return (n << 1) ^ (n >> 63);
  }

  public static int decodeZigZag32(final int n) {
    return (n >>> 1) ^ -(n & 1);
  }

  public static long decodeZigZag64(final long n) {
    return (n >>> 1) ^ -(n & 1);
  }

  static void pack(MessagePacker messagePack, OsmElement.Way way, boolean id) throws IOException {
    if (id) {
      messagePack.packLong(way.id());
    }
    messagePack.packInt(way.version());
    messagePack.packArrayHeader(way.nodes().size());
    LongArrayList nodes = way.nodes();
    long last = 0;
    for (int i = 0; i < nodes.size(); i++) {
      long node = nodes.get(i);
      messagePack.packLong(encodeZigZag64(node - last));
      last = node;
    }
    packTags(way.tags(), messagePack);
  }

  private static OsmElement.Way unpackWay(MessageUnpacker messagePack) throws IOException {
    long id = messagePack.unpackLong();
    return unpackWay(messagePack, id);
  }

  private static OsmElement.Way unpackWay(MessageUnpacker messagePack, long id) throws IOException {
    int version = messagePack.unpackInt();
    int nodeCount = messagePack.unpackArrayHeader();
    long nodeId = 0;
    LongArrayList longs = new LongArrayList(nodeCount);
    for (int i = 0; i < nodeCount; i++) {
      nodeId += decodeZigZag64(messagePack.unpackLong());
      longs.add(nodeId);
    }
    Map<String, Object> tags = unpackTags(messagePack);
    return new OsmElement.Way(id, tags, longs, OsmElement.Info.forVersion(version));
  }

  static void pack(MessagePacker messagePack, OsmElement.Relation relation, boolean id) throws IOException {
    if (id) {
      messagePack.packLong(relation.id());
    }
    messagePack.packInt(relation.version());
    messagePack.packArrayHeader(relation.members().size());
    var members = relation.members();
    long lastRef = 0;
    String lastRole = "";
    for (var member : members) {
      long ref = member.ref();
      messagePack.packLong(encodeZigZag64(ref - lastRef));
      lastRef = ref;

      messagePack.packInt(member.type().ordinal());

      String role = member.role();
      messagePack.packString(Objects.equals(lastRole, role) ? "" : role);
      lastRole = role;
    }
    packTags(relation.tags(), messagePack);
  }

  private static OsmElement.Relation unpackRelation(MessageUnpacker messagePack) throws IOException {
    long id = messagePack.unpackLong();
    return unpackRelation(messagePack, id);
  }

  private static OsmElement.Relation unpackRelation(MessageUnpacker messagePack, long id) throws IOException {
    int version = messagePack.unpackInt();
    int memberCount = messagePack.unpackArrayHeader();
    List<OsmElement.Relation.Member> members = new ArrayList<>(memberCount);

    long ref = 0;
    String lastRole = "";
    for (int i = 0; i < memberCount; i++) {
      ref += decodeZigZag64(messagePack.unpackLong());
      OsmElement.Type type = OsmElement.Type.values()[messagePack.unpackInt()];
      String role = messagePack.unpackString();
      role = role.isBlank() ? lastRole : role;
      members.add(new OsmElement.Relation.Member(
        type,
        ref,
        role
      ));
      lastRole = role;
    }
    var tags = unpackTags(messagePack);
    return new OsmElement.Relation(
      id,
      tags,
      members,
      OsmElement.Info.forVersion(version)
    );
  }

  public static void packElement(MessageBufferPacker packer, OsmElement item, boolean id) throws IOException {
    if (item instanceof OsmElement.Node node) {
      pack(packer, node, id);
    } else if (item instanceof OsmElement.Way way) {
      pack(packer, way, id);
    } else if (item instanceof OsmElement.Relation relation) {
      pack(packer, relation, id);
    }
  }

  public static OsmElement.Way decodeWay(long id, byte[] bytes) {
    try (var messagePack = MessagePack.newDefaultUnpacker(bytes)) {
      return unpackWay(messagePack, id);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static OsmElement.Node decodeNode(long id, byte[] bytes) {
    try (var messagePack = MessagePack.newDefaultUnpacker(bytes)) {
      return unpackNode(messagePack, id);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static OsmElement.Relation decodeRelation(long id, byte[] bytes) {
    try (var messagePack = MessagePack.newDefaultUnpacker(bytes)) {
      return unpackRelation(messagePack, id);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
