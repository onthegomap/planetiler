// This software is released into the Public Domain.
// See NOTICE.md here or copying.txt from https://github.com/openstreetmap/osmosis/blob/master/package/copying.txt for details.
package com.onthegomap.planetiler.reader.osm;

import com.carrotsearch.hppc.LongArrayList;
import com.google.common.collect.Iterators;
import com.onthegomap.planetiler.reader.FileFormatException;
import crosby.binary.Fileformat;
import crosby.binary.Osmformat;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.IntUnaryOperator;
import java.util.stream.StreamSupport;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;
import org.locationtech.jts.geom.Envelope;
import us.hebi.quickbuf.RepeatedInt;
import us.hebi.quickbuf.RepeatedMessage;

/**
 * Converts PBF block data into decoded entities. This class was adapted from Osmosis to expose an iterator over blocks
 * to give more control over the parallelism.
 *
 * @author Brett Henderson
 */
public class PbfDecoder implements Iterable<OsmElement> {

  private final Osmformat.PrimitiveBlock block;
  private final PbfFieldDecoder fieldDecoder;

  private PbfDecoder(byte[] rawBlob) throws IOException {
    byte[] data = readBlobContent(rawBlob);
    block = Osmformat.PrimitiveBlock.parseFrom(data);
    fieldDecoder = new PbfFieldDecoder(block);
  }

  private PbfDecoder(ByteBuffer rawBlob) throws IOException {
    byte[] data = readBlobContent(rawBlob);
    block = Osmformat.PrimitiveBlock.parseFrom(data);
    fieldDecoder = new PbfFieldDecoder(block);
  }

  private static byte[] readBlobContent(ByteBuffer input) throws IOException {
    return readBlobContent(Fileformat.Blob.parseFrom(input));

  }

  private static byte[] readBlobContent(byte[] input) throws IOException {
    return readBlobContent(Fileformat.Blob.parseFrom(input));
  }

  private static byte[] readBlobContent(Fileformat.Blob blob) {
    byte[] blobData;

    if (blob.hasRaw()) {
      blobData = blob.getRaw().toByteArray();
    } else if (blob.hasZlibData()) {
      Inflater inflater = new Inflater();
      inflater.setInput(blob.getZlibData().toByteArray());
      blobData = new byte[blob.getRawSize()];
      try {
        inflater.inflate(blobData);
      } catch (DataFormatException e) {
        throw new FileFormatException("Unable to decompress PBF blob.", e);
      }
      if (!inflater.finished()) {
        throw new FileFormatException("PBF blob contains incomplete compressed data.");
      }
      inflater.end();
    } else {
      throw new FileFormatException("PBF blob uses unsupported compression, only raw or zlib may be used.");
    }

    return blobData;
  }

  /** Decompresses and parses a block of primitive OSM elements. */
  public static Iterable<OsmElement> decode(byte[] raw) {
    try {
      return new PbfDecoder(raw);
      //      return StreamSupport.stream(new PbfDecoder(raw).spliterator(), false).toList();
    } catch (IOException e) {
      throw new UncheckedIOException("Unable to process PBF blob", e);
    }
  }

  /** Decompresses and parses a block of primitive OSM elements. */
  public static Iterable<OsmElement> decode(ByteBuffer raw) {
    try {
      return new PbfDecoder(raw);
      //      return StreamSupport.stream(new PbfDecoder(raw).spliterator(), false).toList();
    } catch (IOException e) {
      throw new UncheckedIOException("Unable to process PBF blob", e);
    }
  }

  /** Decompresses and parses a header block of an OSM input file. */
  public static OsmHeader decodeHeader(byte[] raw) {
    try {
      byte[] data = readBlobContent(raw);
      Osmformat.HeaderBlock header = Osmformat.HeaderBlock.parseFrom(data);
      Osmformat.HeaderBBox bbox = header.getBbox();
      Envelope bounds = new Envelope(
        bbox.getLeft() / 1e9,
        bbox.getRight() / 1e9,
        bbox.getBottom() / 1e9,
        bbox.getTop() / 1e9
      );
      return new OsmHeader(
        bounds,
        StreamSupport.stream(header.getRequiredFeatures().spliterator(), false).toList(),
        StreamSupport.stream(header.getOptionalFeatures().spliterator(), false).toList(),
        header.getWritingprogram(),
        header.getSource(),
        Instant.ofEpochSecond(header.getOsmosisReplicationTimestamp()),
        header.getOsmosisReplicationSequenceNumber(),
        header.getOsmosisReplicationBaseUrl()
      );
    } catch (IOException e) {
      throw new UncheckedIOException("Unable to decode PBF header", e);
    }
  }

  @Override
  public Iterator<OsmElement> iterator() {
    return Iterators.concat(new GroupIter());
  }

  private class GroupIter implements Iterator<Iterator<OsmElement>> {

    private final Iterator<Osmformat.PrimitiveGroup> iter;

    GroupIter() {
      this.iter = block.getPrimitivegroup().iterator();
    }

    @Override
    public boolean hasNext() {
      return iter.hasNext();
    }

    @Override
    public Iterator<OsmElement> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      var primitiveGroup = iter.next();
      return Iterators.concat(
        new DenseNodeIterator(primitiveGroup.getDense()),
        new NodeIterator(primitiveGroup.getNodes()),
        new WayIterator(primitiveGroup.getWays()),
        new RelationIterator(primitiveGroup.getRelations())
      );
    }
  }

  private Map<String, Object> buildTags(int num, IntUnaryOperator key, IntUnaryOperator value) {
    if (num > 0) {
      Map<String, Object> tags = new HashMap<>(num);
      for (int i = 0; i < num; i++) {
        String k = fieldDecoder.decodeString(key.applyAsInt(i));
        String v = fieldDecoder.decodeString(value.applyAsInt(i));
        tags.put(k, v);
      }
      return tags;
    }
    return Collections.emptyMap();
  }

  private class NodeIterator implements Iterator<OsmElement.Node> {

    private final Iterator<Osmformat.Node> nodes;

    public NodeIterator(RepeatedMessage<Osmformat.Node> nodes) {
      this.nodes = nodes.iterator();
    }

    @Override
    public boolean hasNext() {
      return nodes.hasNext();
    }

    @Override
    public OsmElement.Node next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      var node = nodes.next();
      return new OsmElement.Node(
        node.getId(),
        buildTags(node.getKeys(), node.getVals()),
        fieldDecoder.decodeLatitude(node.getLat()),
        fieldDecoder.decodeLongitude(node.getLon()),
        parseInfo(node.getInfo())
      );
    }
  }

  private Map<String, Object> buildTags(RepeatedInt keys, RepeatedInt vals) {
    int num = keys.length();
    Map<String, Object> result = new HashMap<>(num);

    for (int i = 0; i < num; i++) {
      result.put(fieldDecoder.decodeString(keys.get(i)), fieldDecoder.decodeString(vals.get(i)));
    }
    return result;
  }

  private class RelationIterator implements Iterator<OsmElement.Relation> {

    private final Iterator<Osmformat.Relation> relations;

    public RelationIterator(RepeatedMessage<Osmformat.Relation> relations) {
      this.relations = relations.iterator();
    }

    @Override
    public boolean hasNext() {
      return relations.hasNext();
    }

    @Override
    public OsmElement.Relation next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      var relation = relations.next();
      int num = relation.getMemids().length();

      List<OsmElement.Relation.Member> members = new ArrayList<>(num);

      long memberId = 0;
      for (int j = 0; j < num; j++) {
        memberId += relation.getMemids().get(j);
        var memberType = switch (relation.getTypes().get(j)) {
          case WAY -> OsmElement.Type.WAY;
          case NODE -> OsmElement.Type.NODE;
          case RELATION -> OsmElement.Type.RELATION;
        };
        members.add(new OsmElement.Relation.Member(
          memberType,
          memberId,
          fieldDecoder.decodeString(relation.getRolesSid().get(j))
        ));
      }

      // Add the bound object to the results.
      return new OsmElement.Relation(
        relation.getId(),
        buildTags(relation.getKeys(), relation.getVals()),
        members,
        parseInfo(relation.getInfo())
      );
    }
  }

  private class WayIterator implements Iterator<OsmElement.Way> {

    private final Iterator<Osmformat.Way> ways;

    public WayIterator(RepeatedMessage<Osmformat.Way> ways) {
      this.ways = ways.iterator();
    }

    @Override
    public boolean hasNext() {
      return ways.hasNext();
    }

    @Override
    public OsmElement.Way next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      var way = ways.next();
      // Build up the list of way nodes for the way. The node ids are
      // delta encoded meaning that each id is stored as a delta against
      // the previous one.
      long nodeId = 0;
      int numNodes = way.getRefs().length();
      LongArrayList wayNodesList = new LongArrayList(numNodes);
      wayNodesList.elementsCount = numNodes;
      long[] wayNodes = wayNodesList.buffer;
      for (int j = 0; j < numNodes; j++) {
        long nodeIdOffset = way.getRefs().get(j);
        nodeId += nodeIdOffset;
        wayNodes[j] = nodeId;
      }

      return new OsmElement.Way(
        way.getId(),
        buildTags(way.getKeys(), way.getVals()),
        wayNodesList,
        parseInfo(way.getInfo())
      );
    }
  }

  private OsmElement.Info parseInfo(Osmformat.Info info) {
    return info == null ? null : new OsmElement.Info(
      info.getChangeset(),
      info.getTimestamp(),
      info.getUid(),
      info.getVersion(),
      fieldDecoder.decodeString(info.getUserSid())
    );
  }

  private class DenseNodeIterator implements Iterator<OsmElement.Node> {

    final Osmformat.DenseNodes nodes;
    final Osmformat.DenseInfo denseInfo;
    private final RepeatedInt kv;
    long nodeId = 0;
    long latitude = 0;
    long longitude = 0;
    int i = 0;
    int kvIndex = 0;
    // info
    long timestamp = 0;
    long changeset = 0;
    int uid = 0;
    int userSid = 0;

    public DenseNodeIterator(Osmformat.DenseNodes nodes) {
      this.nodes = nodes;
      this.kv = nodes.getKeysVals();
      this.denseInfo = nodes.getDenseinfo();
    }


    @Override
    public boolean hasNext() {
      return i < nodes.getId().length();
    }

    @Override
    public OsmElement.Node next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      // Delta decode node fields.
      nodeId += nodes.getId().get(i);
      latitude += nodes.getLat().get(i);
      longitude += nodes.getLon().get(i);
      int version = 0;

      if (denseInfo != null) {
        version = denseInfo.getVersion().length() > i ? denseInfo.getVersion().get(i) : 0;
        timestamp += denseInfo.getTimestamp().length() > i ? denseInfo.getTimestamp().get(i) : 0;
        changeset += denseInfo.getChangeset().length() > i ? denseInfo.getChangeset().get(i) : 0;
        uid += denseInfo.getUid().length() > i ? denseInfo.getUid().get(i) : 0;
        userSid += denseInfo.getUserSid().length() > i ? denseInfo.getUserSid().get(i) : 0;
      }

      i++;

      // Build the tags. The key and value string indexes are sequential
      // in the same PBF array. Each set of tags is delimited by an index
      // with a value of 0.
      Map<String, Object> tags = null;
      while (kvIndex < kv.length()) {
        int keyIndex = kv.get(kvIndex++);
        if (keyIndex == 0) {
          break;
        }
        int valueIndex = kv.get(kvIndex++);

        if (tags == null) {
          // divide by 2 as key&value, multiply by 2 because of the better approximation
          tags = new HashMap<>(Math.max(3, 2 * (kv.length() / 2) / kv.length()));
        }

        tags.put(fieldDecoder.decodeString(keyIndex), fieldDecoder.decodeString(valueIndex));
      }

      return new OsmElement.Node(
        nodeId,
        tags == null ? Collections.emptyMap() : tags,
        ((double) latitude) / 10000000,
        ((double) longitude) / 10000000,
        denseInfo == null ? null : new OsmElement.Info(
          changeset,
          timestamp,
          uid,
          version,
          fieldDecoder.decodeString(userSid)
        )
      );
    }
  }
}
