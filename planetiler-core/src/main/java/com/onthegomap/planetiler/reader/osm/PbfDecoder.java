// This software is released into the Public Domain.
// See NOTICE.md here or copying.txt from https://github.com/openstreetmap/osmosis/blob/master/package/copying.txt for details.
package com.onthegomap.planetiler.reader.osm;

import com.carrotsearch.hppc.LongArrayList;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;
import org.locationtech.jts.geom.Envelope;
import org.openstreetmap.osmosis.osmbinary.Fileformat;
import org.openstreetmap.osmosis.osmbinary.Osmformat;

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

  @Override
  public Iterator<OsmElement> iterator() {
    // TODO change to Iterables.concat(processNodes, processWays, processRelations)
    return block.getPrimitivegroupList().stream()
      .<OsmElement>mapMulti((primitiveGroup, next) -> {
        processNodes(primitiveGroup.getDense(), next);
        processNodes(primitiveGroup.getNodesList(), next);
        processWays(primitiveGroup.getWaysList(), next);
        processRelations(primitiveGroup.getRelationsList(), next);
      }).iterator();
  }

  private static byte[] readBlobContent(byte[] input) throws IOException {
    Fileformat.Blob blob = Fileformat.Blob.parseFrom(input);
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
        throw new RuntimeException("Unable to decompress PBF blob.", e);
      }
      if (!inflater.finished()) {
        throw new RuntimeException("PBF blob contains incomplete compressed data.");
      }
      inflater.end();
    } else {
      throw new RuntimeException("PBF blob uses unsupported compression, only raw or zlib may be used.");
    }

    return blobData;
  }

  private Map<String, Object> buildTags(List<Integer> keys, List<Integer> values) {
    Iterator<Integer> keyIterator = keys.iterator();
    Iterator<Integer> valueIterator = values.iterator();
    if (keyIterator.hasNext()) {
      Map<String, Object> tags = new HashMap<>(keys.size());
      while (keyIterator.hasNext()) {
        String key = fieldDecoder.decodeString(keyIterator.next());
        String value = fieldDecoder.decodeString(valueIterator.next());
        tags.put(key, value);
      }
      return tags;
    }
    return Collections.emptyMap();
  }

  private void processNodes(List<Osmformat.Node> nodes, Consumer<OsmElement> receiver) {
    for (Osmformat.Node node : nodes) {
      receiver.accept(new OsmElement.Node(
        node.getId(),
        buildTags(node.getKeysList(), node.getValsList()),
        fieldDecoder.decodeLatitude(node.getLat()),
        fieldDecoder.decodeLongitude(node.getLon())
      ));
    }
  }

  private void processNodes(Osmformat.DenseNodes nodes, Consumer<OsmElement> receiver) {
    List<Long> idList = nodes.getIdList();
    List<Long> latList = nodes.getLatList();
    List<Long> lonList = nodes.getLonList();

    Iterator<Integer> keysValuesIterator = nodes.getKeysValsList().iterator();
    long nodeId = 0;
    long latitude = 0;
    long longitude = 0;
    for (int i = 0; i < idList.size(); i++) {
      // Delta decode node fields.
      nodeId += idList.get(i);
      latitude += latList.get(i);
      longitude += lonList.get(i);

      // Build the tags. The key and value string indexes are sequential
      // in the same PBF array. Each set of tags is delimited by an index
      // with a value of 0.
      Map<String, Object> tags = null;
      while (keysValuesIterator.hasNext()) {
        int keyIndex = keysValuesIterator.next();
        if (keyIndex == 0) {
          break;
        }
        int valueIndex = keysValuesIterator.next();

        if (tags == null) {
          // divide by 2 as key&value, multiple by 2 because of the better approximation
          tags = new HashMap<>(Math.max(3, 2 * (nodes.getKeysValsList().size() / 2) / idList.size()));
        }

        tags.put(fieldDecoder.decodeString(keyIndex), fieldDecoder.decodeString(valueIndex));
      }

      receiver.accept(new OsmElement.Node(
        nodeId,
        tags == null ? Collections.emptyMap() : tags,
        ((double) latitude) / 10000000,
        ((double) longitude) / 10000000)
      );
    }
  }

  private void processWays(List<Osmformat.Way> ways, Consumer<OsmElement> receiver) {
    for (Osmformat.Way way : ways) {
      // Build up the list of way nodes for the way. The node ids are
      // delta encoded meaning that each id is stored as a delta against
      // the previous one.
      long nodeId = 0;
      int numNodes = way.getRefsCount();
      LongArrayList wayNodesList = new LongArrayList(numNodes);
      wayNodesList.elementsCount = numNodes;
      long[] wayNodes = wayNodesList.buffer;
      for (int i = 0; i < numNodes; i++) {
        long nodeIdOffset = way.getRefs(i);
        nodeId += nodeIdOffset;
        wayNodes[i] = nodeId;
      }

      receiver.accept(new OsmElement.Way(
        way.getId(),
        buildTags(way.getKeysList(), way.getValsList()),
        wayNodesList
      ));
    }
  }

  private void processRelations(List<Osmformat.Relation> relations, Consumer<OsmElement> receiver) {
    for (Osmformat.Relation relation : relations) {

      int num = relation.getMemidsCount();

      List<OsmElement.Relation.Member> members = new ArrayList<>(num);

      long memberId = 0;
      for (int i = 0; i < num; i++) {
        memberId += relation.getMemids(i);
        var memberType = switch (relation.getTypes(i)) {
          case WAY -> OsmElement.Relation.Type.WAY;
          case NODE -> OsmElement.Relation.Type.NODE;
          case RELATION -> OsmElement.Relation.Type.RELATION;
        };
        members.add(new OsmElement.Relation.Member(
          memberType,
          memberId,
          fieldDecoder.decodeString(relation.getRolesSid(i))
        ));
      }

      // Add the bound object to the results.
      receiver.accept(new OsmElement.Relation(
        relation.getId(),
        buildTags(relation.getKeysList(), relation.getValsList()),
        members
      ));
    }
  }

  /** Decompresses and parses a block of primitive OSM elements. */
  public static Iterable<OsmElement> decode(byte[] raw) {
    try {
      return new PbfDecoder(raw);
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
        header.getRequiredFeaturesList(),
        header.getOptionalFeaturesList(),
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
}
