package com.onthegomap.flatmap.reader.osm;

import com.carrotsearch.hppc.LongArrayList;
import com.graphhopper.reader.ReaderElement;
import com.graphhopper.reader.ReaderElementUtils;
import com.graphhopper.reader.ReaderNode;
import com.graphhopper.reader.ReaderRelation;
import com.graphhopper.reader.ReaderWay;
import com.onthegomap.flatmap.reader.WithTags;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface OsmElement extends WithTags {

  long id();

  enum Type {
    NODE, WAY, RELATION
  }

  record Other(
    @Override long id,
    @Override Map<String, Object> tags
  ) implements OsmElement {}

  record Node(
    @Override long id,
    @Override Map<String, Object> tags,
    double lat,
    double lon
  ) implements OsmElement {

    public Node(long id, double lat, double lon) {
      this(id, new HashMap<>(), lat, lon);
    }
  }

  record Way(
    @Override long id,
    @Override Map<String, Object> tags,
    LongArrayList nodes
  ) implements OsmElement {

    public Way(long id) {
      this(id, new HashMap<>(), new LongArrayList(5));
    }
  }

  record Relation(
    @Override long id,
    @Override Map<String, Object> tags,
    List<Member> members
  ) implements OsmElement {

    public Relation(long id) {
      this(id, new HashMap<>(), new ArrayList<>());
    }

    public Relation {
      if (members == null) {
        members = Collections.emptyList();
      }
    }

    public static record Member(
      Type type,
      long ref,
      String role
    ) {}
  }

  static OsmElement fromGraphhopper(ReaderElement element) {
    if (element instanceof ReaderNode node) {
      return fromGraphopper(node);
    } else if (element instanceof ReaderWay way) {
      return fromGraphopper(way);
    } else if (element instanceof ReaderRelation relation) {
      return fromGraphopper(relation);
    } else {
      long id = element.getId();
      Map<String, Object> tags = ReaderElementUtils.getTags(element);
      return new Other(id, tags);
    }
  }

  static Node fromGraphopper(ReaderNode node) {
    long id = node.getId();
    Map<String, Object> tags = ReaderElementUtils.getTags(node);
    return new Node(id, tags, node.getLat(), node.getLon());
  }

  static Way fromGraphopper(ReaderWay way) {
    long id = way.getId();
    Map<String, Object> tags = ReaderElementUtils.getTags(way);
    return new Way(id, tags, way.getNodes());
  }

  static Relation fromGraphopper(ReaderRelation relation) {
    long id = relation.getId();
    Map<String, Object> tags = ReaderElementUtils.getTags(relation);
    List<ReaderRelation.Member> readerMembers = relation.getMembers();
    List<Relation.Member> members = new ArrayList<>(readerMembers.size());
    for (var member : readerMembers) {
      Type type = switch (member.getType()) {
        case ReaderRelation.Member.NODE -> Type.NODE;
        case ReaderRelation.Member.WAY -> Type.WAY;
        case ReaderRelation.Member.RELATION -> Type.RELATION;
        default -> throw new IllegalArgumentException("Unrecognized type: " + member.getType());
      };
      members.add(new Relation.Member(type, member.getRef(), member.getRole()));
    }
    return new Relation(id, tags, members);
  }
}
