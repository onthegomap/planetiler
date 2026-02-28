package com.onthegomap.planetiler.reader.osm;

import com.carrotsearch.hppc.LongArrayList;
import java.util.Map;

/**
 * Provides access to stored member data for relation_members processing.
 * Used to retrieve way geometries, way tags, and node tags for relation members.
 */
public interface RelationMemberDataProvider {
  
  /**
   * Gets the node IDs for a way member.
   * @param wayId the way ID
   * @return the node IDs, or null if not found
   */
  LongArrayList getWayGeometry(long wayId);
  
  /**
   * Gets the tags for a way member.
   * @param wayId the way ID
   * @return the tags, or null if not found
   */
  Map<String, Object> getWayTags(long wayId);
  
  /**
   * Gets the tags for a node member.
   * @param nodeId the node ID
   * @return the tags, or null if not found
   */
  Map<String, Object> getNodeTags(long nodeId);
  
  /**
   * Gets the coordinate for a node.
   * @param nodeId the node ID
   * @return the coordinate, or null if not found
   */
  org.locationtech.jts.geom.Coordinate getNodeCoordinate(long nodeId);
}

