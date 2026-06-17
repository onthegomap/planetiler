package com.onthegomap.planetiler.custommap.configschema;

import com.onthegomap.planetiler.reader.osm.OsmRelationInfo;

/**
 * Marker class to indicate that a relation should be processed for member extraction.
 * Used when a feature has {@code geometry: relation_members}.
 */
public record RelationMembersInfo(long id) implements OsmRelationInfo {
}

