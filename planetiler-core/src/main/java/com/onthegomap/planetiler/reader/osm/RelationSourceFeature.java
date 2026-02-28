package com.onthegomap.planetiler.reader.osm;

import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SourceFeature;
import java.util.List;
import org.locationtech.jts.geom.Geometry;

/**
 * A SourceFeature representing an OSM relation that will be processed to extract member geometries.
 * Relations themselves don't have direct geometry, but this provides access to relation members.
 */
public class RelationSourceFeature extends SourceFeature implements OsmSourceFeature {

  private final OsmElement.Relation relation;
  private final RelationMemberDataProvider memberDataProvider;
  private Geometry worldGeometry;
  private Geometry latLonGeometry;

  public RelationSourceFeature(OsmElement.Relation relation,
    List<OsmReader.RelationMember<OsmRelationInfo>> parentRelations,
    RelationMemberDataProvider memberDataProvider) {
    super(relation.tags(), "osm", null, parentRelations, relation.id());
    this.relation = relation;
    this.memberDataProvider = memberDataProvider;
  }
  
  /**
   * Constructor for use when member data provider is not available (e.g., in tests).
   */
  public RelationSourceFeature(OsmElement.Relation relation,
    List<OsmReader.RelationMember<OsmRelationInfo>> parentRelations) {
    this(relation, parentRelations, null);
  }

  @Override
  public Geometry worldGeometry() throws GeometryException {
    // Relations don't have direct geometry - return empty geometry collection
    if (worldGeometry == null) {
      worldGeometry = GeoUtils.JTS_FACTORY.createGeometryCollection();
    }
    return worldGeometry;
  }

  @Override
  public Geometry latLonGeometry() throws GeometryException {
    // Relations don't have direct geometry - return empty geometry collection
    if (latLonGeometry == null) {
      latLonGeometry = GeoUtils.JTS_FACTORY.createGeometryCollection();
    }
    return latLonGeometry;
  }

  @Override
  public boolean isPoint() {
    return false;
  }

  @Override
  public boolean canBeLine() {
    return false;
  }

  @Override
  public boolean canBePolygon() {
    return false;
  }

  @Override
  public OsmElement originalElement() {
    return relation;
  }

  /**
   * Returns the OSM relation this feature represents.
   */
  public OsmElement.Relation relation() {
    return relation;
  }
  
  /**
   * Returns the member data provider, or null if not available.
   */
  public RelationMemberDataProvider memberDataProvider() {
    return memberDataProvider;
  }
}

