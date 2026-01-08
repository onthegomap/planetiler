package com.onthegomap.planetiler.reader;

import com.onthegomap.planetiler.geo.WithGeometry;
import com.onthegomap.planetiler.reader.osm.OsmReader;
import com.onthegomap.planetiler.reader.osm.OsmRelationInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Base class for input features read from a data source.
 * <p>
 * Provides cached convenience methods with lazy initialization for geometric attributes derived from
 * {@link #latLonGeometry()} and {@link #worldGeometry()} to avoid computing them if not needed, and recomputing them if
 * needed by multiple features.
 * <p>
 * All geometries except for {@link #latLonGeometry()} return elements in world web mercator coordinates where (0,0) is
 * the northwest corner and (1,1) is the southeast corner of the planet.
 */
public abstract class SourceFeature extends WithGeometry
  implements WithTags, WithSource, WithSourceLayer {

  private final Map<String, Object> tags;
  private final String source;
  private final String sourceLayer;
  protected final List<OsmReader.RelationMember<OsmRelationInfo>> relationInfos;
  private final long id;

  /**
   * Constructs a new input feature.
   *
   * @param tags          string key/value pairs associated with this element
   * @param source        source name that profile can use to distinguish between elements from different data sources
   * @param sourceLayer   layer name within {@code source} that profile can use to distinguish between different kinds
   *                      of elements in a given source.
   * @param relationInfos relations that this element is contained within
   * @param id            numeric ID of this feature within this source (i.e. an OSM element ID)
   */
  protected SourceFeature(Map<String, Object> tags, String source, String sourceLayer,
    List<OsmReader.RelationMember<OsmRelationInfo>> relationInfos, long id) {
    this.tags = tags;
    this.source = source;
    this.sourceLayer = sourceLayer;
    this.relationInfos = relationInfos;
    this.id = id;
  }


  @Override
  public Map<String, Object> tags() {
    return tags;
  }

  /** Returns the ID of the source that this feature came from. */
  @Override
  public String getSource() {
    return source;
  }

  /** Returns the layer ID within a source that this feature comes from. */
  @Override
  public String getSourceLayer() {
    return sourceLayer;
  }

  /**
   * Returns a list of OSM relations that this element belongs to.
   * <p>
   * Use {@link #relationInfo(Class, boolean)} to include super-relations.
   *
   * @param relationInfoClass class of the processed relation data
   * @param <T>               type of {@code relationInfoClass}
   * @return A list containing the parent OSM relation info along with the role that this element is tagged with in that
   *         relation
   */
  public <T extends OsmRelationInfo> List<OsmReader.RelationMember<T>> relationInfo(
    Class<T> relationInfoClass) {
    return relationInfo(relationInfoClass, false);
  }

  /**
   * Returns a list of OSM relations that this element belongs to.
   *
   * @param relationInfoClass     class of the processed relation data
   * @param includeSuperRelations {@code true} to include super-relations {@code false} to only include direct parents
   *                              of this element
   * @param <T>                   type of {@code relationInfoClass}
   * @return A list containing the ancestor OSM relation info along with the role that this element is tagged with in
   *         that relation
   */
  // TODO this should be in a specialized OSM subclass, not the generic superclass
  public <T extends OsmRelationInfo> List<OsmReader.RelationMember<T>> relationInfo(
    Class<T> relationInfoClass, boolean includeSuperRelations) {
    List<OsmReader.RelationMember<T>> result = null;
    if (relationInfos != null) {
      for (OsmReader.RelationMember<?> info : relationInfos) {
        if (relationInfoClass.isInstance(info.relation()) &&
          (includeSuperRelations || !info.isSuperRelation())) {
          if (result == null) {
            result = new ArrayList<>();
          }
          @SuppressWarnings("unchecked") OsmReader.RelationMember<T> casted = (OsmReader.RelationMember<T>) info;
          result.add(casted);
        }
      }
    }
    return result == null ? List.of() : result;
  }

  /** Returns the ID for this element from the input data source (i.e. OSM element ID). */
  public final long id() {
    return id;
  }

  /** By default, the feature id is taken as-is from the input data source id. */
  public long vectorTileFeatureId(int multiplier) {
    return multiplier * id;
  }

  /** Returns true if this element has any OSM relation info. */
  public boolean hasRelationInfo() {
    return relationInfos != null && !relationInfos.isEmpty();
  }

  @Override
  public String toString() {
    return "Feature[source=" + getSource() +
      ", source layer=" + getSourceLayer() +
      ", id=" + id() +
      ", tags=" + tags + ']';
  }

}
