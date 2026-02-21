package com.onthegomap.planetiler;

import com.onthegomap.planetiler.expression.Expression;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmRelationInfo;
import com.onthegomap.planetiler.util.Wikidata;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Provides methods for implementations to control how maps are generated.
 * <p>
 * This includes:
 * <ul>
 * <li>How source features (OSM elements, shapefile elements, etc.) map to output features and their tags</li>
 * <li>How vector tile features in an output tile should be post-processed (see {@link FeatureMerge})</li>
 * <li>What attributes to include in the mbtiles metadata output (see {@link Mbtiles})</li>
 * <li>Whether {@link Wikidata} class should fetch wikidata translations for an OSM element</li>
 * </ul>
 * <p>
 * {@link Profile#processFeature(SourceFeature, FeatureCollector)} only handles a single element at a time. To "join"
 * elements across or within sources, implementations can store data in instance fields and wait to process them until
 * an element is encountered in a later source, or the
 * {@link Profile#finish(String, FeatureCollector.Factory, Consumer)} method is called for a source. All methods may be
 * called concurrently by multiple threads, so implementations must be careful to ensure access to instance fields is
 * thread-safe.
 * <p>
 * For complex profiles, {@link ForwardingProfile} provides a framework for splitting the logic up into several handlers
 * (i.e. one per layer) and forwarding each element/event to the handlers that care about it.
 */
public interface Profile extends FeatureProcessor<SourceFeature> {
  // TODO might want to break this apart into sub-interfaces that ForwardingProfile (and TileArchiveMetadata) can use too

  /**
   * Default attribution recommended for profiles using OpenStreetMap data
   *
   * @see <a href="https://www.openstreetmap.org/copyright">www.openstreetmap.org/copyright</a>
   */
  String OSM_ATTRIBUTION = """
    <a href="https://www.openstreetmap.org/copyright" target="_blank">&copy; OpenStreetMap contributors</a>
    """.trim();

  /**
   * Allows profile to extract any information it needs from a {@link OsmElement.Node} during the first pass through OSM
   * elements.
   * <p>
   * The default implementation does nothing.
   *
   * @param node the OSM node
   */
  default void preprocessOsmNode(OsmElement.Node node) {}

  /**
   * Allows profile to extract any information it needs from a {@link OsmElement.Way} during the first pass through OSM
   * elements.
   * <p>
   * The default implementation does nothing.
   *
   * @param way the OSM way
   */
  default void preprocessOsmWay(OsmElement.Way way) {}

  /**
   * To break OSM ways at nodes where they intersect, return true from this method to make it participate in way
   * splitting and also ask for the split way instead of full one using {@link FeatureCollector#splitLine(String)} in
   * {@link #processFeature(SourceFeature, FeatureCollector)}.
   * <p>
   * The default implementation returns false, which means this OSM way will not get split or split other ways if it
   * intersects them at a node.
   *
   * @param way the OSM way
   * @return true for this way to participate in way splitting or false to not participate.
   */
  default boolean splitOsmWayAtIntersections(OsmElement.Way way) {
    return false;
  }

  /**
   * Extracts information from <a href="https://wiki.openstreetmap.org/wiki/Relation">OSM relations</a> that will be
   * passed along to {@link #processFeature(SourceFeature, FeatureCollector)} for any OSM element in that relation.
   * <p>
   * The result of this method is stored in memory.
   * <p>
   * The default implementation returns {@code null} to ignore all relations
   *
   * @param relation the OSM relation
   * @return a list of relation info instances with information extracted from the relation to pass to
   *         {@link #processFeature(SourceFeature, FeatureCollector)}, or {@code null} to ignore.
   */
  default List<OsmRelationInfo> preprocessOsmRelation(OsmElement.Relation relation) {
    return null;
  }

  /** Free any resources associated with this profile (i.e. shared data structures) */
  default void release() {}

  /**
   * Apply any post-processing to features in an output layer of a tile before writing it to the output file
   * <p>
   * These transformations may add, remove, or change the tags, geometry, or ordering of output features based on other
   * features present in this tile. See {@link FeatureMerge} class for a set of common transformations that merge
   * linestrings/polygons.
   * <p>
   * Many threads invoke this method concurrently so ensure thread-safe access to any shared data structures.
   * <p>
   * The default implementation passes through input features unaltered
   *
   * @param layer the output layer name
   * @param zoom  zoom level of the tile
   * @param items all the output features in this layer in this tile
   * @return the new list of output features or {@code null} to not change anything. Set any elements of the list to
   *         {@code null} if they should be ignored.
   * @throws GeometryException for any recoverable geometric operation failures - the framework will log the error, emit
   *                           the original input features, and continue processing other layers
   */
  default List<VectorTile.Feature> postProcessLayerFeatures(String layer, int zoom,
    List<VectorTile.Feature> items) throws GeometryException {
    return items;
  }

  default List<VectorTile.Feature> postProcessLayerFeatures(String layer, TileCoord tileCoord,
    List<VectorTile.Feature> items) throws GeometryException {
    return postProcessLayerFeatures(layer, tileCoord.z(), items);
  }

  /**
   * Apply any post-processing to layers in an output tile before writing it to the output.
   * <p>
   * This is called before {@link #postProcessLayerFeatures(String, int, List)} gets called for each layer. Use this
   * method if features in one layer should influence features in another layer, to create new layers from existing
   * ones, or if you need to remove a layer entirely from the output.
   * <p>
   * These transformations may add, remove, or change the tags, geometry, or ordering of output features based on other
   * features present in this tile. See {@link FeatureMerge} class for a set of common transformations that merge
   * linestrings/polygons.
   * <p>
   * Many threads invoke this method concurrently so ensure thread-safe access to any shared data structures.
   * <p>
   * The default implementation passes through input features unaltered
   *
   * @param tileCoord the tile being post-processed
   * @param layers    all the output features in each layer on this tile
   * @return the new map from layer to features or {@code null} to not change anything. Set any elements of the lists to
   *         {@code null} if they should be ignored.
   * @throws GeometryException for any recoverable geometric operation failures - the framework will log the error, emit
   *                           the original input features, and continue processing other tiles
   */
  default Map<String, List<VectorTile.Feature>> postProcessTileFeatures(TileCoord tileCoord,
    Map<String, List<VectorTile.Feature>> layers) throws GeometryException {
    return layers;
  }

  /**
   * Returns the name of the generated tileset to put into {@link Mbtiles} metadata
   *
   * @see <a href="https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md#metadata">MBTiles specification</a>
   */
  default String name() {
    return getClass().getSimpleName();
  }

  /**
   * Returns the description of the generated tileset to put into {@link Mbtiles} metadata
   *
   * @see <a href="https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md#metadata">MBTiles specification</a>
   */
  default String description() {
    return null;
  }

  /**
   * Returns the attribution of the generated tileset to put into {@link Mbtiles} metadata
   *
   * @see <a href="https://www.openstreetmap.org/copyright">https://www.openstreetmap.org/copyright</a> for attribution
   *      requirements of any map using OpenStreetMap data
   * @see <a href="https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md#metadata">MBTiles specification</a>
   */
  default String attribution() {
    return null;
  }

  /**
   * Returns the version of the generated tileset to put into {@link Mbtiles} metadata
   *
   * @see <a href="https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md#metadata">MBTiles specification</a>
   */
  default String version() {
    return null;
  }

  /**
   * Returns {@code true} to set {@code type="overlay"} in {@link Mbtiles} metadata otherwise sets
   * {@code type="baselayer"}
   * <p>
   * The default implementation sets {@code type="baselayer"}
   *
   * @see <a href="https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md#metadata">MBTiles specification</a>
   */
  default boolean isOverlay() {
    return false;
  }

  default Map<String, String> extraArchiveMetadata() {
    return Map.of();
  }

  /**
   * Defines whether {@link Wikidata} should fetch wikidata translations for the input element.
   * <p>
   * The default implementation returns {@code true} for all elements
   *
   * @param elem the input OSM element
   * @return {@code true} to fetch wikidata translations for {@code elem}, {@code false} to ignore
   */
  default boolean caresAboutWikidataTranslation(OsmElement elem) {
    return true;
  }

  /**
   * Invoked once for each source after all elements for that source have been processed.
   *
   * @param sourceName        the name of the source that just finished
   * @param featureCollectors a supplier for new {@link FeatureCollector} instances for a {@link SourceFeature}.
   * @param next              a consumer to pass finished map features to
   */
  default void finish(String sourceName, FeatureCollector.Factory featureCollectors,
    Consumer<FeatureCollector.Feature> next) {}

  /**
   * Returns true if this profile will use any of the elements from an input source.
   * <p>
   * The default implementation returns true.
   *
   * @param name the input source name
   * @return {@code true} if this profile uses that source, {@code false} if it is safe to ignore
   */
  default boolean caresAboutSource(String name) {
    return true;
  }

  /**
   * Returns an estimate for how many bytes of disk this profile will use for intermediate feature storage to warn when
   * running with insufficient disk space.
   */
  default long estimateIntermediateDiskBytes(long osmFileSize) {
    return 0L;
  }

  /**
   * Returns an estimate for how many bytes the output file will be to warn when running with insufficient disk space.
   */
  default long estimateOutputBytes(long osmFileSize) {
    return 0L;
  }

  /**
   * Returns an estimate for how many bytes of RAM this will use to warn when running with insufficient memory.
   * <p>
   * This should include memory for things the profile stores in memory, as well as relations and multipolygons.
   */
  default long estimateRamRequired(long osmFileSize) {
    return 0L;
  }

  /**
   * Returns false if this profile will ignore every feature in a set where {@linkplain Expression.PartialInput partial
   * attributes} are known ahead of time.
   */
  default boolean caresAbout(Expression.PartialInput input) {
    return true;
  }

  /**
   * A default implementation of {@link Profile} that emits no output elements.
   */
  class NullProfile implements Profile {

    @Override
    public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {}

    @Override
    public List<VectorTile.Feature> postProcessLayerFeatures(String layer, int zoom,
      List<VectorTile.Feature> items) {
      return items;
    }

    @Override
    public String name() {
      return "Null";
    }
  }

}
