package com.onthegomap.planetiler;

import com.onthegomap.planetiler.expression.Expression;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmRelationInfo;
import com.onthegomap.planetiler.util.Wikidata;
import java.util.HashSet;
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
   * 对瓦片的输出层中的特征应用任何后处理，然后再将其写入输出文件。
   *
   * 这些转换可能基于此瓦片中的其他特征添加、移除或更改输出特征的标签、几何形状或顺序。
   * 参见 {@link FeatureMerge} 类以获取一组常见的转换，这些转换合并线串/多边形。
   *
   * 许多线程同时调用此方法，因此请确保对任何共享数据结构的线程安全访问。
   *
   * 默认实现不改变输入特征。
   *
   * @param layer 输出层名称
   * @param zoom  瓦片的缩放级别
   * @param items 此层中的所有输出特征
   * @return 新的输出特征列表或 {@code null} 不做任何改变。将列表的任何元素设置为 {@code null} 表示应忽略它们。
   * @throws GeometryException 对于任何可恢复的几何操作失败 - 框架将记录错误，发出原始输入特征，并继续处理其他层
   */
  default List<VectorTile.Feature> postProcessLayerFeatures(String layer, int zoom,
    List<VectorTile.Feature> items) throws GeometryException {
    return items;
  }

  /**
   * 对输出瓦片中的层应用任何后处理，然后再将其写入输出。
   *
   * 这是在为每个层调用 {@link #postProcessLayerFeatures(String, int, List)} 之前调用的。
   * 如果一个层中的特征应该影响另一个层中的特征、从现有层创建新层，或者需要从输出中完全移除一个层，则使用此方法。
   *
   * 这些转换可能基于此瓦片中的其他特征添加、移除或更改输出特征的标签、几何形状或顺序。
   * 参见 {@link FeatureMerge} 类以获取一组常见的转换，这些转换合并线串/多边形。
   *
   * 许多线程同时调用此方法，因此请确保对任何共享数据结构的线程安全访问。
   *
   * 默认实现不改变输入特征。
   *
   * @param tileCoord 被后处理的瓦片
   * @param layers    此瓦片中每个层中的所有输出特征
   * @return 新的从层到特征的映射或 {@code null} 不做任何改变。将列表的任何元素设置为 {@code null} 表示应忽略它们。
   * @throws GeometryException 对于任何可恢复的几何操作失败 - 框架将记录错误，发出原始输入特征，并继续处理其他瓦片
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
   * Returns {@code true} to set {@code type="overlay"} in {@link Mbtiles} metadata otherwise sets {@code
   * type="baselayer"}
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
   * 在处理完某个源的所有元素后，每个源调用一次。
   *
   * @param sourceName        刚刚完成的源的名称
   * @param featureCollectors 一个新 {@link FeatureCollector} 实例的供应商，用于 {@link SourceFeature}。
   * @param next              传递完成的地图特征的消费者
   */
  default void finish(String sourceName, FeatureCollector.Factory featureCollectors,
    Consumer<FeatureCollector.Feature> next) {}

  /**
   * 返回此配置文件是否将使用输入源中的任何元素。
   *
   * 默认实现返回 true。
   *
   * @param name 输入源名称
   * @return {@code true} 如果此配置文件使用该源，{@code false} 如果可以忽略
   */
  default boolean caresAboutSource(String name) {
    return true;
  }

  default Map<String, HashSet<String>> geomTypes() {
    return null;
  }

  /**
   * 返回此配置文件将使用的中间特征存储的磁盘字节数的估计值，以便在磁盘空间不足时发出警告。
   */
  default long estimateIntermediateDiskBytes(long osmFileSize) {
    return 0L;
  }

  /**
   * 返回输出文件的字节数估计值，以便在磁盘空间不足时发出警告。
   */
  default long estimateOutputBytes(long osmFileSize) {
    return 0L;
  }

  /**
   * 返回此将使用的RAM的字节数估计值，以便在内存不足时发出警告。
   *
   * 这应包括配置文件存储在内存中的东西，以及关系和多边形。
   */
  default long estimateRamRequired(long osmFileSize) {
    return 0L;
  }

  /**
   * 如果此配置文件将忽略部分属性已知的输入集中所有特征，则返回 false。
   */
  default boolean caresAbout(Expression.PartialInput input) {
    return true;
  }

  /**
   * {@link Profile} 的默认实现，不发出任何输出元素。
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
