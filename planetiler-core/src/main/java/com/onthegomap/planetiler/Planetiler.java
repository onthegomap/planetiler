package com.onthegomap.planetiler;

import com.onthegomap.planetiler.archive.TileArchiveConfig;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.archive.TileArchiveWriter;
import com.onthegomap.planetiler.archive.TileArchives;
import com.onthegomap.planetiler.archive.WriteableTileArchive;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.collection.LongLongMap;
import com.onthegomap.planetiler.collection.LongLongMultimap;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.reader.GeoPackageReader;
import com.onthegomap.planetiler.reader.NaturalEarthReader;
import com.onthegomap.planetiler.reader.ShapefileReader;
import com.onthegomap.planetiler.reader.osm.OsmInputFile;
import com.onthegomap.planetiler.reader.osm.OsmNodeBoundsProvider;
import com.onthegomap.planetiler.reader.osm.OsmReader;
import com.onthegomap.planetiler.reader.parquet.AvroParquetReader;
import com.onthegomap.planetiler.stats.ProcessInfo;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.stats.Timers;
import com.onthegomap.planetiler.stream.StreamArchiveUtils;
import com.onthegomap.planetiler.util.AnsiColors;
import com.onthegomap.planetiler.util.BuildInfo;
import com.onthegomap.planetiler.util.ByteBufferUtil;
import com.onthegomap.planetiler.util.Downloader;
import com.onthegomap.planetiler.util.FileUtils;
import com.onthegomap.planetiler.util.Format;
import com.onthegomap.planetiler.util.Geofabrik;
import com.onthegomap.planetiler.util.LogUtil;
import com.onthegomap.planetiler.util.ResourceUsage;
import com.onthegomap.planetiler.util.TileSizeStats;
import com.onthegomap.planetiler.util.TopOsmTiles;
import com.onthegomap.planetiler.util.Translations;
import com.onthegomap.planetiler.util.Wikidata;
import com.onthegomap.planetiler.worker.RunnableThatThrows;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * High-level API for creating a new map that ties together lower-level utilities in a way that is suitable for the most
 * common use-cases.
 * <p>
 * For example:
 *
 * <pre>
 * <code>
 * public static void main(String[] args) {
 *   Planetiler.create(arguments)
 *     .setProfile(new CustomProfile())
 *     .addShapefileSource("shapefile", Path.of("shapefile.zip"))
 *     .addNaturalEarthSource("natural_earth", Path.of("natural_earth.zip"))
 *     .addOsmSource("osm", Path.of("source.osm.pbf"))
 *     .setOutput("mbtiles", Path.of("output.mbtiles"))
 *     .run();
 * }</code>
 * </pre>
 * <p>
 * Each call to a builder API mutates the runner instance and returns it for more chaining.
 * <p>
 * See {@code ToiletsOverlayLowLevelApi} or unit tests for examples using the low-level API.
 */
@SuppressWarnings("UnusedReturnValue")
public class Planetiler {

  private static final Logger LOGGER = LoggerFactory.getLogger(Planetiler.class);
  private final List<Stage> stages = new ArrayList<>();
  private final List<ToDownload> toDownload = new ArrayList<>();
  private final List<InputPath> inputPaths = new ArrayList<>();
  private final Timers.Finishable overallTimer;
  private final Arguments arguments;
  private final Stats stats;
  private final Path tmpDir;
  private final Path nodeDbPath;
  private final Path multipolygonPath;
  private final Path featureDbPath;
  private final boolean downloadSources;
  private final boolean onlyDownloadSources;
  private final boolean parseNodeBounds;
  private Profile profile = null;
  private Function<Planetiler, Profile> profileProvider = null;
  private final PlanetilerConfig config;
  private FeatureGroup featureGroup;
  private OsmInputFile osmInputFile;
  private TileArchiveConfig output;
  private boolean overwrite = false;
  private boolean ran = false;
  // most common OSM languages
  private List<String> languages = List.of(
    "en", "ru", "ar", "zh", "ja", "ko", "fr",
    "de", "fi", "pl", "es", "be", "br", "he"
  );
  private Translations translations;
  private Path wikidataNamesFile;
  private boolean useWikidata = false;
  private boolean onlyFetchWikidata = false;
  private boolean fetchWikidata = false;
  private final boolean fetchOsmTileStats;
  private TileArchiveMetadata tileArchiveMetadata;

  private Planetiler(Arguments arguments) {
    this.arguments = arguments;
    stats = arguments.getStats();
    overallTimer = stats.startStageQuietly("overall");
    config = PlanetilerConfig.from(arguments);
    if (config.color() != null) {
      AnsiColors.setUseColors(config.color());
    }
    tmpDir = config.tmpDir();
    onlyDownloadSources = arguments.getBoolean("only_download", "download source data then exit", false);
    downloadSources = onlyDownloadSources || arguments.getBoolean("download", "download sources", false);
    fetchOsmTileStats =
      arguments.getBoolean("download_osm_tile_weights", "download OSM tile weights file", downloadSources);
    nodeDbPath = arguments.file("temp_nodes", "temp node db location", tmpDir.resolve("node.db"));
    multipolygonPath =
      arguments.file("temp_multipolygons", "temp multipolygon db location", tmpDir.resolve("multipolygon.db"));
    featureDbPath = arguments.file("temp_features", "temp feature db location", tmpDir.resolve("feature.db"));
    parseNodeBounds =
      arguments.getBoolean("osm_parse_node_bounds", "parse bounds from OSM nodes instead of header", false);
  }

  /** Returns a new empty runner that will get configuration from {@code arguments}. */
  public static Planetiler create(Arguments arguments) {
    return new Planetiler(arguments);
  }

  /**
   * Adds a new {@code .osm.pbf} source that will be processed when {@link #run()} is called.
   * <p>
   * To override the location of the {@code .osm.pbf} file, set {@code name_path=newpath.osm.pbf} in the arguments.
   *
   * @param name        string to use in stats and logs to identify this stage
   * @param defaultPath path to the input file to use if {@code name_path} argument is not set
   * @return this runner instance for chaining
   * @see OsmInputFile
   * @see OsmReader
   */
  public Planetiler addOsmSource(String name, Path defaultPath) {
    return addOsmSource(name, defaultPath, null);
  }

  /**
   * Adds a new {@code .osm.pbf} source that will be processed when {@link #run()} is called.
   * <p>
   * If the file does not exist and {@code download=true} argument is set, then the file will first be downloaded from
   * {@code defaultUrl}.
   * <p>
   * To override the location of the {@code .osm.pbf} file, set {@code name_path=newpath.osm.pbf} in the arguments and
   * to override the download URL set {@code name_url=http://url/of/osm.pbf}.
   *
   * @param name        string to use in stats and logs to identify this stage
   * @param defaultPath path to the input file to use if {@code name_path} argument is not set
   * @param defaultUrl  remote URL that the file to download if {@code download=true} argument is set and
   *                    {@code name_url} argument is not set. As a shortcut, can use "geofabrik:monaco" or
   *                    "geofabrik:australia" shorthand to find an extract by name from
   *                    <a href="https://download.geofabrik.de/">Geofabrik download site</a> or "aws:latest" to download
   *                    the latest {@code planet.osm.pbf} file from <a href="https://registry.opendata.aws/osm/">AWS
   *                    Open Data Registry</a>.
   * @return this runner instance for chaining
   * @see OsmInputFile
   * @see OsmReader
   * @see Downloader
   * @see Geofabrik
   */
  public Planetiler addOsmSource(String name, Path defaultPath, String defaultUrl) {
    if (osmInputFile != null) {
      // TODO: support more than one input OSM file
      throw new IllegalArgumentException("Currently only one OSM input file is supported");
    }
    Path path = getPath(name, "OSM input file", defaultPath, defaultUrl);
    var thisInputFile = new OsmInputFile(path, config.osmLazyReads());
    osmInputFile = thisInputFile;
    // fail fast if there is some issue with madvise on this system
    if (config.nodeMapMadvise() || config.multipolygonGeometryMadvise()) {
      ByteBufferUtil.init();
    }
    return appendStage(new Stage(
      name,
      List.of(
        name + "_pass1: Pre-process OpenStreetMap input (store node locations then relation members)",
        name + "_pass2: Process OpenStreetMap nodes, ways, then relations"
      ),
      ifSourceUsed(name, () -> {
        var header = osmInputFile.getHeader();
        tileArchiveMetadata.setExtraMetadata("planetiler:" + name + ":osmosisreplicationtime", header.instant());
        tileArchiveMetadata.setExtraMetadata("planetiler:" + name + ":osmosisreplicationseq",
          header.osmosisReplicationSequenceNumber());
        tileArchiveMetadata.setExtraMetadata("planetiler:" + name + ":osmosisreplicationurl",
          header.osmosisReplicationBaseUrl());
        try (
          var nodeLocations =
            LongLongMap.from(config.nodeMapType(), config.nodeMapStorage(), nodeDbPath, config.nodeMapMadvise());
          var multipolygonGeometries = LongLongMultimap.newReplaceableMultimap(
            config.multipolygonGeometryStorage(), multipolygonPath, config.multipolygonGeometryMadvise());
          var osmReader = new OsmReader(name, thisInputFile, nodeLocations, multipolygonGeometries, profile(), stats)
        ) {
          osmReader.pass1(config);
          osmReader.pass2(featureGroup, config);
        } finally {
          FileUtils.delete(nodeDbPath);
          FileUtils.delete(multipolygonPath);
        }
      }))
    );
  }

  /**
   * Adds a new ESRI shapefile source that will be processed using a projection inferred from the shapefile when
   * {@link #run()} is called.
   * <p>
   * To override the location of the {@code shapefile} file, set {@code name_path=newpath.shp.zip} in the arguments.
   *
   * @param name        string to use in stats and logs to identify this stage
   * @param defaultPath path to the input file to use if {@code name_path} key is not set through arguments. Can be a
   *                    {@code .shp} file with other shapefile components in the same directory, or a {@code .zip} file
   *                    containing the shapefile components.
   * @return this runner instance for chaining
   * @see ShapefileReader
   */
  public Planetiler addShapefileSource(String name, Path defaultPath) {
    return addShapefileSource(null, name, defaultPath);
  }

  /**
   * Adds a new ESRI shapefile source that will be processed using an explicit projection when {@link #run()} is called.
   * <p>
   * To override the location of the {@code shapefile} file, set {@code name_path=newpath.shp.zip} in the arguments.
   *
   * @param projection  the Coordinate Reference System authority code to use, parsed with
   *                    {@link org.geotools.referencing.CRS#decode(String)}
   * @param name        string to use in stats and logs to identify this stage
   * @param defaultPath path to the input file to use if {@code name_path} key is not set through arguments. Can be a
   *                    {@code .shp} file with other shapefile components in the same directory, or a {@code .zip} file
   *                    containing the shapefile components.
   * @return this runner instance for chaining
   * @see ShapefileReader
   */
  public Planetiler addShapefileSource(String projection, String name, Path defaultPath) {
    return addShapefileSource(projection, name, defaultPath, null);
  }

  /**
   * Adds a new ESRI shapefile source that will be processed with a projection inferred from the shapefile when
   * {@link #run()} is called.
   * <p>
   * If the file does not exist and {@code download=true} argument is set, then the file will first be downloaded from
   * {@code defaultUrl}.
   * <p>
   * To override the location of the {@code shapefile} file, set {@code name_path=newpath.shp.zip} in the arguments and
   * to override the download URL set {@code name_url=http://url/of/shapefile.zip}.
   *
   * @param name        string to use in stats and logs to identify this stage
   * @param defaultPath path to the input file to use if {@code name_path} key is not set through arguments. Can be a
   *                    {@code .shp} file with other shapefile components in the same directory, or a {@code .zip} file
   *                    containing the shapefile components.
   * @param defaultUrl  remote URL that the file to download if {@code download=true} argument is set and
   *                    {@code name_url} argument is not set
   * @return this runner instance for chaining
   * @see ShapefileReader
   * @see Downloader
   */
  public Planetiler addShapefileSource(String name, Path defaultPath, String defaultUrl) {
    return addShapefileSource(null, name, defaultPath, defaultUrl);
  }

  /**
   * Adds a new ESRI shapefile glob source that will process all files under {@param basePath} matching
   * {@param globPattern}. {@param basePath} may be a directory or ZIP archive.
   *
   * @param sourceName  string to use in stats and logs to identify this stage
   * @param basePath    path to the directory containing shapefiles to process
   * @param globPattern string to match filenames against, as described in {@link FileSystem#getPathMatcher(String)}.
   * @return this runner instance for chaining
   * @see ShapefileReader
   */
  public Planetiler addShapefileGlobSource(String sourceName, Path basePath, String globPattern) {
    return addShapefileGlobSource(null, sourceName, basePath, globPattern, null);
  }

  /**
   * Adds a new ESRI shapefile glob source that will process all files under {@param basePath} matching
   * {@param globPattern} using an explicit projection. {@param basePath} may be a directory or ZIP archive.
   * <p>
   * If {@param globPattern} matches a ZIP archive, all files ending in {@code .shp} within the archive will be used for
   * this source.
   * <p>
   * If the file does not exist and {@code download=true} argument is set, then the file will first be downloaded from
   * {@code defaultUrl}.
   * <p>
   *
   * @param projection  the Coordinate Reference System authority code to use, parsed with
   *                    {@link org.geotools.referencing.CRS#decode(String)}
   * @param sourceName  string to use in stats and logs to identify this stage
   * @param basePath    path to the directory or zip file containing shapefiles to process
   * @param globPattern string to match filenames against, as described in {@link FileSystem#getPathMatcher(String)}.
   * @param defaultUrl  remote URL that the file to download if {@code download=true} argument is set and
   *                    {@code name_url} argument is not set
   * @return this runner instance for chaining
   * @see ShapefileReader
   */
  public Planetiler addShapefileGlobSource(String projection, String sourceName, Path basePath,
    String globPattern, String defaultUrl) {
    Path dirPath = getPath(sourceName, "shapefile glob", basePath, defaultUrl);

    return addStage(sourceName, "Process all files matching " + dirPath + "/" + globPattern,
      ifSourceUsed(sourceName, () -> {
        var sourcePaths = FileUtils.walkPathWithPattern(basePath, globPattern,
          zipPath -> FileUtils.walkPathWithPattern(zipPath, "*.shp"));
        ShapefileReader.processWithProjection(projection, sourceName, sourcePaths, featureGroup, config,
          profile, stats);
      }));
  }


  /**
   * Adds a new ESRI shapefile source that will be processed with an explicit projection when {@link #run()} is called.
   * <p>
   * If the file does not exist and {@code download=true} argument is set, then the file will first be downloaded from
   * {@code defaultUrl}.
   * <p>
   * To override the location of the {@code shapefile} file, set {@code name_path=newpath.shp.zip} in the arguments and
   * to override the download URL set {@code name_url=http://url/of/shapefile.zip}.
   *
   * @param projection  the Coordinate Reference System authority code to use, parsed with
   *                    {@link org.geotools.referencing.CRS#decode(String)}
   * @param name        string to use in stats and logs to identify this stage
   * @param defaultPath path to the input file to use if {@code name_path} key is not set through arguments. Can be a
   *                    {@code .shp} file with other shapefile components in the same directory, or a {@code .zip} file
   *                    containing the shapefile components.
   * @param defaultUrl  remote URL that the file to download if {@code download=true} argument is set and
   *                    {@code name_url} argument is not set
   * @return this runner instance for chaining
   * @see ShapefileReader
   * @see Downloader
   */
  public Planetiler addShapefileSource(String projection, String name, Path defaultPath, String defaultUrl) {
    Path path = getPath(name, "shapefile", defaultPath, defaultUrl);
    return addStage(name, "Process features in " + path,
      ifSourceUsed(name, () -> {
        List<Path> sourcePaths = List.of(path);
        if (FileUtils.hasExtension(path, "zip") || Files.isDirectory(path)) {
          sourcePaths = FileUtils.walkPathWithPattern(path, "*.shp");
        }

        ShapefileReader.processWithProjection(projection, name, sourcePaths, featureGroup, config, profile, stats);
      }));
  }

  /**
   * Adds a new OGC GeoPackage source that will be processed when {@link #run()} is called.
   * <p>
   * If the file does not exist and {@code download=true} argument is set, then the file will first be downloaded from
   * {@code defaultUrl}.
   * <p>
   * To override the location of the {@code geopackage} file, set {@code name_path=newpath.gpkg} in the arguments and to
   * override the download URL set {@code name_url=http://url/of/file.gpkg}.
   * <p>
   * If given a path to a ZIP file containing one or more GeoPackages, each {@code .gpkg} file within will be extracted
   * to a temporary directory at runtime.
   *
   * @param projection  the Coordinate Reference System authority code to use, parsed with
   *                    {@link org.geotools.referencing.CRS#decode(String)}
   * @param name        string to use in stats and logs to identify this stage
   * @param defaultPath path to the input file to use if {@code name_path} key is not set through arguments
   * @param defaultUrl  remote URL that the file to download if {@code download=true} argument is set and
   *                    {@code name_url} argument is not set
   * @return this runner instance for chaining
   * @see GeoPackageReader
   * @see Downloader
   */
  public Planetiler addGeoPackageSource(String projection, String name, Path defaultPath, String defaultUrl) {
    Path path = getPath(name, "geopackage", defaultPath, defaultUrl);
    boolean keepUnzipped = getKeepUnzipped(name);
    return addStage(name, "Process features in " + path,
      ifSourceUsed(name, () -> {
        List<Path> sourcePaths = List.of(path);
        if (FileUtils.hasExtension(path, "zip")) {
          sourcePaths = FileUtils.walkPathWithPattern(path, "*.gpkg");
        }

        if (sourcePaths.isEmpty()) {
          throw new IllegalArgumentException("No .gpkg files found in " + path);
        }

        GeoPackageReader.process(projection, name, sourcePaths,
          keepUnzipped ? path.resolveSibling(path.getFileName() + "-unzipped") : tmpDir, featureGroup, config, profile,
          stats, keepUnzipped);
      }));
  }

  /**
   * Adds a new OGC GeoPackage source that will be processed when {@link #run()} is called.
   * <p>
   * If the file does not exist and {@code download=true} argument is set, then the file will first be downloaded from
   * {@code defaultUrl}.
   * <p>
   * To override the location of the {@code geopackage} file, set {@code name_path=newpath.gpkg} in the arguments and to
   * override the download URL set {@code name_url=http://url/of/file.gpkg}.
   * <p>
   * If given a path to a ZIP file containing one or more GeoPackages, each {@code .gpkg} file within will be extracted
   * to a temporary directory at runtime.
   *
   * @param name        string to use in stats and logs to identify this stage
   * @param defaultPath path to the input file to use if {@code name_path} key is not set through arguments
   * @param defaultUrl  remote URL that the file to download if {@code download=true} argument is set and
   *                    {@code name_url} argument is not set
   * @return this runner instance for chaining
   * @see GeoPackageReader
   * @see Downloader
   */
  public Planetiler addGeoPackageSource(String name, Path defaultPath, String defaultUrl) {
    return addGeoPackageSource(null, name, defaultPath, defaultUrl);
  }

  /**
   * Adds a new Natural Earth sqlite file source that will be processed when {@link #run()} is called.
   * <p>
   * To override the location of the {@code sqlite} file, set {@code name_path=newpath.zip} in the arguments and to
   * override the download URL set {@code name_url=http://url/of/natural_earth.zip}.
   *
   * @param name        string to use in stats and logs to identify this stage
   * @param defaultPath path to the input file to use if {@code name} key is not set through arguments. Can be the
   *                    {@code .sqlite} file or a {@code .zip} file containing the sqlite file.
   * @return this runner instance for chaining
   * @see NaturalEarthReader
   * @deprecated can be replaced by {@link #addGeoPackageSource(String, Path, String)}.
   */
  @Deprecated(forRemoval = true)
  public Planetiler addNaturalEarthSource(String name, Path defaultPath) {
    return addNaturalEarthSource(name, defaultPath, null);
  }

  /**
   * Adds a new Natural Earth sqlite file source that will be processed when {@link #run()} is called.
   * <p>
   * If the file does not exist and {@code download=true} argument is set, then the file will first be downloaded from
   * {@code defaultUrl}.
   * <p>
   * To override the location of the {@code sqlite} file, set {@code name_path=newpath.zip} in the arguments and to
   * override the download URL set {@code name_url=http://url/of/natural_earth.zip}.
   *
   * @param name        string to use in stats and logs to identify this stage
   * @param defaultPath path to the input file to use if {@code name} key is not set through arguments. Can be the
   *                    {@code .sqlite} file or a {@code .zip} file containing the sqlite file.
   * @param defaultUrl  remote URL that the file to download if {@code download=true} argument is set and
   *                    {@code name_url} argument is not set
   * @return this runner instance for chaining
   * @see NaturalEarthReader
   * @see Downloader
   * @deprecated can be replaced by {@link #addGeoPackageSource(String, Path, String)}.
   */
  @Deprecated(forRemoval = true)
  public Planetiler addNaturalEarthSource(String name, Path defaultPath, String defaultUrl) {
    Path path = getPath(name, "sqlite db", defaultPath, defaultUrl);
    boolean keepUnzipped = getKeepUnzipped(name);
    return addStage(name, "Process features in " + path, ifSourceUsed(name, () -> NaturalEarthReader
      .process(name, path, keepUnzipped ? path.resolveSibling(path.getFileName() + "-unzipped") : tmpDir, featureGroup,
        config, profile, stats, keepUnzipped)));
  }

  /**
   * Adds a new stage that will be invoked when {@link #run()} is called.
   *
   * @param name        string to use in stats and logs to identify this stage
   * @param description details to print when logging what stages will run
   * @param task        the task to run
   * @return this runner instance for chaining
   */
  public Planetiler addStage(String name, String description, RunnableThatThrows task) {
    return appendStage(new Stage(name, description, task));
  }

  /**
   * Sets the default languages that will be used by {@link #translations()} when not overridden by {@code languages}
   * argument.
   *
   * @param languages the list of languages to use when {@code name} argument is not set
   * @return this runner instance for chaining
   */
  public Planetiler setDefaultLanguages(List<String> languages) {
    this.languages = languages;
    return this;
  }

  /**
   * Updates {@link #translations()} to use name translations fetched from wikidata based on the
   * <a href="https://www.wikidata.org/wiki/Wikidata:OpenStreetMap">wikidata tag</a> on OSM elements.
   * <p>
   * When either {@code only_fetch_wikidata} or {@code fetch_wikidata} arguments are set to true, this downloads
   * translations for every OSM element that the profile cares about and stores them to {@code defaultWikidataCache} (or
   * the value of the {@code wikidata_cache} argument) before processing any sources.
   * <p>
   * As long as {@code use_wikidata} is not set to false, then previously-downloaded wikidata translations will be
   * loaded from the cache file, so you can run with {@code fetch_wikidata=true} once, then without it each subsequent
   * run to only download translations once.
   *
   * @param defaultWikidataCache Path to store downloaded wikidata name translations to, and to read them from on
   *                             subsequent runs. Overridden by {@code wikidata_cache} argument value.
   * @return this runner for chaining
   * @see Wikidata
   */
  public Planetiler fetchWikidataNameTranslations(Path defaultWikidataCache) {
    onlyFetchWikidata = arguments
      .getBoolean("only_fetch_wikidata", "fetch wikidata translations then quit", onlyFetchWikidata);
    fetchWikidata =
      onlyFetchWikidata || arguments.getBoolean("fetch_wikidata", "fetch wikidata translations then continue",
        fetchWikidata);
    useWikidata = fetchWikidata || arguments.getBoolean("use_wikidata", "use wikidata translations", true);
    wikidataNamesFile = arguments.file("wikidata_cache", "wikidata cache file", defaultWikidataCache);
    return this;
  }

  public Translations translations() {
    if (translations == null) {
      boolean transliterate = arguments.getBoolean("transliterate", "attempt to transliterate latin names", true);
      List<String> languages = arguments.getList("languages", "languages to use", this.languages);
      translations = Translations.defaultProvider(languages).setShouldTransliterate(transliterate);
    }
    return translations;
  }

  private Planetiler appendStage(Stage stage) {
    if (stages.stream().anyMatch(other -> stage.id.equals(other.id))) {
      throw new IllegalArgumentException("Duplicate stage name: " + stage.id);
    }
    stages.add(stage);
    return this;
  }

  private boolean getKeepUnzipped(String name) {
    return arguments.getBoolean(name + "_keep_unzipped",
      "keep unzipped " + name + " after reading", config.keepUnzippedSources());
  }

  /** Sets the profile implementation that controls how source feature map to output map elements. */
  public Planetiler setProfile(Profile profile) {
    this.profile = profile;
    return this;
  }

  /**
   * Sets a profile that needs information from this runner to be instantiated.
   * <p>
   * Construction will be deferred until all inputs are read.
   */
  public Planetiler setProfile(Function<Planetiler, Profile> profileProvider) {
    this.profileProvider = profileProvider;
    return this;
  }

  /**
   * Sets the location of the output archive to write rendered tiles to.
   *
   * @deprecated Use {@link #setOutput(String)} instead
   */
  @Deprecated(forRemoval = true)
  public Planetiler setOutput(String argument, Path fallback) {
    this.output =
      TileArchiveConfig
        .from(arguments.getString("output|" + argument, "output tile archive path", fallback.toString()));
    return this;
  }

  /**
   * Sets the location of the output archive to write rendered tiles to. Fails if the archive already exists.
   * <p>
   * To override the location of the file, set {@code argument=newpath} in the arguments. To set options for the output
   * drive add {@code output.mbtiles?arg=value} or add command-line argument {@code mbtiles_arg=value}.
   *
   * @param defaultOutputUri The default output URI string to write to.
   * @return this runner instance for chaining
   * @see TileArchiveConfig For details on URI string formats and options.
   */
  public Planetiler setOutput(String defaultOutputUri) {
    this.output = TileArchiveConfig.from(arguments.getString("output", "output tile archive URI", defaultOutputUri));
    return this;
  }

  /** Alias for {@link #setOutput(String)} which infers the output type based on extension. */
  public Planetiler setOutput(Path path) {
    return setOutput(path.toString());
  }

  /**
   * Sets the location of the output archive to write rendered tiles to.
   *
   * @deprecated Use {@link #overwriteOutput(String)} instead
   */
  @Deprecated(forRemoval = true)
  public Planetiler overwriteOutput(String argument, Path fallback) {
    this.overwrite = true;
    return setOutput(argument, fallback);
  }

  /**
   * Sets the location of the output archive to write rendered tiles to. Overwrites if the archive already exists.
   * <p>
   * To override the location of the file, set {@code argument=newpath} in the arguments. To set options for the output
   * drive add {@code output.mbtiles?arg=value} or add command-line argument {@code mbtiles_arg=value}.
   *
   * @param defaultOutputUri The default output URI string to write to.
   * @return this runner instance for chaining
   * @see TileArchiveConfig For details on URI string formats and options.
   */
  public Planetiler overwriteOutput(String defaultOutputUri) {
    this.overwrite = true;
    return setOutput(defaultOutputUri);
  }

  /** Alias for {@link #overwriteOutput(String)} which infers the output type based on extension. */
  public Planetiler overwriteOutput(Path defaultOutput) {
    return overwriteOutput(defaultOutput.toString());
  }

  /**
   * Reads all elements from all sourced that have been added, generates map features according to the profile, and
   * writes the rendered tiles to the output archive.
   *
   * @throws IllegalArgumentException if expected inputs have not been provided
   * @throws Exception                if an error occurs while processing
   */
  public void run() throws Exception {
    var showVersion = arguments.getBoolean("version", "show version then exit", false);
    var buildInfo = BuildInfo.get();
    if (buildInfo != null && LOGGER.isInfoEnabled()) {
      LOGGER.info("Planetiler build git hash: {}", buildInfo.githash());
      LOGGER.info("Planetiler build version: {}", buildInfo.version());
      LOGGER.info("Planetiler build timestamp: {}", buildInfo.buildTimeString());
    }
    if (showVersion) {
      System.exit(0);
    }
    if (profile() == null) {
      throw new IllegalArgumentException("No profile specified");
    }
    if (output == null) {
      throw new IllegalArgumentException("No output specified");
    }
    if (stages.isEmpty()) {
      throw new IllegalArgumentException("No sources specified");
    }
    if (ran) {
      throw new IllegalArgumentException("Can only run once");
    }
    ran = true;

    if (arguments.getBoolean("help", "show arguments then exit", false)) {
      System.exit(0);
    } else if (onlyDownloadSources) {
      // don't check files if not generating map
    } else if (config.append()) {
      if (!output.format().supportsAppend()) {
        throw new IllegalArgumentException("cannot append to " + output.format().id());
      }
      if (!output.exists()) {
        throw new IllegalArgumentException(output.uri() + " must exist when appending");
      }
    } else if (overwrite || config.force()) {
      output.delete();
    } else if (output.exists()) {
      throw new IllegalArgumentException(
        output.uri() + " already exists, use the --force argument to overwrite or --append.");
    }

    Path layerStatsPath = arguments.file("layer_stats", "layer stats output path",
      // default to <output file>.layerstats.tsv.gz
      TileSizeStats.getDefaultLayerstatsPath(Optional.ofNullable(output.getLocalPath()).orElse(Path.of("output"))));

    if (config.tileWriteThreads() < 1) {
      throw new IllegalArgumentException("require tile_write_threads >= 1");
    }
    if (config.tileWriteThreads() > 1) {
      if (!output.format().supportsConcurrentWrites()) {
        throw new IllegalArgumentException(output.format() + " doesn't support concurrent writes");
      }
      IntStream.range(1, config.tileWriteThreads())
        .mapToObj(index -> StreamArchiveUtils.constructIndexedPath(output.getLocalPath(), index))
        .forEach(p -> {
          if (!config.append() && (overwrite || config.force())) {
            FileUtils.delete(p);
          }
          if (config.append() && !Files.exists(p)) {
            throw new IllegalArgumentException("indexed file \"" + p + "\" must exist when appending");
          } else if (!config.append() && Files.exists(p)) {
            throw new IllegalArgumentException("indexed file \"" + p + "\" must not exist when not appending");
          }
        });
    }

    LOGGER.info("Building {} profile into {} in these phases:", profile.getClass().getSimpleName(), output.uri());

    if (!toDownload.isEmpty()) {
      LOGGER.info("  download: Download sources {}", toDownload.stream().map(d -> d.id).toList());
    }

    if (!onlyDownloadSources && fetchWikidata) {
      LOGGER.info("  wikidata: Fetch translations from wikidata query service");
    }

    if (!onlyDownloadSources && !onlyFetchWikidata) {
      for (Stage stage : stages) {
        for (String details : stage.details) {
          LOGGER.info("  {}", details);
        }
      }
      LOGGER.info("  sort: Sort rendered features by tile ID");
      LOGGER.info("  archive: Encode each tile and write to {}", output);
    }

    // in case any temp files are left from a previous run...
    FileUtils.delete(tmpDir, nodeDbPath, featureDbPath, multipolygonPath);
    Files.createDirectories(tmpDir);
    FileUtils.createParentDirectories(nodeDbPath, featureDbPath, multipolygonPath, output.getLocalPath());

    if (!toDownload.isEmpty()) {
      download();
    }
    if (fetchOsmTileStats) {
      TopOsmTiles.downloadPrecomputed(config);
    }
    ensureInputFilesExist();

    if (fetchWikidata) {
      Wikidata.fetch(osmInputFile(), wikidataNamesFile, config(), profile(), stats());
    }
    if (useWikidata) {
      translations().addFallbackTranslationProvider(Wikidata.load(wikidataNamesFile));
    }
    if (onlyDownloadSources || onlyFetchWikidata) {
      return; // exit only if just fetching wikidata or downloading sources
    }

    if (osmInputFile != null) {
      checkDiskSpace();
      checkMemory();
      var bounds = config.bounds();
      if (!parseNodeBounds) {
        bounds.addFallbackProvider(osmInputFile);
      }
      bounds.addFallbackProvider(new OsmNodeBoundsProvider(osmInputFile, config, stats));
    }
    // must construct this after bounds providers are added in order to infer bounds from the input source if not provided
    tileArchiveMetadata = new TileArchiveMetadata(profile, config);

    try (WriteableTileArchive archive = TileArchives.newWriter(output, config)) {
      featureGroup =
        FeatureGroup.newDiskBackedFeatureGroup(archive.tileOrder(), featureDbPath, profile, config, stats);
      stats.monitorFile("nodes", nodeDbPath);
      stats.monitorFile("features", featureDbPath);
      stats.monitorFile("multipolygons", multipolygonPath);
      stats.monitorFile("archive", output.getLocalPath());

      for (Stage stage : stages) {
        stage.task.run();
      }

      LOGGER.info("Deleting node.db to make room for output file");
      profile.release();
      for (var inputPath : inputPaths) {
        if (inputPath.freeAfterReading()) {
          LOGGER.info("Deleting {} ({}) to make room for output file", inputPath.id, inputPath.path);
          FileUtils.delete(inputPath.path());
        }
      }

      featureGroup.prepare();

      TileArchiveWriter.writeOutput(featureGroup, archive, output::size, tileArchiveMetadata, layerStatsPath, config,
        stats);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to write to " + output, e);
    }

    overallTimer.stop();
    LOGGER.info("FINISHED!");
    stats.printSummary();
    stats.close();
  }

  private void checkDiskSpace() {
    ResourceUsage readPhase = new ResourceUsage("read phase disk");
    ResourceUsage writePhase = new ResourceUsage("write phase disk");
    long osmSize = osmInputFile.diskUsageBytes();
    long nodeMapSize =
      OsmReader.estimateNodeLocationUsage(config.nodeMapType(), config.nodeMapStorage(), osmSize, tmpDir).diskUsage();
    long multipolygonGeometrySize =
      OsmReader.estimateMultipolygonGeometryUsage(config.multipolygonGeometryStorage(), osmSize, tmpDir).diskUsage();
    long featureSize = profile.estimateIntermediateDiskBytes(osmSize);
    long outputSize = profile.estimateOutputBytes(osmSize);

    // node locations and multipolygon geometries only needed while reading inputs
    readPhase.addDisk(nodeDbPath, nodeMapSize, "temporary node location cache");
    readPhase.addDisk(multipolygonPath, multipolygonGeometrySize, "temporary multipolygon geometry cache");
    // feature db persists across read/write phase
    readPhase.addDisk(featureDbPath, featureSize, "temporary feature storage");
    writePhase.addDisk(featureDbPath, featureSize, "temporary feature storage");
    // output only needed during write phase
    writePhase.addDisk(output.getLocalPath(), outputSize, "archive output");
    // if the user opts to remove an input source after reading to free up additional space for the output...
    for (var input : inputPaths) {
      if (input.freeAfterReading()) {
        writePhase.addDisk(input.path, -FileUtils.size(input.path), "delete " + input.id + " source after reading");
      }
    }

    readPhase.checkAgainstLimits(config.force(), true);
    writePhase.checkAgainstLimits(config.force(), true);
  }

  private void checkMemory() {
    Format format = Format.defaultInstance();
    ResourceUsage check = new ResourceUsage("read phase");
    ResourceUsage nodeMapUsages = OsmReader.estimateNodeLocationUsage(config.nodeMapType(), config.nodeMapStorage(),
      osmInputFile.diskUsageBytes(), tmpDir);
    ResourceUsage multipolygonGeometryUsages =
      OsmReader.estimateMultipolygonGeometryUsage(config.nodeMapStorage(), osmInputFile.diskUsageBytes(), tmpDir);
    long memoryMappedFiles = nodeMapUsages.diskUsage() + multipolygonGeometryUsages.diskUsage();

    check
      .addAll(nodeMapUsages)
      .addAll(multipolygonGeometryUsages)
      .addMemory(profile().estimateRamRequired(osmInputFile.diskUsageBytes()), "temporary profile storage");

    check.checkAgainstLimits(config().force(), true);

    // check off-heap memory if we can get it
    ProcessInfo.getSystemFreeMemoryBytes().ifPresent(extraMemory -> {
      if (extraMemory < memoryMappedFiles) {
        LOGGER.warn(
          """
            Planetiler will use ~%s memory-mapped files for node locations and multipolygon geometries but the OS only
            has %s available to cache pages, this may slow the import down. To speed up, run on a machine with more
            memory or reduce the -Xmx setting.
            """
            .formatted(
              format.storage(memoryMappedFiles),
              format.storage(extraMemory)
            ));
      } else {
        LOGGER.debug("✓ %s temporary files and %s of free memory for OS to cache them".formatted(
          format.storage(memoryMappedFiles),
          format.storage(extraMemory)

        ));
      }
    });
  }

  public Arguments arguments() {
    return arguments;
  }

  public OsmInputFile osmInputFile() {
    return osmInputFile;
  }

  public PlanetilerConfig config() {
    return config;
  }

  public Profile profile() {
    if (profile == null && profileProvider != null) {
      profile = profileProvider.apply(this);
    }
    return profile;
  }

  public Stats stats() {
    return stats;
  }

  private RunnableThatThrows ifSourceUsed(String name, RunnableThatThrows task) {
    return () -> {
      if (profile.caresAboutSource(name)) {
        task.run();
      } else {
        LogUtil.setStage(name);
        LOGGER.info("Skipping since profile does not use it");
        LogUtil.clearStage();
      }
    };
  }

  private Path getPath(String name, String type, Path defaultPath, String defaultUrl) {
    Path path = arguments.file(name + "_path", name + " " + type + " path", defaultPath);
    boolean freeAfterReading = arguments.getBoolean("free_" + name + "_after_read",
      "delete " + name + " input file after reading to make space for output (reduces peak disk usage)", false);
    if (downloadSources) {
      String url = arguments.getString(name + "_url", name + " " + type + " url", defaultUrl);
      if (!Files.exists(path) && url != null) {
        toDownload.add(new ToDownload(name, url, path));
      }
    }
    inputPaths.add(new InputPath(name, path, freeAfterReading));
    return path;
  }

  private void download() {
    var timer = stats.startStage("download");
    Downloader downloader = Downloader.create(config(), stats());
    for (ToDownload toDownload : toDownload) {
      if (profile.caresAboutSource(toDownload.id)) {
        downloader.add(toDownload.id, toDownload.url, toDownload.path);
      }
    }
    downloader.run();
    timer.stop();
  }

  private void ensureInputFilesExist() {
    for (InputPath inputPath : inputPaths) {
      if (profile.caresAboutSource(inputPath.id) && !Files.exists(inputPath.path)) {
        throw new IllegalArgumentException(inputPath.path + " does not exist");
      }
    }
  }

  public Planetiler addAvroParquetSource(String name, Path root) {
    Path path = getPath(name, "avro-parquet", root, null);
    return addStage(name, "Process features in " + path,
      ifSourceUsed(name, () -> {
        var sourcePaths = FileUtils.walkPathWithPattern(path, "*").stream().filter(Files::isRegularFile).toList();
        new AvroParquetReader(name, profile, stats)
          .process(sourcePaths, featureGroup, config);
      }));
  }

  private record Stage(String id, List<String> details, RunnableThatThrows task) {

    Stage(String id, String description, RunnableThatThrows task) {
      this(id, List.of(id + ": " + description), task);
    }
  }

  private record ToDownload(String id, String url, Path path) {}

  private record InputPath(String id, Path path, boolean freeAfterReading) {}
}
