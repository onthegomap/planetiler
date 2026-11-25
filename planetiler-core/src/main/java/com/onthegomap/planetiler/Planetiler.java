package com.onthegomap.planetiler;

import static com.onthegomap.planetiler.util.LogUtil.UNTIMED_LOGGER;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import com.onthegomap.planetiler.archive.TileArchiveConfig;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.archive.TileArchiveWriter;
import com.onthegomap.planetiler.archive.TileArchives;
import com.onthegomap.planetiler.archive.WriteableTileArchive;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.collection.LongLongMap;
import com.onthegomap.planetiler.collection.LongLongMultimap;
import com.onthegomap.planetiler.config.ArgumentGroup;
import com.onthegomap.planetiler.config.ArgumentHelp;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.HelpPage;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.reader.FileFormatException;
import com.onthegomap.planetiler.reader.GeoPackageReader;
import com.onthegomap.planetiler.reader.NaturalEarthReader;
import com.onthegomap.planetiler.reader.ShapefileOptions;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.SourceFeatureProcessor;
import com.onthegomap.planetiler.reader.SourceReader;
import com.onthegomap.planetiler.reader.geojson.GeoJsonReader;
import com.onthegomap.planetiler.reader.osm.OsmHeader;
import com.onthegomap.planetiler.reader.osm.OsmInputFile;
import com.onthegomap.planetiler.reader.osm.OsmNodeBoundsProvider;
import com.onthegomap.planetiler.reader.osm.OsmReader;
import com.onthegomap.planetiler.reader.parquet.ParquetReader;
import com.onthegomap.planetiler.reader.remote.CachedRangeFile;
import com.onthegomap.planetiler.reader.remote.DownloadMode;
import com.onthegomap.planetiler.reader.shapefile.ShapefileReader;
import com.onthegomap.planetiler.stats.ProcessInfo;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.SourceProgress;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.stats.Timers;
import com.onthegomap.planetiler.util.AnsiColors;
import com.onthegomap.planetiler.util.BuildInfo;
import com.onthegomap.planetiler.util.ByteBufferUtil;
import com.onthegomap.planetiler.util.CompletableFutures;
import com.onthegomap.planetiler.util.DiskSpaceAdvice;
import com.onthegomap.planetiler.util.Downloader;
import com.onthegomap.planetiler.util.Downloader.Download;
import com.onthegomap.planetiler.util.Exceptions;
import com.onthegomap.planetiler.util.FailureReport;
import com.onthegomap.planetiler.util.FileUtils;
import com.onthegomap.planetiler.util.Format;
import com.onthegomap.planetiler.util.Geofabrik;
import com.onthegomap.planetiler.util.Glob;
import com.onthegomap.planetiler.util.InsufficientResourcesException;
import com.onthegomap.planetiler.util.LogUtil;
import com.onthegomap.planetiler.util.OvertureStac;
import com.onthegomap.planetiler.util.ResourceUsage;
import com.onthegomap.planetiler.util.TileSizeStats;
import com.onthegomap.planetiler.util.TopOsmTiles;
import com.onthegomap.planetiler.util.Translations;
import com.onthegomap.planetiler.util.Wikidata;
import com.onthegomap.planetiler.validator.JavaProfileValidator;
import com.onthegomap.planetiler.worker.RunnableThatThrows;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

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
@SuppressWarnings({"UnusedReturnValue", "java:S6539"})
public class Planetiler {

  private static final Logger LOGGER = LoggerFactory.getLogger(Planetiler.class);
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final String OSM_BOUNDS_STAGE = "osm_bounds";
  private final List<Stage> stages = new ArrayList<>();
  private final List<ToDownload> toDownload = new ArrayList<>();
  private final Map<String, List<Download>> downloadsBySource = new ConcurrentHashMap<>();
  private final List<InputPath> inputPaths = new ArrayList<>();
  private final Timers.Finishable overallTimer;
  private final Arguments arguments;
  private final Arguments sourceArgs;
  private final Map<String, Source> sourcesForHelp = new LinkedHashMap<>();
  private String areaSourceName = null;
  private String acknowledgments = null;
  private final Stats stats;
  private final Path tmpDir;
  private final Path nodeDbPath;
  private final Path multipolygonPath;
  private final Path featureDbPath;
  private final Path onlyRunTests;
  private final boolean downloadSources;
  private final DownloadMode downloadMode;
  private Downloader downloader;
  private OvertureStac overtureStac;
  private final Map<String, String> sourceMetadata = new TreeMap<>();
  private final boolean refreshSources;
  private final boolean onlyDownloadSources;
  private final boolean parseNodeBounds;
  private final boolean readSourcesSequentially;
  private boolean readSourcesTogether = false;
  private boolean nextSourceWaitsForPrevious = false;
  private int multiSourceStages = 0;
  private Profile profile = null;
  private Function<Planetiler, Profile> profileProvider = null;
  private final PlanetilerConfig config;
  private FeatureGroup featureGroup;
  private OsmInputFile osmInputFile;
  private TileArchiveConfig output;
  private boolean overwrite = false;
  private boolean ran = false;
  // track whether sources have been read yet to improve error message if we run out of disk
  private boolean sourcesRead = false;
  // most common OSM languages
  private List<String> defaultLanguages = List.of(
    "en", "ru", "ar", "zh", "ja", "ko", "fr",
    "de", "fi", "pl", "es", "be", "br", "he"
  );
  private Translations translations;
  private Path wikidataNamesFile;
  private boolean useWikidata = false;
  private boolean onlyFetchWikidata = false;
  private boolean fetchWikidata = false;
  private Duration wikidataMaxAge = Duration.ZERO;
  private int wikidataUpdateLimit = 0;
  private final boolean fetchOsmTileStats;
  private final ExecutorService virtualThreadPoolExecutor = Executors.newVirtualThreadPerTaskExecutor();

  private Planetiler(Arguments arguments) {
    this.arguments = arguments;
    this.sourceArgs = arguments.withGroup(ArgumentGroup.SOURCES);
    var inputArgs = arguments.withGroup(ArgumentGroup.INPUT);
    var processingArgs = arguments.withGroup(ArgumentGroup.PROCESSING_ADVANCED);
    stats = arguments.getStats();
    overallTimer = stats.startStageQuietly("overall");
    config = PlanetilerConfig.from(arguments);
    stats.timers().setLogInterval(config.logInterval());
    if (config.color() != null) {
      AnsiColors.setUseColors(config.color());
    }
    tmpDir = config.tmpDir();
    onlyDownloadSources = inputArgs.getBoolean("only_download", "download the sources then exit", false);
    onlyRunTests = arguments.file(ArgumentGroup.DEBUGGING, "tests", "run test cases in a yaml then quit", null);
    downloadMode = DownloadMode.parse(inputArgs.getString(
      "download",
      "download missing sources: false, cached (same as true), streaming or full",
      "false"));
    downloadSources = onlyDownloadSources || downloadMode.enabled();
    refreshSources =
      inputArgs.getBoolean("refresh_sources", "download sources again if the remote copy changed", false);
    fetchOsmTileStats = arguments.getBoolean(
      ArgumentGroup.DOWNLOADS,
      "download_osm_tile_weights",
      "download OSM tile weights file",
      downloadSources);
    nodeDbPath = processingArgs.file("temp_nodes", "temp node db location", tmpDir.resolve("node.db"));
    multipolygonPath =
      processingArgs.file("temp_multipolygons", "temp multipolygon db location", tmpDir.resolve("multipolygon.db"));
    featureDbPath = processingArgs.file("temp_features", "temp feature db location", tmpDir.resolve("feature.db"));
    parseNodeBounds =
      sourceArgs.getBoolean("osm_parse_node_bounds", "parse bounds from OSM nodes instead of header", false);
    readSourcesSequentially = inputArgs.getBoolean(
      "read_sources_sequentially",
      "read each source in its own stage even if the profile reads sources together",
      false);
  }

  /** Returns a new empty runner that will get configuration from {@code arguments}. */
  public static Planetiler create(Arguments arguments) {
    return new Planetiler(arguments);
  }

  /**
   * Adds a new {@code .osm.pbf} source that will be processed when {@link #run()} is called.
   * <p>
   * To override the location of the {@code .osm.pbf} file, set {@code name_path=newpath.osm.pbf} in the arguments.
   * <p>
   * OSM sources always run alone, never in parallel with other sources.
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
   * Adds a new {@code .osm.pbf} source for the extract named by the {@code area} argument (default
   * {@code defaultArea}), like {@link #addOsmSource(String, Path, String)}: {@code downloadDir/<area>.osm.pbf}
   * downloaded from <a href="https://download.geofabrik.de/">Geofabrik</a>, or the latest planet from the
   * <a href="https://registry.opendata.aws/osm/">AWS Open Data Registry</a> for {@code area=planet}.
   * <p>
   * A {@code name_path} that doesn't exist only gets the extract downloaded into it if {@code area} is set too, and
   * fails with an error otherwise, since filling it with {@code defaultArea} usually hides a typo in the path.
   *
   * @param name        string to use in stats and logs to identify this stage
   * @param downloadDir directory for the downloaded extract if {@code name_path} argument is not set
   * @param defaultArea Geofabrik extract to use if {@code area} argument is not set, e.g. "monaco" or "rhode island"
   * @return this runner instance for chaining
   */
  public Planetiler addOsmSourceForArea(String name, Path downloadDir, String defaultArea) {
    areaSourceName = name;
    boolean areaIsExplicit = arguments.contains("area");
    String area = arguments.getString(
      ArgumentGroup.INPUT,
      "area=<name>",
      "region to build: monaco, massachusetts, germany, planet, ... (downloaded if missing)",
      defaultArea);
    Path path = downloadDir.resolve(area.replaceAll("[^a-zA-Z]+", "_") + ".osm.pbf");
    String url = "planet".equalsIgnoreCase(area) ? "aws:latest" : "geofabrik:" + area;
    return addOsmSource(name, path, url, areaIsExplicit);
  }

  /**
   * Adds a new {@code .osm.pbf} source that will be processed when {@link #run()} is called.
   * <p>
   * If the file does not exist and {@code download=true} argument is set, then the file will first be downloaded from
   * {@code defaultUrl}.
   * <p>
   * To override the location of the {@code .osm.pbf} file, set {@code name_path=newpath.osm.pbf} in the arguments and
   * to override the download URL set {@code name_url=http://url/of/osm.pbf}.
   * <p>
   * OSM sources always run alone, never in parallel with other sources.
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
    return addOsmSource(name, defaultPath, defaultUrl, true);
  }

  private Planetiler addOsmSource(String name, Path defaultPath, String defaultUrl, boolean defaultUrlFillsPath) {
    if (osmInputFile != null) {
      // TODO: support more than one input OSM file
      throw new IllegalArgumentException("Currently only one OSM input file is supported");
    }
    boolean canFillPath = defaultUrlFillsPath || !arguments.contains(name + "_path");
    Path path = getPath(name, "OSM input file", defaultPath, canFillPath ? defaultUrl : null);
    if (!canFillPath && !arguments.contains(name + "_url") && !Files.exists(path)) {
      throw new IllegalArgumentException(path + " does not exist. Set --area or --" + name.replace('_', '-') +
        "-url (with --download) to download it");
    }
    var thisInputFile = new OsmInputFile(path, config.osmLazyReads());
    osmInputFile = thisInputFile;
    // fail fast if there is some issue with madvise on this system
    if (config.nodeMapMadvise() || config.multipolygonGeometryMadvise()) {
      ByteBufferUtil.init();
    }
    return appendStage(new SequentialStage(
      name,
      List.of(
        name + "_pass1: Pre-process OpenStreetMap input (store node locations then relation members)",
        name + "_pass2: Process OpenStreetMap nodes, ways, then relations"
      ),
      ifSourceUsed(name, () -> {
        var header = osmInputFile.getHeader();
        setSourceMetadata("planetiler:" + name + ":osmosisreplicationtime", header.instant());
        setSourceMetadata("planetiler:" + name + ":osmosisreplicationseq", header.osmosisReplicationSequenceNumber());
        setSourceMetadata("planetiler:" + name + ":osmosisreplicationurl", header.osmosisReplicationBaseUrl());
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
   * Adds a new shapefile source downloaded from a single URL that will be processed when {@link #run()} is called.
   * <p>
   * {@code defaultPath} can be:
   * <ul>
   * <li>A single {@code roads.shp}, with {@code roads.dbf}, {@code roads.prj}, ... next to it</li>
   * <li>{@code water-polygons.zip}, containing one shapefile anywhere inside it</li>
   * <li>a directory or {@code .zip} file with many shapefiles, to read the ones matching
   * {@link ShapefileOptions#globPattern()} like {@code "ne_10m_*.shp"} (in a directory, also inside each matching
   * {@code .zip} file)</li>
   * </ul>
   * Each feature's {@link SourceFeature#getSourceLayer()} is the name of its {@code .shp} file without the extension,
   * like {@code roads}.
   * <p>
   * If the file does not exist and the {@code download} argument is set, then it gets downloaded from
   * {@code defaultUrl} first. To override them, set {@code name_path=newpath} and
   * {@code name_url=http://url/of/shapefile.zip} in the arguments.
   * <p>
   * Shapefile sources run in parallel with adjacent sources when {@link #readSourcesTogether()} is enabled.
   *
   * @param name        string to use in stats and logs to identify this stage
   * @param defaultPath path to the input if {@code name_path} key is not set through arguments
   * @param defaultUrl  remote URL to download from if {@code download=true} argument is set and {@code name_url}
   *                    argument is not set, or {@code null} for a local source
   * @param options     the projection, which files to read, and changes to make to the attributes of each feature
   * @return this runner instance for chaining
   * @see ShapefileReader
   * @see Downloader
   */
  public Planetiler addShapefileSource(String name, Path defaultPath, String defaultUrl, ShapefileOptions options) {
    Path path = getPath(name, "shapefile", defaultPath, defaultUrl);
    return addSource(name, "Process features in " + path, false, () -> ShapefileReader.source(name,
      () -> findShapefiles(path, options.globPattern()), options, config, whenDownloaded(name)));
  }

  /**
   * Adds a new shapefile source downloaded from multiple URLs that will be processed when {@link #run()} is called.
   * <p>
   * {@code defaultDirectory} can be a local directory on disk containing the file names specified in
   * {@code defaultUrls}. When you run with {@code --download}, it will download those files into that directory.
   * <p>
   * {@code defaultDirectory} can also be a zip file containing all of the necessary files (or set
   * {@code name_path=path/to/shapefiles.zip} in the arguments), and only the shapefiles named like the files in
   * {@code defaultUrls} get read from it.
   * <p>
   * Each feature's {@link SourceFeature#getSourceLayer()} is the name of its {@code .shp} file without the extension.
   * To override the URLs, set {@code name_urls=http://url/one.zip,http://url/two.zip} in the arguments.
   * <p>
   * For example, to read two Natural Earth tables, downloading only their zip files into
   * {@code data/sources/natural_earth} with {@code --download}:
   *
   * {@snippet :
   * List<String> urls = List.of(
   *   "https://naciscdn.org/naturalearth/10m/physical/ne_10m_ocean.zip",
   *   "https://naciscdn.org/naturalearth/10m/physical/ne_10m_lakes.zip");
   * planetiler.addShapefileMultiUrlSource("natural_earth", Path.of("data/sources/natural_earth"), urls);
   * }
   *
   * Or to read the same tables from the full Natural Earth package downloaded manually from
   * <a href="https://naciscdn.org/naturalearth/packages/natural_earth_vector.zip">naciscdn.org</a>:
   *
   * {@snippet :
   * planetiler.addShapefileMultiUrlSource("natural_earth", Path.of("data/sources/natural_earth_vector.zip"), urls);
   * }
   * <p>
   * Shapefile sources run in parallel with adjacent sources when {@link #readSourcesTogether()} is enabled.
   *
   * @param name             string to use in stats and logs to identify this stage
   * @param defaultDirectory directory to store the files in if {@code name_path} key is not set through arguments
   * @param defaultUrls      remote URLs of the {@code .zip} files containing the shapefile components if
   *                         {@code name_urls} argument is not set
   * @param options          the projection, which files to read within each {@code .zip} file, and changes to make to
   *                         the attributes of each feature
   * @return this runner instance for chaining
   * @see ShapefileReader
   * @see Downloader
   */
  public Planetiler addShapefileMultiUrlSource(String name, Path defaultDirectory, List<String> defaultUrls,
    ShapefileOptions options) {
    String argPrefix = "--" + name.replace('_', '-');
    Path path = sourceArgs.file(
      name + "_path",
      name + " shapefile directory, or a .zip file containing all the shapefiles",
      defaultDirectory);
    // the URLs name the local files, so they're needed even when not downloading
    List<String> urls = sourceArgs.getList(name + "_urls", name + " shapefile urls", defaultUrls);
    registerSource(name, defaultDirectory,
      defaultUrls.isEmpty() ? "" : defaultUrls.size() + " files from " + host(defaultUrls.getFirst()));
    if (sourceArgs.getString(
      name + "_url",
      "not supported for " + name + ", use " + name + "_urls",
      null) != null) {
      throw new IllegalArgumentException(argPrefix + "-url is not supported since " + name + " reads " + urls.size() +
        " files, set " + argPrefix + "-urls (comma separated) or " + argPrefix +
        "-path to a .zip file containing all the shapefiles instead");
    }
    boolean freeAfterReading = sourceArgs.getBoolean(
      "free_" + name + "_after_read",
      "delete " + name + " input files after reading to make space for output (reduces peak disk usage)",
      false);
    if (Files.isRegularFile(path)) {
      // select the shapefiles now so a file without them fails before any download
      List<Path> shapefiles = findShapefilesMatchingUrls(path, urls, options.globPattern(), argPrefix);
      inputPaths.add(new InputPath(name, path, freeAfterReading, false));
      return addSource(name, "Process features in " + path, false,
        () -> ShapefileReader.source(name, () -> shapefiles, options, config, whenDownloaded(name)));
    }

    boolean refresh = refreshSource(name);
    Map<String, Path> urlToPath = new LinkedHashMap<>();
    for (String url : urls) {
      Path file = path.resolve(url.substring(url.lastIndexOf('/') + 1));
      urlToPath.put(url, file);
      inputPaths.add(new InputPath(name, file, freeAfterReading, false));
    }
    if (downloadMode(name).enabled() || refresh) {
      urlToPath.forEach((url, file) -> {
        if (needsDownload(file, refresh)) {
          toDownload.add(new ToDownload(name, url, file));
        }
      });
    }
    return addSource(name, "Process features in " + urls.size() + " files in " + path, false,
      () -> ShapefileReader.source(name, () -> urlToPath.values().stream()
        .flatMap(file -> findShapefiles(file, options.globPattern()).stream())
        .toList(), options, config, whenDownloaded(name)));
  }

  private static List<Path> findShapefiles(Path input, String globPattern) {
    return FileUtils.hasExtension(input, "zip") || Files.isDirectory(input) ?
      FileUtils.walkPathWithPattern(input, globPattern, zip -> FileUtils.walkPathWithPattern(zip, "*.shp")) :
      List.of(input);
  }

  /** Returns the shapefiles in {@code file} named like the files of {@code urls}, in the order of the URLs. */
  private static List<Path> findShapefilesMatchingUrls(Path file, List<String> urls, String globPattern,
    String argPrefix) {
    Map<String, Path> byName = new HashMap<>();
    for (Path shapefile : findShapefiles(file, globPattern)) {
      String fileName = shapefile.getFileName().toString();
      if (byName.put(fileName, shapefile) != null) {
        throw new FileFormatException(file + " contains more than one " + fileName);
      }
    }
    List<Path> result = new ArrayList<>();
    List<String> missing = new ArrayList<>();
    for (String url : urls) {
      String fileName = url.substring(url.lastIndexOf('/') + 1).replaceFirst("\\.zip$", "");
      fileName = fileName.endsWith(".shp") ? fileName : fileName + ".shp";
      Path shapefile = byName.get(fileName);
      if (shapefile == null) {
        missing.add(fileName);
      } else {
        result.add(shapefile);
      }
    }
    if (!missing.isEmpty()) {
      throw new FileFormatException(file + " is missing " + missing.size() + " of the " + urls.size() +
        " shapefiles needed (" + String.join(", ", missing) + "), set " + argPrefix +
        "-path to a directory to download them into, or a .zip file containing them");
    }
    return result;
  }

  /**
   * Alias for {@link #addShapefileSource(String, Path, String, ShapefileOptions)} for a local shapefile with the
   * projection it defines.
   */
  public Planetiler addShapefileSource(String name, Path defaultPath) {
    return addShapefileSource(name, defaultPath, null, ShapefileOptions.DEFAULT);
  }

  /**
   * Alias for {@link #addShapefileSource(String, Path, String, ShapefileOptions)} for a local shapefile with an
   * explicit projection, a Coordinate Reference System authority code parsed with
   * {@link org.geotools.referencing.CRS#decode(String)}.
   */
  public Planetiler addShapefileSource(String projection, String name, Path defaultPath) {
    return addShapefileSource(name, defaultPath, null, ShapefileOptions.DEFAULT.withProjection(projection));
  }

  /**
   * Alias for {@link #addShapefileSource(String, Path, String, ShapefileOptions)} for a shapefile downloaded from
   * {@code defaultUrl} with the projection it defines.
   */
  public Planetiler addShapefileSource(String name, Path defaultPath, String defaultUrl) {
    return addShapefileSource(name, defaultPath, defaultUrl, ShapefileOptions.DEFAULT);
  }

  /**
   * Alias for {@link #addShapefileSource(String, Path, String, ShapefileOptions)} for a shapefile downloaded from
   * {@code defaultUrl} with an explicit projection, a Coordinate Reference System authority code parsed with
   * {@link org.geotools.referencing.CRS#decode(String)}.
   */
  public Planetiler addShapefileSource(String projection, String name, Path defaultPath, String defaultUrl) {
    return addShapefileSource(name, defaultPath, defaultUrl, ShapefileOptions.DEFAULT.withProjection(projection));
  }

  /**
   * Alias for {@link #addShapefileSource(String, Path, String, ShapefileOptions)} for the files matching
   * {@code globPattern} (as described in {@link FileSystem#getPathMatcher(String)}) in a local directory or ZIP
   * archive, with the projection each defines.
   */
  public Planetiler addShapefileGlobSource(String sourceName, Path basePath, String globPattern) {
    return addShapefileSource(sourceName, basePath, null, ShapefileOptions.DEFAULT.withGlobPattern(globPattern));
  }

  /**
   * Alias for {@link #addShapefileSource(String, Path, String, ShapefileOptions)} for the files matching
   * {@code globPattern} (as described in {@link FileSystem#getPathMatcher(String)}) in a directory or ZIP archive
   * downloaded from {@code defaultUrl}, with an explicit projection, a Coordinate Reference System authority code
   * parsed with {@link org.geotools.referencing.CRS#decode(String)}.
   */
  public Planetiler addShapefileGlobSource(String projection, String sourceName, Path basePath,
    String globPattern, String defaultUrl) {
    return addShapefileSource(sourceName, basePath, defaultUrl,
      ShapefileOptions.DEFAULT.withProjection(projection).withGlobPattern(globPattern));
  }

  /**
   * Alias for {@link #addShapefileMultiUrlSource(String, Path, List, ShapefileOptions)} that reads every shapefile with
   * the projection it defines and its attributes as they are.
   */
  public Planetiler addShapefileMultiUrlSource(String name, Path defaultDirectory, List<String> defaultUrls) {
    return addShapefileMultiUrlSource(name, defaultDirectory, defaultUrls, ShapefileOptions.DEFAULT);
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
   * <p>
   * GeoPackage sources run in parallel with adjacent sources when {@link #readSourcesTogether()} is enabled.
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
    return addSource(name, "Process features in " + path, false, () -> GeoPackageReader.source(projection, name, () -> {
      List<Path> sourcePaths =
        FileUtils.hasExtension(path, "zip") ? FileUtils.walkPathWithPattern(path, "*.gpkg") : List.of(path);
      if (sourcePaths.isEmpty()) {
        throw new IllegalArgumentException("No .gpkg files found in " + path);
      }
      return sourcePaths;
    }, keepUnzipped ? path.resolveSibling(path.getFileName() + "-unzipped") : tmpDir, config, keepUnzipped,
      whenDownloaded(name)));
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
   * <p>
   * GeoPackage sources run in parallel with adjacent sources when {@link #readSourcesTogether()} is enabled.
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
   * Adds a new GeoJSON or newline-delimited GeoJSON source that will be processed when {@link #run()} is called.
   * <p>
   * If the file does not exist and {@code download=true} argument is set, then the file will first be downloaded from
   * {@code defaultUrl}.
   * <p>
   * To override the location of the {@code geojson} file, set {@code name_path=newpath.geojson} in the arguments and to
   * override the download URL set {@code name_url=http://url/of/file.geojson}.
   * <p>
   * GeoJSON sources run in parallel with adjacent sources when {@link #readSourcesTogether()} is enabled.
   *
   * @param name        string to use in stats and logs to identify this stage
   * @param defaultPath path to the input file to use if {@code name_path} key is not set through arguments
   * @param defaultUrl  remote URL that the file to download if {@code download=true} argument is set and {@code
   *                    name_url} argument is not set
   * @return this runner instance for chaining
   * @see GeoJsonReader
   * @see Downloader
   */
  public Planetiler addGeoJsonSource(String name, Path defaultPath, String defaultUrl) {
    Path path = getPath(name, "geojson", defaultPath, defaultUrl);
    return addSource(name, "Process features in " + path, false,
      () -> GeoJsonReader.source(name, () -> List.of(path), config, whenDownloaded(name)));
  }

  /** Same as {@link #addGeoJsonSource(String, Path, String)} except don't download a remote file. */
  public Planetiler addGeoJsonSource(String name, Path defaultPath) {
    return addGeoJsonSource(name, defaultPath, null);
  }

  /**
   * Adds a new Natural Earth sqlite file source that will be processed when {@link #run()} is called.
   * <p>
   * To override the location of the {@code sqlite} file, set {@code name_path=newpath.zip} in the arguments and to
   * override the download URL set {@code name_url=http://url/of/natural_earth.zip}.
   * <p>
   * Natural Earth sources run in parallel with adjacent sources when {@link #readSourcesTogether()} is enabled.
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
   * <p>
   * Natural Earth sources run in parallel with adjacent sources when {@link #readSourcesTogether()} is enabled.
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
    return addSource(name, "Process features in " + path, false, () -> NaturalEarthReader.source(name, path,
      keepUnzipped ? path.resolveSibling(path.getFileName() + "-unzipped") : tmpDir, config, keepUnzipped,
      whenDownloaded(name)));
  }


  /**
   * Adds a new <a href="https://github.com/opengeospatial/geoparquet">geoparquet</a> source that will be processed when
   * {@link #run()} is called.
   * <p>
   * Parquet sources run in parallel with adjacent sources when {@link #readSourcesTogether()} is enabled.
   *
   * @param name             string to use in stats and logs to identify this stage
   * @param paths            paths to the geoparquet files to read.
   * @param hivePartitioning Set to true to parse extra feature tags from the file path, for example
   *                         {@code {them="buildings", type="part"}} from
   *                         {@code base/theme=buildings/type=part/file.parquet}
   * @param getId            function that extracts a unique vector tile feature ID from each input feature, string or
   *                         binary features will be hashed to a {@code long}.
   * @param getLayer         function that extracts {@link SourceFeature#getSourceLayer()} from the properties of each
   *                         input feature
   * @return this runner instance for chaining
   * @see GeoPackageReader
   */
  public Planetiler addParquetSource(String name, List<Path> paths, boolean hivePartitioning,
    Function<Map<String, Object>, Object> getId, Function<Map<String, Object>, Object> getLayer) {
    return addParquetSource(name, paths, () -> paths, hivePartitioning, getId, getLayer, () -> whenDownloaded(name),
      null);
  }

  private Planetiler addParquetSource(String name, List<Path> base, Supplier<List<Path>> paths,
    boolean hivePartitioning, Function<Map<String, Object>, Object> getId,
    Function<Map<String, Object>, Object> getLayer, Supplier<CompletableFuture<Void>> download, String defaultUrl) {
    boolean freeAfterReading = sourceArgs.getBoolean(
      "free_" + name + "_after_read",
      "delete " + name + " input file after reading to make space for output (reduces peak disk usage)",
      false);
    var separator = Pattern.quote(base.isEmpty() ? "/" : base.getFirst().getFileSystem().getSeparator());
    String prefix = StringUtils.getCommonPrefix(base.stream().map(Path::toString).toArray(String[]::new))
      .replaceAll(separator + "[^" + separator + "]*$", "");
    for (var path : base) {
      inputPaths.add(new InputPath(name, path, freeAfterReading, true));
    }
    return addSource(name, "Process features in " + prefix, false, () -> withDownloadHint(defaultUrl,
      new ParquetReader(name, profile, stats, getId, getLayer, hivePartitioning).source(paths, download.get(),
        config)));
  }

  /**
   * Alias for {@link #addParquetSource(String, List, boolean, Function, Function)} using the default layer and ID
   * extractors.
   */
  public Planetiler addParquetSource(String name, List<Path> paths, boolean hivePartitioning) {
    return addParquetSource(name, paths, hivePartitioning, null, null);
  }

  /**
   * Alias for {@link #addParquetSource(String, List, boolean, Function, Function)} without hive partitioning and using
   * the default layer and ID extractors.
   */
  public Planetiler addParquetSource(String name, List<Path> paths) {
    return addParquetSource(name, paths, false);
  }

  /**
   * Adds a new <a href="https://github.com/opengeospatial/geoparquet">GeoParquet</a> source that will be processed when
   * {@link #run()} is called.
   * <p>
   * If the file does not exist and the {@code download} argument is set, then with {@code download=cached} (or
   * {@code true}) or {@code download=streaming} only the parts of {@code defaultUrl} the map bounds need get read with
   * range requests (keeping them in a sparse file at the source's path with {@code cached}), and with
   * {@code download=full} the whole file gets downloaded first. {@code download_<name>} overrides that for this source.
   * <p>
   * To override the location of the file, set {@code name_path=newpath.parquet} in the arguments and to override the
   * download URL set {@code name_url=http://url/of/file.parquet}.
   * <p>
   * Parquet sources run in parallel with adjacent sources when {@link #readSourcesTogether()} is enabled.
   *
   * @param name        string to use in stats and logs to identify this stage
   * @param defaultPath path to the input file to use if {@code name_path} key is not set through arguments
   * @param defaultUrl  remote URL of the file if {@code name_url} argument is not set
   * @return this runner instance for chaining
   * @see ParquetReader
   */
  public Planetiler addParquetSource(String name, Path defaultPath, String defaultUrl) {
    var source = sourceArgs(name, "parquet", defaultPath, defaultUrl);
    if (source.mode().fetchWhileProcessing() && source.url() != null) {
      return addSource(name, "Process features in " + source.path() + " using " +
        source.mode().name().toLowerCase(Locale.ROOT) + " range requests to " + source.url(), true,
        () -> withDownloadHint(defaultUrl, new ParquetReader(name, profile, stats).remoteSource(
          CompletableFuture.completedFuture(Map.of(source.path(), source.url())), source.mode(), source.refresh(),
          downloader(), config)));
    }
    addDownload(source);
    return addParquetSource(name, List.of(source.path()), () -> List.of(source.path()), false, null, null,
      () -> whenDownloaded(name), defaultUrl);
  }

  /**
   * Adds a new <a href="https://overturemaps.org/">Overture Maps</a> source that reads from a mirror of overture data
   * on disk, either fetched using an external tool or downloaded automatically using the stac catalog.
   * <p>
   * With {@code download=cached} (or {@code true}) or {@code download=streaming}, only the parts of the files in the
   * catalog that the map bounds need get read with range requests (keeping them in sparse files under {@code rootPath}
   * with {@code cached}), and with {@code download=full} the whole files in the map bounds get downloaded first.
   * {@code download_<name>} overrides that for this source.
   * <p>
   * Overture sources run in parallel with adjacent sources when {@link #readSourcesTogether()} is enabled.
   */
  public Planetiler addOvertureSource(String name, String theme, String type, Path rootPath) {
    Path downloadDir = sourceArgs.file(name + "_path", name + " overture download directory", rootPath);
    registerSource(name, rootPath, "Overture");
    boolean refresh = refreshSource(name);
    String catalogUrl = arguments.getString(
      ArgumentGroup.DOWNLOADS,
      "overture_catalog_url",
      "Overture STAC catalog to find files in",
      OvertureStac.DEFAULT_CATALOG_URL);
    DownloadMode mode = downloadMode(name);
    Path typeDir = downloadDir.resolve("theme=" + theme).resolve("type=" + type);
    Function<String, Path> localPath = url -> typeDir.resolve(url.substring(url.lastIndexOf('/') + 1));

    // sources get opened once the map bounds are known, so each one's catalog lookup starts then, all at once
    Supplier<CompletableFuture<List<String>>> lookup = Suppliers.memoize(() -> downloader()
      .async(() -> overtureStac().getParquetUrls(catalogUrl, theme, type, config.bounds(),
        typeDir.resolve("stac-lookup.json"), refresh)));

    if (mode.fetchWhileProcessing()) {
      return addSource(name, "Process features in " + typeDir + " using " + mode.name().toLowerCase(Locale.ROOT) +
        " range requests", true,
        () -> new ParquetReader(name, profile, stats, fields -> fields.get("id"), fields -> fields.get("type"), true)
          .remoteSource(lookup.get().thenApply(urls -> urls.stream().collect(Collectors.toMap(localPath, url -> url))),
            mode, refresh, downloader(), config));
    }

    boolean download = mode.enabled() || refresh;
    if (download) {
      // ensure parent directory exists
      FileUtils.createDirectory(typeDir);
    }
    return addParquetSource(name, List.of(typeDir),
      () -> Glob.of(downloadDir).resolve("theme=" + theme, "type=" + type, "*.parquet").find(),
      true,
      fields -> fields.get("id"),
      fields1 -> fields1.get("type"),
      () -> download ? lookup.get().thenCompose(urls -> {
        Map<String, Path> urlToPath = new LinkedHashMap<>();
        for (String url : urls) {
          if (needsDownload(localPath.apply(url), refresh)) {
            urlToPath.put(url, localPath.apply(url));
          }
        }
        startDownloads(name, urlToPath);
        return whenDownloaded(name);
      }) : CompletableFutures.NULL,
      null);
  }

  private synchronized OvertureStac overtureStac() {
    if (overtureStac == null) {
      overtureStac = new OvertureStac(downloader());
    }
    return overtureStac;
  }

  /** Makes Overture sources list their files with {@code overtureStac}, for tests to simulate the STAC catalog. */
  synchronized Planetiler setOvertureStac(OvertureStac overtureStac) {
    this.overtureStac = overtureStac;
    return this;
  }

  // use one downloader for whole files and range requests, so they share the bandwidth limit
  private synchronized Downloader downloader() {
    if (downloader == null) {
      downloader = Downloader.create(config);
    }
    return downloader;
  }

  /** Makes downloads use {@code downloader}, for tests to simulate a server. */
  synchronized Planetiler setDownloader(Downloader downloader) {
    this.downloader = downloader;
    return this;
  }

  private boolean refreshSource(String name) {
    return sourceArgs.getBoolean(
      "refresh_" + name,
      "download new version of " + name + " if changed",
      refreshSources);
  }

  private DownloadMode downloadMode(String name) {
    var mode = DownloadMode.parse(sourceArgs.getString(
      "download_" + name,
      "how to download " + name + ": false, cached (same as true), streaming or full",
      downloadMode.name().toLowerCase(Locale.ROOT)));
    // only_download is for running offline later, which needs whole files (unless explicitly marked as streaming)
    return onlyDownloadSources && mode != DownloadMode.STREAMING ? DownloadMode.FULL : mode;
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
    return appendStage(new SequentialStage(name, List.of(name + ": " + description), task));
  }

  /**
   * Reads the sources added next to each other (anything but OpenStreetMap data) together in one shared pipeline
   * instead of one after another, which keeps more cores busy while sources are small or still downloading.
   * <p>
   * Features from sources read together reach {@link Profile#processFeature} in no particular order, and
   * {@link Profile#finish} runs for each of them once all of them have been processed. Call
   * {@link #waitForPreviousSources()} before adding a source that needs the ones before it to be finished first.
   * OpenStreetMap data and {@link #addStage stages} always wait for the sources added before them. The
   * {@code --read-sources-sequentially} argument turns this off when running.
   *
   * @return this runner instance for chaining
   */
  public Planetiler readSourcesTogether() {
    readSourcesTogether = true;
    return this;
  }

  /**
   * Makes the next source added wait until the sources added before it have been processed and {@link Profile#finish}
   * has run for them, instead of being read together with them after {@link #readSourcesTogether()}.
   *
   * @return this runner instance for chaining
   */
  public Planetiler waitForPreviousSources() {
    nextSourceWaitsForPrevious = true;
    return this;
  }

  /**
   * Adds a source that {@link #readSourcesTogether()} can read together with the sources added right before and after
   * it.
   * <p>
   * {@code open} gets called once the map bounds are known.
   */
  private Planetiler addSource(String name, String description, boolean fetchesWhileProcessing,
    Supplier<SourceReader> open) {
    var stage =
      new ParallelStage(name, List.of(name + ": " + description), fetchesWhileProcessing, nextSourceWaitsForPrevious,
        Suppliers.memoize(open::get));
    nextSourceWaitsForPrevious = false;
    return appendStage(stage);
  }

  /**
   * Sets text to print at the end of a successful run, before the summary of what was written: for example the licenses
   * of the data in the output and the credit that maps made from it must show.
   *
   * @param acknowledgments the text, may span several lines
   * @return this runner instance for chaining
   */
  public Planetiler setAcknowledgments(String acknowledgments) {
    this.acknowledgments = acknowledgments;
    return this;
  }

  /**
   * Sets the default languages that will be used by {@link #translations()} when not overridden by {@code languages}
   * argument.
   *
   * @param languages the list of languages to use when {@code name} argument is not set
   * @return this runner instance for chaining
   */
  public Planetiler setDefaultLanguages(List<String> languages) {
    this.defaultLanguages = languages;
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
    var args = arguments.withGroup(ArgumentGroup.WIKIDATA);
    onlyFetchWikidata = args
      .getBoolean("only_fetch_wikidata", "fetch wikidata translations then quit", onlyFetchWikidata);
    fetchWikidata =
      onlyFetchWikidata || args.getBoolean(
        "fetch_wikidata",
        "fetch wikidata translations then continue",
        fetchWikidata);
    useWikidata = fetchWikidata || args.getBoolean("use_wikidata", "use wikidata translations", true);
    wikidataNamesFile = args.file("wikidata_cache", "wikidata cache file", defaultWikidataCache);
    wikidataMaxAge = args.getDuration(
      "wikidata_max_age",
      "maximum age of Wikidata translations (in ISO-8601 duration format PnDTnHnMn.nS; 0S = disabled)",
      "0s");
    wikidataUpdateLimit = args.getInteger(
      "wikidata_update_limit",
      "limit on how many old translations to update during one download (0 = disabled)",
      0);
    return this;
  }

  public Translations translations() {
    if (translations == null) {
      boolean transliterate =
        arguments.getBoolean("transliterate", "transliterate names to latin script when there is no latin name", false);
      List<String> languages = arguments.getList("languages",
        "languages to include labels for; \"default\", \"-xx\" to leave one out, \"*\" for every language",
        this.defaultLanguages);
      if (languages.contains("default")) {
        languages = Stream.concat(
          languages.stream().filter(language -> !language.equals("default")),
          this.defaultLanguages.stream()
        ).toList();
      }
      translations = Translations.defaultProvider(languages)
        .setShouldTransliterate(transliterate)
        .setExtraNameTags(config.extraNameTags());
    }
    return translations;
  }

  private Planetiler appendStage(Stage stage) {
    if (stages.stream().anyMatch(other -> stage.id().equals(other.id()))) {
      throw new IllegalArgumentException("Duplicate stage name: " + stage.id());
    }
    stages.add(stage);
    return this;
  }

  private boolean getKeepUnzipped(String name) {
    return sourceArgs.getBoolean(
      name + "_keep_unzipped",
      "keep unzipped " + name + " after reading",
      config.keepUnzippedSources());
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
        .from(arguments.withGroup(ArgumentGroup.OUTPUT)
          .getString("output=<file>|" + argument + "|-o", ".pmtiles, .mbtiles, or a directory of tiles",
            fallback.toString()));
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
    this.output = TileArchiveConfig.from(arguments.withGroup(ArgumentGroup.OUTPUT)
      .getString("output=<file>|-o", ".pmtiles, .mbtiles, or a directory of tiles", defaultOutputUri));
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

  /**
   * Alias for {@link #overwriteOutput(String)} which infers the output type based on extension.
   * <p>
   * This will override the value returned by
   */
  public Planetiler overwriteOutput(Path defaultOutput) {
    return overwriteOutput(defaultOutput.toString());
  }

  /**
   * Reads all elements from all sources that have been added, generates map features according to the profile, and
   * writes the rendered tiles to the output archive.
   * <p>
   * The following errors come with user-facing messages explaining how to recover that can be caught/logged without
   * printing the whole stack trace:
   *
   * @throws IllegalArgumentException       if expected inputs have not been provided
   * @throws InsufficientResourcesException if there isn't enough memory or disk space for the run, or a disk filled up
   * @throws ExpectedFailureException       if the run failed for another expected reason (out of memory, an error
   *                                        reading or writing a file, ...), with the original error as its cause
   */
  public void run() {
    try {
      doRun();
    } catch (Throwable e) { // NOSONAR
      String message = FailureReport.expectedFailureMessage(e);
      throw Exceptions.<RuntimeException>throwFatalException(
        message == null || message.equals(e.getMessage()) ? e : new ExpectedFailureException(message, e));
    }
  }

  @SuppressWarnings("java:S106")
  private void doRun() {
    var showVersion = arguments.getBoolean(ArgumentGroup.OTHER, "version", "show version then exit", false);
    var buildInfo = BuildInfo.get();
    if (showVersion) {
      System.out.println(BuildInfo.versionDetails());
      System.exit(0);
    }
    boolean verbose = arguments.getBoolean(
      ArgumentGroup.LOGGING,
      "verbose|-v",
      "show option values and where each came from, details of each stage, and stack traces for errors",
      false);
    boolean quiet = arguments.getBoolean(ArgumentGroup.LOGGING, "quiet|-q", "only show warnings and errors", false);
    // creates the profile, so the options it reads show up in the help and option list
    profile();
    Path layerStatsPath = arguments.file(
      ArgumentGroup.OUTPUT_ADVANCED,
      "layer_stats",
      "layer stats output path",
      // default to <output file>.layerstats.tsv.gz
      TileSizeStats.getDefaultLayerstatsPath(Optional.ofNullable(output)
        .map(TileArchiveConfig::getLocalPath)
        .orElse(Path.of("output"))));
    if (output != null && profile() != null) {
      // the archive and its metadata read their options when they're created, after the sources are read
      TileArchiveMetadata.readOptions(profile(), config);
      TileArchives.parseWriterOptions(output, config);
    }
    arguments.printHelpIfRequested(this::help);
    if (verbose) {
      LogUtil.setLevel(Level.DEBUG);
    } else if (quiet) {
      LogUtil.setLevel(Level.WARN);
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
    if (onlyRunTests == null && !onlyDownloadSources) {
      // before anything prints, so a rerun that would overwrite the output stops with one line saying so
      checkOutput();
    }
    UNTIMED_LOGGER.atInfo().setMessage("{}, {} profile")
      .addArgument(BuildInfo::planetilerVersion)
      .addArgument(this::profileName)
      .log();
    if (UNTIMED_LOGGER.isDebugEnabled()) {
      if (buildInfo != null) {
        UNTIMED_LOGGER.debug("Built from git {} at {}", buildInfo.githash(), buildInfo.buildTimeString());
      }
      UNTIMED_LOGGER.debug("{}, {} threads, max heap {}",
        ProcessInfo.describeRuntime(),
        config.threads(),
        Format.defaultInstance().storage(ProcessInfo.getMaxMemoryBytes()));
    }
    arguments.warnAboutUnknownOptions();
    if (verbose) {
      System.out.println(ArgumentHelp.verboseList(arguments.options(), profileName() + " options"));
      arguments.logOptionsReadFromNowOn();
    }
    if (onlyRunTests != null) {
      boolean success = JavaProfileValidator.validate(profile(), onlyRunTests, config());
      System.exit(success ? 0 : 1);
    }

    // Keep reuse metadata next to feature.db so it survives directory cleanup and is easy to atomically manage.
    var stringEncoderPath = featureDbPath.resolve(featureDbPath.getFileName() + ".strings");
    var chunkManifestPath = featureDbPath.resolve(featureDbPath.getFileName() + ".manifest");
    boolean hasFeatureDb = Files.exists(featureDbPath);
    boolean hasStringEncoders = Files.exists(stringEncoderPath);
    boolean hasChunkManifest = Files.exists(chunkManifestPath);
    boolean hasSourceInfo = Files.exists(sourceInfoPath());
    boolean hasReusableFeatureDb = hasFeatureDb && hasStringEncoders && hasChunkManifest && hasSourceInfo;
    boolean reuseExistingFeatureDb = config.reuseFeatureDb() && hasReusableFeatureDb;

    logStart();
    logPhases(reuseExistingFeatureDb);

    if (reuseExistingFeatureDb) {
      LOGGER.info("Reusing existing feature DB state at {}", featureDbPath);
    } else {
      if (config.reuseFeatureDb()) {
        LOGGER.info("--reuse_featuredb enabled with no previous reusable state; building feature DB for reuse");
      }
      FileUtils.delete(featureDbPath);
    }
    // in case any temp files are left from a previous run...
    FileUtils.delete(nodeDbPath, multipolygonPath);
    if (!reuseExistingFeatureDb) {
      FileUtils.delete(tmpDir);
    }
    FileUtils.createDirectory(tmpDir);
    FileUtils.createParentDirectories(nodeDbPath, featureDbPath, multipolygonPath, output.getLocalBasePath());

    if (reuseExistingFeatureDb) {
      // no source gets read, and the bounds come from the run that wrote the feature DB
      restoreSourceInfo();
    } else {
      ensureInputFilesExist();
      addOsmBoundsFallback();
    }
    startPendingDownloads();
    // A reused feature DB, --only-download and --only-fetch-wikidata don't read the sources, but their downloads
    // still run, so only open the sources that need downloading.
    Map<String, SourceReader> sources = openSources(onlyDownloadSources || onlyFetchWikidata || reuseExistingFeatureDb);
    long tilesWritten;
    try {
      if (fetchOsmTileStats) {
        TopOsmTiles.downloadPrecomputed(downloader());
      }

      if (fetchWikidata) {
        List<Download> allDownloads = stages.stream().map(Stage::id).flatMap(this::downloadsOf).toList();
        awaitDownloads("wikidata", allDownloads);
        Wikidata.fetch(osmInputFile(), wikidataNamesFile, downloader(), profile(), stats(), wikidataMaxAge,
          wikidataUpdateLimit);
      }
      if (useWikidata) {
        translations().addFallbackTranslationProvider(Wikidata.load(wikidataNamesFile));
      }
      if (onlyDownloadSources || onlyFetchWikidata || reuseExistingFeatureDb) {
        // A source whose files come from a catalog lookup (Overture) only starts its downloads once the lookup
        // returns, so wait for the sources to be ready rather than for their downloads started so far.
        List<Download> otherDownloads = downloadsBySource.entrySet().stream()
          .filter(entry -> !sources.containsKey(entry.getKey()))
          .flatMap(entry -> entry.getValue().stream())
          .toList();
        awaitSourcesAndDownloads("download", sources.values(), otherDownloads);
        FileUtils.closeAll(sources.values());
        sources.clear();
        if (onlyDownloadSources || onlyFetchWikidata) {
          return; // exit only if just fetching wikidata or downloading sources
        }
      }

      if (osmInputFile != null && !reuseExistingFeatureDb) {
        checkDiskSpace();
        checkMemory();
      }
      // must construct this after bounds providers are added in order to infer bounds from the input source if not provided
      TileArchiveMetadata tileArchiveMetadata = new TileArchiveMetadata(profile, config);

      try (WriteableTileArchive archive = TileArchives.newWriter(output, config)) {
        featureGroup =
          FeatureGroup.newDiskBackedFeatureGroup(archive.tileOrder(), featureDbPath, profile, config, stats,
            reuseExistingFeatureDb);
        stats.monitorFile("nodes", nodeDbPath);
        stats.monitorFile("features", featureDbPath);
        stats.monitorFile("multipolygons", multipolygonPath);
        stats.monitorFile("archive", output.getLocalPath(), archive::bytesWritten);

        if (reuseExistingFeatureDb) {
          featureGroup.loadStringEncoders(stringEncoderPath);
          featureGroup.initFromManifest(chunkManifestPath);
        } else {
          // with readSourcesTogether(), sources next to each other get read together, a sequential stage needs everything
          // before it to be finished
          List<SourceReader> readTogether = new ArrayList<>();
          for (Stage stage : stages) {
            switch (stage) {
              case ParallelStage source when sources.containsKey(source.id) -> {
                if (!readSourcesTogether || readSourcesSequentially || source.waitsForPrevious) {
                  processSources(readTogether);
                  readTogether.clear();
                }
                // the stage that reads a source closes it
                readTogether.add(sources.remove(source.id));
              }
              case ParallelStage _ -> {
                // the profile doesn't use this source, so it never got opened
              }
              case SequentialStage task -> {
                processSources(readTogether);
                readTogether.clear();
                try {
                  awaitDownloads(task.id, downloadsOf(task.id).toList());
                  task.task.run();
                } catch (Exception e) {
                  throw new PlanetilerException("Error occurred during stage " + task.id, e);
                }
              }
            }
          }
          processSources(readTogether);
          sourcesRead = true;

          LOGGER.debug("Deleting node.db to make room for output file");
          profile.release();
          for (var inputPath : inputPaths) {
            if (inputPath.freeAfterReading()) {
              LOGGER.info("Deleting {} ({}) to make room for output file", inputPath.id, inputPath.path);
              FileUtils.delete(inputPath.path());
            }
          }

          featureGroup.prepare();
          if (config.reuseFeatureDb()) {
            featureGroup.saveStringEncoders(stringEncoderPath);
            featureGroup.saveChunkManifest(chunkManifestPath);
            saveSourceInfo();
          }
        }

        sourceMetadata.forEach(tileArchiveMetadata::setExtraMetadata);
        tilesWritten =
          TileArchiveWriter.writeOutput(featureGroup, archive, archive::bytesWritten, tileArchiveMetadata,
            layerStatsPath, config, stats);
      } catch (IOException e) {
        throw new PlanetilerException("Unable to write to " + output, e);
      }
    } catch (Throwable e) { // NOSONAR
      // before deleting the output, so it counts toward what filled up the disk
      Throwable explained = diskSpaceAdvice().explainFailure(e, sourcesRead)
        .<Throwable>map(message -> new InsufficientResourcesException(message, e))
        .orElse(e);
      if (!config.append()) {
        // only a finished archive is usable, and without it the next run doesn't need --force
        FileUtils.deleteQuietly(output.getLocalPath());
      }
      throw Exceptions.<RuntimeException>throwFatalException(explained);
    } finally {
      // the sources a stage hasn't read, like when an earlier one failed or nothing reads them
      FileUtils.closeAll(sources.values());
    }

    overallTimer.stop();
    stats.printSummary();
    logClosingLines(tilesWritten);
    try {
      stats.close();
      virtualThreadPoolExecutor.close();
      if (downloader != null) {
        downloader.close();
      }
    } catch (Exception e) {
      throw new PlanetilerException(e);
    }
  }

  private DiskSpaceAdvice diskSpaceAdvice() {
    var sources = inputPaths.stream()
      .map(input -> new DiskSpaceAdvice.Source(input.id(), input.path(), input.freeAfterReading()))
      .toList();
    return new DiskSpaceAdvice(sources, tmpDir, output.getLocalPath(), config.compressTempStorage(),
      DiskSpaceAdvice.Disk::of);
  }

  private void checkOutput() {
    if (config.append()) {
      if (!output.format().supportsAppend()) {
        throw new IllegalArgumentException("cannot append to " + output.format().id());
      }
      if (!output.exists()) {
        throw new IllegalArgumentException(outputName() + " must exist when appending");
      }
    } else if (overwrite || config.force() || !output.existsWithTiles()) {
      // also replaces what a run that stopped before writing any tile left behind
      output.delete();
    } else {
      throw new IllegalArgumentException(outputName() + " already exists, add --force to overwrite it" +
        (output.format().supportsAppend() ? " or --append to add to it" : "") + ".");
    }
    if (config.tileWriteThreads() < 1) {
      throw new IllegalArgumentException("require tile_write_threads >= 1");
    }
    if (config.tileWriteThreads() > 1) {
      if (!output.format().supportsConcurrentWrites()) {
        throw new IllegalArgumentException(output.format() + " doesn't support concurrent writes");
      }
      IntStream.range(1, config.tileWriteThreads())
        .mapToObj(output::getPathForMultiThreadedWriter)
        .forEach(p -> {
          if (!config.append() && (overwrite || config.force())) {
            FileUtils.delete(p);
          }
          if (config.append() && !output.exists(p)) {
            throw new IllegalArgumentException("indexed archive \"" + p + "\" must exist when appending");
          } else if (!config.append() && output.exists(p)) {
            throw new IllegalArgumentException("indexed archive \"" + p + "\" must not exist when not appending");
          }
        });
    }
  }

  private void logStart() {
    if (UNTIMED_LOGGER.isInfoEnabled()) {
      UNTIMED_LOGGER.info("Building {}", outputName());
    }
  }

  private void logPhases(boolean reuseExistingFeatureDb) {
    if (!LOGGER.isDebugEnabled()) {
      return;
    }
    LOGGER.debug("Building {} profile into {} in these phases:", profile.getClass().getSimpleName(), output.uri());
    if (!toDownload.isEmpty()) {
      LOGGER.debug("  download: Download sources {}", toDownload.stream().map(d -> d.id).distinct().toList());
    }
    if (!onlyDownloadSources && fetchWikidata) {
      LOGGER.debug("  wikidata: Fetch translations from wikidata query service");
    }
    if (!onlyDownloadSources && !onlyFetchWikidata) {
      if (reuseExistingFeatureDb) {
        LOGGER.debug("  reuse: Reusing existing feature DB at {} (skipping source reading stages)", featureDbPath);
      } else {
        for (Stage stage : stages) {
          for (String details : stage.details()) {
            LOGGER.debug("  {}", details);
          }
        }
        LOGGER.debug("  sort: Sort rendered features by tile ID");
      }
      LOGGER.debug("  archive: Encode each tile and write to {}", output);
    }
  }

  private String outputName() {
    return output.getLocalPath() == null ? output.uri().toString() : FileUtils.displayPath(output.getLocalPath());
  }

  private void logClosingLines(long tilesWritten) {
    if (!UNTIMED_LOGGER.isInfoEnabled()) {
      return;
    }
    Format format = Format.defaultInstance();
    UNTIMED_LOGGER.info("");
    if (acknowledgments != null) {
      UNTIMED_LOGGER.info(AnsiColors.dim(acknowledgments.strip()));
    }
    UNTIMED_LOGGER.info(AnsiColors.dim("Questions or issues: https://github.com/onthegomap/planetiler"));
    UNTIMED_LOGGER.info(AnsiColors.dim("Sponsor planetiler: https://github.com/sponsors/onthegomap"));
    // Only the launcher builds the warm-up cache with one command, under java -jar it takes two runs with JVM flags.
    if (ArgumentHelp.wasStartedByLauncher() && !ProcessInfo.usesAotCache()) {
      UNTIMED_LOGGER.info("Tip: run `{}` once to make every later run start faster.",
        ArgumentHelp.planetilerCommand("setup"));
    }
    String viewHint = output.format() == TileArchiveConfig.Format.PMTILES ?
      ". View it on https://pmtiles.io" : "";
    UNTIMED_LOGGER.info("");
    UNTIMED_LOGGER.info("Wrote {} ({}B, {} tiles){}", outputName(), format.storage(output.size(), false),
      format.integer(tilesWritten), viewHint);
    UNTIMED_LOGGER.info("");
  }

  private void checkDiskSpace() {
    var advice = diskSpaceAdvice();
    ResourceUsage readPhase = new ResourceUsage("temporary files while reading the sources", advice.readPhaseHint());
    ResourceUsage writePhase = new ResourceUsage("temporary files and the output", advice.writePhaseHint());
    long osmSize = osmSizeBytes();
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
    ResourceUsage check = new ResourceUsage("reading the sources", diskSpaceAdvice().readPhaseHint());
    ResourceUsage nodeMapUsages = OsmReader.estimateNodeLocationUsage(config.nodeMapType(), config.nodeMapStorage(),
      osmSizeBytes(), tmpDir);
    ResourceUsage multipolygonGeometryUsages =
      OsmReader.estimateMultipolygonGeometryUsage(config.nodeMapStorage(), osmSizeBytes(), tmpDir);
    long memoryMappedFiles = nodeMapUsages.diskUsage() + multipolygonGeometryUsages.diskUsage();

    check
      .addAll(nodeMapUsages)
      .addAll(multipolygonGeometryUsages)
      .addMemory(profile().estimateRamRequired(osmSizeBytes()), "temporary profile storage");

    check.checkAgainstLimits(config().force(), true);

    // Only at debug level: runs with less free memory than this work fine, just slower (planets on 8 GB machines).
    if (LOGGER.isDebugEnabled()) {
      ProcessInfo.getSystemFreeMemoryBytes().ifPresent(freeMemory -> LOGGER.debug(
        "{} {} memory-mapped temporary files, {} of free memory for the OS to cache them",
        freeMemory < memoryMappedFiles ? "⚠️" : "✓", format.storage(memoryMappedFiles), format.storage(freeMemory)));
    }
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
        logUnused(name);
      }
    };
  }

  private static void logUnused(String name) {
    LogUtil.setStage(name);
    LOGGER.debug("Skipping since profile does not use it");
    LogUtil.clearStage();
  }

  /**
   * Opens every source the profile uses, which starts whatever it can do before its turn to be read, except the ones
   * that fetch while processing if {@code onlyDownloads}.
   */
  private Map<String, SourceReader> openSources(boolean onlyDownloads) {
    Map<String, SourceReader> result = new LinkedHashMap<>();
    for (Stage stage : stages) {
      if (stage instanceof ParallelStage source) {
        if (!profile.caresAboutSource(source.id)) {
          logUnused(source.id);
        } else if (!onlyDownloads || !source.fetchesWhileProcessing) {
          var reader = source.open.get();
          // A source that isn't ready yet waits for downloads, which a catalog lookup (Overture) may only start later,
          // so the row looks them up each time and counts as downloaded once the source is ready.
          if (!source.fetchesWhileProcessing &&
            (downloadsBySource.containsKey(source.id) || !reader.whenReady().isDone())) {
            reader.progress().setDownload(
              () -> downloadsOf(source.id).mapToLong(Download::bytesDownloaded).sum(),
              () -> downloadsOf(source.id).mapToLong(Download::bytesToFetch).sum(),
              () -> reader.whenReady().isDone());
          }
          result.put(source.id, reader);
        }
      }
    }
    return result;
  }

  /** Reads {@code sources} in one shared pipeline, named after the source when there is only one. */
  private void processSources(List<SourceReader> sources) {
    if (sources.isEmpty()) {
      return;
    }
    Set<String> names = sources.stream().map(SourceReader::name).collect(Collectors.toSet());
    // show the downloads for later stages too, so every transfer in progress is visible
    List<SourceProgress> otherDownloads = downloadsBySource.entrySet().stream()
      .filter(entry -> !names.contains(entry.getKey()))
      .filter(entry -> !entry.getValue().stream().allMatch(download -> download.done().isDone()))
      .map(entry -> Download.progress(entry.getKey(), entry.getValue()))
      .toList();
    String stageName;
    if (sources.size() == 1) {
      stageName = sources.getFirst().name();
    } else {
      multiSourceStages++;
      stageName = multiSourceStages == 1 ? "sources" : "sources-" + multiSourceStages;
    }
    try {
      SourceFeatureProcessor.process(
        stageName, List.copyOf(sources), otherDownloads, featureGroup, config, profile, stats);
    } catch (RuntimeException e) {
      // report what the reader threw, like a sequential stage does, not the wrappers it picked up on the way from the worker
      Throwable cause = Exceptions.unwrap(e);
      throw new PlanetilerException("Error occurred during stage " + stageName, cause instanceof Exception ex ? ex : e);
    }
  }

  /** Starts downloading every file queued by the sources the profile uses, without waiting for any of them. */
  private void startPendingDownloads() {
    Map<String, Map<String, Path>> bySource = new LinkedHashMap<>();
    for (ToDownload download : toDownload) {
      if (profile.caresAboutSource(download.id)) {
        bySource.computeIfAbsent(download.id, _ -> new LinkedHashMap<>()).put(download.url, download.path);
      }
    }
    toDownload.clear();
    bySource.forEach(this::startDownloads);
  }

  private void startDownloads(String name, Map<String, Path> urlToPath) {
    if (!urlToPath.isEmpty()) {
      downloadsBySource.computeIfAbsent(name, _ -> new CopyOnWriteArrayList<>())
        .addAll(downloader().startAll(name, urlToPath));
    }
  }

  /** Returns a future that completes when every download started so far for source {@code name} has finished. */
  private CompletableFuture<Void> whenDownloaded(String name) {
    return CompletableFuture.allOf(downloadsOf(name).map(Download::done).toArray(CompletableFuture[]::new));
  }

  private Stream<Download> downloadsOf(String name) {
    return downloadsBySource.getOrDefault(name, List.of()).stream();
  }

  private void awaitDownloads(String stage, List<Download> downloads) {
    awaitSourcesAndDownloads(stage, List.of(), downloads);
  }

  /**
   * Waits for {@code sources} to be ready and for {@code downloads} to finish, logging their progress under
   * {@code stage} meanwhile.
   */
  private void awaitSourcesAndDownloads(String stage, Collection<SourceReader> sources, List<Download> downloads) {
    List<SourceProgress> rows = new ArrayList<>();
    List<CompletableFuture<Void>> waitingFor = new ArrayList<>();
    for (SourceReader source : sources) {
      rows.add(source.progress());
      waitingFor.add(source.whenReady());
    }
    downloads.stream().collect(Collectors.groupingBy(Download::id, LinkedHashMap::new, Collectors.toList()))
      .forEach((id, ofSource) -> rows.add(Download.progress(id, ofSource)));
    downloads.forEach(download -> waitingFor.add(download.done()));
    var done = CompletableFutures.joinFutures(waitingFor);
    if (!done.isDone()) {
      LogUtil.runInStage(stage, () -> ProgressLoggers.awaitSourceProgress(rows, done, config.logInterval()));
    }
    CompletableFutures.await(done);
  }

  /** Returns the downloads of the {@code .osm.pbf} file, if it needs downloading. */
  private List<Download> osmDownloads() {
    return downloadsBySource.values().stream().flatMap(List::stream)
      .filter(download -> download.output().equals(osmInputFile.getPath()))
      .toList();
  }

  /** Returns the size of the {@code .osm.pbf} file, which may still be downloading. */
  private long osmSizeBytes() {
    long downloadSize = osmDownloads().stream()
      .mapToLong(download -> download.whenSizeKnown().join())
      .sum();
    // an up-to-date file that didn't need downloading after all reports 0
    return downloadSize > 0 ? downloadSize : osmInputFile.diskUsageBytes();
  }

  private Path getPath(String name, String type, Path defaultPath, String defaultUrl) {
    var source = sourceArgs(name, type, defaultPath, defaultUrl);
    boolean freeAfterReading = sourceArgs.getBoolean(
      "free_" + name + "_after_read",
      "delete " + name + " input file after reading to make space for output (reduces peak disk usage)",
      false);
    addDownload(source);
    inputPaths.add(new InputPath(name, source.path(), freeAfterReading, false));
    return source.path();
  }

  /** Reads the arguments of a source with a local path and a download URL, each once so each gets logged once. */
  private SourceArgs sourceArgs(String name, String type, Path defaultPath, String defaultUrl) {
    Path path = sourceArgs.file(name + "_path", name + " " + type + " path", defaultPath);
    registerSource(name, defaultPath, defaultUrl == null ? "" : defaultUrl.startsWith("http") ? host(defaultUrl) :
      defaultUrl);
    boolean refresh = refreshSource(name);
    DownloadMode mode = downloadMode(name);
    String url = mode.enabled() || refresh ?
      sourceArgs.getString(name + "_url", name + " " + type + " url", defaultUrl) : null;
    return new SourceArgs(name, path, url, mode, refresh);
  }

  /** Queues {@code source} for a full download if it is incomplete or needs a refresh. */
  private void addDownload(SourceArgs source) {
    if (source.url() != null && needsDownload(source.path(), source.refresh())) {
      toDownload.add(new ToDownload(source.name(), source.url(), source.path()));
    }
  }

  private static boolean needsDownload(Path path, boolean refresh) {
    return refresh || !Files.exists(path) || CachedRangeFile.isPartial(path);
  }

  /** Makes the map bounds fall back to those of the {@code .osm.pbf} when none were given. */
  private void addOsmBoundsFallback() {
    if (osmInputFile != null) {
      var bounds = config.bounds();
      if (!parseNodeBounds) {
        bounds.addFallbackProvider(this::osmHeaderBounds);
      }
      bounds.addFallbackProvider(() -> {
        // only a scan through the nodes can tell, which needs the whole file
        startPendingDownloads();
        awaitDownloads(OSM_BOUNDS_STAGE, osmDownloads());
        return new OsmNodeBoundsProvider(osmInputFile, config, stats).getLatLonBounds();
      });
    }
  }

  /** Returns the bounds in the header of the {@code .osm.pbf}, without waiting for it if it has to be downloaded. */
  private Envelope osmHeaderBounds() {
    Path path = osmInputFile.getPath();
    for (int i = 0; i < toDownload.size(); i++) {
      ToDownload download = toDownload.get(i);
      if (download.path.equals(path) && profile.caresAboutSource(download.id)) {
        // The header is in the first few KB of the file, so read just that from the server and let everything that
        // needs the bounds start while the rest downloads.
        String url = downloader().resolve(download.url);
        // remember the resolved URL so the download doesn't look up a geofabrik: or aws: shortcut a second time
        toDownload.set(i, new ToDownload(download.id, url, path));
        try {
          byte[] prefix =
            downloader().fetchRange(url, new Downloader.ByteRange(0, OsmInputFile.FILE_PREFIX_BYTES)).bytes();
          return OsmInputFile.parseHeader(prefix).map(OsmHeader::bounds).orElse(null);
        } catch (IOException e) {
          // If the server doesn't support range requests, for example, read the header once the whole file is here.
          LOGGER.debug("Unable to read the header of {} before downloading it", url, e);
          startPendingDownloads();
          awaitDownloads(OSM_BOUNDS_STAGE, osmDownloads());
          return osmInputFile.getLatLonBounds();
        }
      }
    }
    return osmInputFile.getLatLonBounds();
  }

  private Path sourceInfoPath() {
    return featureDbPath.resolve(featureDbPath.getFileName() + ".sources");
  }

  // a reused feature DB skips reading the sources, so save what else reading them determined next to it
  private void saveSourceInfo() {
    var bounds = config.bounds();
    var latLon = bounds.latLon();
    var info = new SourceInfo(bounds.isWorld() ? null :
      List.of(latLon.getMinX(), latLon.getMinY(), latLon.getMaxX(), latLon.getMaxY()), sourceMetadata);
    try {
      JSON.writeValue(sourceInfoPath().toFile(), info);
    } catch (IOException e) {
      throw new UncheckedIOException("Unable to save source info to " + sourceInfoPath(), e);
    }
  }

  private void restoreSourceInfo() {
    Path path = sourceInfoPath();
    SourceInfo info;
    try {
      info = JSON.readValue(path.toFile(), SourceInfo.class);
    } catch (IOException e) {
      throw new UncheckedIOException("Unable to load source info from " + path, e);
    }
    sourceMetadata.putAll(info.metadata());
    var bounds = info.bounds();
    if (bounds != null) {
      config.bounds()
        .addFallbackProvider(() -> new Envelope(bounds.get(0), bounds.get(2), bounds.get(1), bounds.get(3)));
    }
  }

  private void setSourceMetadata(String key, Object value) {
    if (value != null) {
      sourceMetadata.put(key, value.toString());
    }
  }

  private void ensureInputFilesExist() {
    Set<Path> queued = toDownload.stream().map(ToDownload::path).collect(Collectors.toSet());
    for (InputPath inputPath : inputPaths) {
      if (queued.contains(inputPath.path)) {
        // about to be downloaded
      } else if (profile.caresAboutSource(inputPath.id) && !Files.exists(inputPath.path)) {
        throw new IllegalArgumentException(inputPath.path + " does not exist. Run with --download to fetch it");
      } else if (profile.caresAboutSource(inputPath.id) && !inputPath.readsPartial &&
        CachedRangeFile.isPartial(inputPath.path)) {
        throw new IllegalArgumentException(
          inputPath.path + " is only partially downloaded. Run with --download to finish downloading it");
      }
    }
  }

  private sealed interface Stage {
    String id();

    List<String> details();
  }

  /** A stage that runs on its own after everything before it has finished, like reading OpenStreetMap data. */
  private record SequentialStage(String id, List<String> details, RunnableThatThrows task) implements Stage {}

  /**
   * A source that can be combined with other adjacent parallel stages when {@link #readSourcesTogether()} is enabled.
   */
  private record ParallelStage(String id, List<String> details, boolean fetchesWhileProcessing,
    boolean waitsForPrevious, Supplier<SourceReader> open) implements Stage {}

  /** A file to download in full before the source {@code id} reads it. */
  private record ToDownload(String id, String url, Path path) {}

  /** Metadata possibly read from OSM input files so we can save/restore as json when reuse_feature_db=true */
  private record SourceInfo(List<Double> bounds, Map<String, String> metadata) {}

  /** An input file; {@code readsPartial} if its reader can read a partially downloaded file. */
  private record InputPath(String id, Path path, boolean freeAfterReading, boolean readsPartial) {}

  private static SourceReader withDownloadHint(String url, SourceReader source) {
    return url == null || !url.endsWith(".parquet") ? source : new WithDownloadHint(source, url);
  }

  /** Adds where to download a valid file to the error when a parquet source turns out not to be one. */
  private record WithDownloadHint(SourceReader source, String url) implements SourceReader {

    @Override
    public String name() {
      return source.name();
    }

    @Override
    public CompletableFuture<Void> whenReady() {
      return source.whenReady().exceptionallyCompose(e -> CompletableFuture.failedFuture(
        Exceptions.unwrap(e) instanceof FileFormatException formatError ? withHint(formatError) : e));
    }

    @Override
    public void read(Consumer<Iterable<? extends SourceFeature>> next) throws Exception {
      try {
        source.read(next);
      } catch (FileFormatException e) {
        throw withHint(e);
      }
    }

    @Override
    public SourceProgress progress() {
      return source.progress();
    }

    @Override
    public void close() throws IOException {
      source.close();
    }

    private FileFormatException withHint(FileFormatException e) {
      return new FileFormatException(e.getMessage() + ", download it from " + url);
    }
  }

  private record SourceArgs(String name, Path path, String url, DownloadMode mode, boolean refresh) {}

  private void registerSource(String name, Path defaultPath, String downloadedFrom) {
    sourcesForHelp.putIfAbsent(name, new Source(name, defaultPath, downloadedFrom));
  }

  private static String host(String url) {
    try {
      String host = new URI(url).getHost();
      return host == null ? url : host;
    } catch (URISyntaxException _) {
      // only shown in --help, so it must never fail a run
      return url;
    }
  }

  /** Returns the text {@code --help} prints: the profile, examples, its options, sources, then core's options. */
  String help(boolean all) {
    String description = profile() == null ? null : profile().description();
    var page = HelpPage.create()
      .line(ArgumentHelp.title())
      .line()
      .line(profileName() + (description == null ? "" : ": " + description));
    if (areaSourceName != null) {
      String pathOption = "--" + areaSourceName.replace('_', '-') + "-path";
      page.section("Build a region")
        .planetilerCommand("--area=monaco --output=monaco.pmtiles")
        .planetilerCommand(pathOption + "=my-region.osm.pbf --output=my-region.pmtiles");
    }
    return page.line()
      .usage("[options]")
      .append(ArgumentHelp.options(
        arguments.options(),
        profileName() + " options",
        Map.of(ArgumentGroup.INPUT, sourcesHelp(sourcesForHelp.values())),
        all))
      .render();
  }

  private record Source(String name, Path defaultPath, String downloadedFrom) {}

  private static HelpPage sourcesHelp(Collection<Source> sources) {
    var page = HelpPage.create();
    if (sources.isEmpty()) {
      return page;
    }
    page.line().row("Sources", "default path", "downloaded from");
    for (var source : sources) {
      page.row("  " + source.name(), source.defaultPath().toString(), source.downloadedFrom());
    }
    return page.line()
      .line("  Each source can also be set on its own:")
      .table(4)
      .row("--<source>-path=<file>", "local file")
      .row("--<source>-url=<url>", "where to download it")
      .row("--refresh-<source>", "re-download if changed")
      .row("--download-<source>=<mode>", "override --download for a single source")
      .row("--free-<source>-after-read", "delete the file after reading it, to save disk");
  }

  private String profileName() {
    return profile() == null ? "Planetiler" : profile().name();
  }

  /** An exception that occurs while running planetiler. */
  public static class PlanetilerException extends RuntimeException {
    public PlanetilerException(String message, Exception e) {
      super(message, e);
    }

    /** Creates an exception with {@code message} and a {@code cause} that may also be an {@link Error}. */
    public PlanetilerException(String message, Throwable cause) {
      super(message, cause);
    }

    public PlanetilerException(Exception e) {
      super(e);
    }
  }

  /** Thrown by {@link #run()} when it fails for a reason the user can fix, with a message saying how. */
  public static class ExpectedFailureException extends PlanetilerException {
    public ExpectedFailureException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
