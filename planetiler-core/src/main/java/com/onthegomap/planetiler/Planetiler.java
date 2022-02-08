package com.onthegomap.planetiler;

import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.collection.LongLongMap;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.MbtilesMetadata;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.mbtiles.MbtilesWriter;
import com.onthegomap.planetiler.reader.NaturalEarthReader;
import com.onthegomap.planetiler.reader.ShapefileReader;
import com.onthegomap.planetiler.reader.osm.OsmInputFile;
import com.onthegomap.planetiler.reader.osm.OsmReader;
import com.onthegomap.planetiler.stats.ProcessInfo;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.stats.Timers;
import com.onthegomap.planetiler.util.Downloader;
import com.onthegomap.planetiler.util.FileUtils;
import com.onthegomap.planetiler.util.Format;
import com.onthegomap.planetiler.util.Geofabrik;
import com.onthegomap.planetiler.util.LogUtil;
import com.onthegomap.planetiler.util.Translations;
import com.onthegomap.planetiler.util.Wikidata;
import com.onthegomap.planetiler.worker.RunnableThatThrows;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * High-level API for creating a new map that ties together lower-level utilities in a way that is suitable for the most
 * common use-cases.
 * <p>
 * For example:
 * <pre><code>
 * public static void main(String[] args) {
 *   Planetiler.create(arguments)
 *     .setProfile(new CustomProfile())
 *     .addShapefileSource("shapefile", Path.of("shapefile.zip"))
 *     .addNaturalEarthSource("natural_earth", Path.of("natural_earth.zip"))
 *     .addOsmSource("osm", Path.of("source.osm.pbf"))
 *     .setOutput("mbtiles", Path.of("output.mbtiles"))
 *     .run();
 * }</code></pre>
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
  private final boolean downloadSources;
  private final boolean onlyDownloadSources;
  private Profile profile = null;
  private Function<Planetiler, Profile> profileProvider = null;
  private final PlanetilerConfig config;
  private FeatureGroup featureGroup;
  private OsmInputFile osmInputFile;
  private Path output;
  private boolean overwrite = false;
  private boolean ran = false;
  private Path nodeDbPath;
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

  private Planetiler(Arguments arguments) {
    this.arguments = arguments;
    stats = arguments.getStats();
    overallTimer = stats.startStage("overall");
    LogUtil.clearStage();
    config = PlanetilerConfig.from(arguments);
    tmpDir = arguments.file("tmpdir", "temp directory", Path.of("data", "tmp"));
    onlyDownloadSources = arguments.getBoolean("only_download", "download source data then exit", false);
    downloadSources = onlyDownloadSources || arguments.getBoolean("download", "download sources", false);
  }

  /** Returns a new empty runner that will get configuration from {@code arguments}. */
  public static Planetiler create(Arguments arguments) {
    return new Planetiler(arguments);
  }

  /**
   * Returns a new empty runner that will get configuration from {@code arguments} to the main method, JVM properties,
   * environmental variables, or a config file specified in {@code config} argument.
   *
   * @param arguments array of string arguments provided to {@code public static void main(String[] args)} entrypoint
   */
  public static Planetiler create(String... arguments) {
    return new Planetiler(Arguments.fromArgsOrConfigFile(arguments));
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
   * @param defaultUrl  remote URL that the file to download if {@code download=true} argument is set and {@code
   *                    name_url} argument is not set.  As a shortcut, can use "geofabrik:monaco" or
   *                    "geofabrik:australia" shorthand to find an extract by name from <a
   *                    href="https://download.geofabrik.de/">Geofabrik download site</a> or "aws:latest" to download
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
    var thisInputFile = new OsmInputFile(path);
    osmInputFile = thisInputFile;
    return appendStage(new Stage(
      name,
      List.of(
        name + "_pass1: Pre-process OpenStreetMap input (store node locations then relation members)",
        name + "_pass2: Process OpenStreetMap nodes, ways, then relations"
      ),
      ifSourceUsed(name, () -> {
        try (
          var nodeLocations = LongLongMap.from(config.nodeMapType(), config.nodeMapStorage(), nodeDbPath);
          var osmReader = new OsmReader(name, thisInputFile, nodeLocations, profile(), stats)
        ) {
          osmReader.pass1(config);
          osmReader.pass2(featureGroup, config);
        } finally {
          FileUtils.delete(nodeDbPath);
        }
      }))
    );
  }

  /**
   * Adds a new ESRI shapefile source that will be processed using a projection inferred from the shapefile when {@link
   * #run()} is called.
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
   * Adds a new ESRI shapefile source that will be processed using an explicit projection when {@link #run()} is
   * called.
   * <p>
   * To override the location of the {@code shapefile} file, set {@code name_path=newpath.shp.zip} in the arguments.
   *
   * @param projection  the Coordinate Reference System authority code to use, parsed with {@link
   *                    org.geotools.referencing.CRS#decode(String)}
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
   * Adds a new ESRI shapefile source that will be processed with a projection inferred from the shapefile when {@link
   * #run()} is called.
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
   * @param defaultUrl  remote URL that the file to download if {@code download=true} argument is set and {@code
   *                    name_url} argument is not set
   * @return this runner instance for chaining
   * @see ShapefileReader
   * @see Downloader
   */
  public Planetiler addShapefileSource(String name, Path defaultPath, String defaultUrl) {
    return addShapefileSource(null, name, defaultPath, defaultUrl);
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
   * @param projection  the Coordinate Reference System authority code to use, parsed with {@link
   *                    org.geotools.referencing.CRS#decode(String)}
   * @param name        string to use in stats and logs to identify this stage
   * @param defaultPath path to the input file to use if {@code name_path} key is not set through arguments. Can be a
   *                    {@code .shp} file with other shapefile components in the same directory, or a {@code .zip} file
   *                    containing the shapefile components.
   * @param defaultUrl  remote URL that the file to download if {@code download=true} argument is set and {@code
   *                    name_url} argument is not set
   * @return this runner instance for chaining
   * @see ShapefileReader
   * @see Downloader
   */
  public Planetiler addShapefileSource(String projection, String name, Path defaultPath, String defaultUrl) {
    Path path = getPath(name, "shapefile", defaultPath, defaultUrl);
    return addStage(name, "Process features in " + path,
      ifSourceUsed(name,
        () -> ShapefileReader.processWithProjection(projection, name, path, featureGroup, config, profile, stats)));
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
   */
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
   * @param defaultUrl  remote URL that the file to download if {@code download=true} argument is set and {@code
   *                    name_url} argument is not set
   * @return this runner instance for chaining
   * @see NaturalEarthReader
   * @see Downloader
   */
  public Planetiler addNaturalEarthSource(String name, Path defaultPath, String defaultUrl) {
    Path path = getPath(name, "sqlite db", defaultPath, defaultUrl);
    return addStage(name, "Process features in " + path, ifSourceUsed(name, () -> NaturalEarthReader
      .process(name, path, tmpDir.resolve("natearth.sqlite"), featureGroup, config, profile, stats)));
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
   * Updates {@link #translations()} to use name translations fetched from wikidata based on the <a
   * href="https://www.wikidata.org/wiki/Wikidata:OpenStreetMap">wikidata tag</a> on OSM elements.
   * <p>
   * When either {@code only_fetch_wikidata} or {@code fetch_wikidata} arguments are set to true, this downloads
   * translations for every OSM element that the profile cares about and stores them to {@code defaultWikidataCache} (or
   * the value of the {@code wikidata_cache} argument) before processing any sources.
   * <p>
   * As long as {@code use_wikidata} is not set to false, then previously-downloaded wikidata translations will be
   * loaded from the cache file so you can run with {@code fetch_wikidata=true} once, then without it each subsequent
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
   * Sets the location of the output {@code .mbtiles} file to write rendered tiles to. Fails if the file already
   * exists.
   * <p>
   * To override the location of the file, set {@code argument=newpath.mbtiles} in the arguments.
   *
   * @param argument the argument key to check for an override to {@code fallback}
   * @param fallback the fallback value if {@code argument} is not set in arguments
   * @return this runner instance for chaining
   * @see MbtilesWriter
   */
  public Planetiler setOutput(String argument, Path fallback) {
    this.output = arguments.file(argument, "mbtiles output file", fallback);
    return this;
  }

  /**
   * Sets the location of the output {@code .mbtiles} file to write rendered tiles to. Overwrites file if it already
   * exists.
   * <p>
   * To override the location of the file, set {@code argument=newpath.mbtiles} in the arguments.
   *
   * @param argument the argument key to check for an override to {@code fallback}
   * @param fallback the fallback value if {@code argument} is not set in arguments
   * @return this runner instance for chaining
   * @see MbtilesWriter
   */
  public Planetiler overwriteOutput(String argument, Path fallback) {
    this.overwrite = true;
    return setOutput(argument, fallback);
  }

  /**
   * Reads all elements from all sourced that have been added, generates map features according to the profile, and
   * writes the rendered tiles to the output mbtiles file.
   *
   * @throws IllegalArgumentException if expected inputs have not been provided
   * @throws Exception                if an error occurs while processing
   */
  public void run() throws Exception {
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
    MbtilesMetadata mbtilesMetadata = new MbtilesMetadata(profile, config.arguments());

    if (arguments.getBoolean("help", "show arguments then exit", false)) {
      System.exit(0);
    } else if (onlyDownloadSources) {
      // don't check files if not generating map
    } else if (overwrite || config.force()) {
      FileUtils.deleteFile(output);
    } else if (Files.exists(output)) {
      throw new IllegalArgumentException(output + " already exists, use the --force argument to overwrite.");
    }

    LOGGER.info(
      "Building " + profile.getClass().getSimpleName() + " profile into " + output + " in these phases:");

    if (!toDownload.isEmpty()) {
      LOGGER.info("  download: Download sources " + toDownload.stream().map(d -> d.id).toList());
    }

    if (!onlyDownloadSources && fetchWikidata) {
      LOGGER.info("  wikidata: Fetch translations from wikidata query service");
    }

    if (!onlyDownloadSources && !onlyFetchWikidata) {
      for (Stage stage : stages) {
        for (String details : stage.details) {
          LOGGER.info("  " + details);
        }
      }
      LOGGER.info("  sort: Sort rendered features by tile ID");
      LOGGER.info("  mbtiles: Encode each tile and write to " + output);
    }

    if (!toDownload.isEmpty()) {
      download();
    }
    ensureInputFilesExist();
    Files.createDirectories(tmpDir);
    checkDiskSpace();
    checkMemory();
    if (onlyDownloadSources) {
      return; // exit only if just downloading
    }
    if (fetchWikidata) {
      Wikidata.fetch(osmInputFile(), wikidataNamesFile, config(), profile(), stats());
    }
    if (useWikidata) {
      translations().addTranslationProvider(Wikidata.load(wikidataNamesFile));
    }
    if (onlyFetchWikidata) {
      return; // exit only if just fetching wikidata
    }
    if (osmInputFile != null) {
      config.bounds().setFallbackProvider(osmInputFile);
    }

    Files.createDirectories(tmpDir);
    nodeDbPath = tmpDir.resolve("node.db");
    Path featureDbPath = tmpDir.resolve("feature.db");
    featureGroup = FeatureGroup.newDiskBackedFeatureGroup(featureDbPath, profile, config, stats);
    stats.monitorFile("nodes", nodeDbPath);
    stats.monitorFile("features", featureDbPath);
    stats.monitorFile("mbtiles", output);

    for (Stage stage : stages) {
      stage.task.run();
    }

    LOGGER.info("Deleting node.db to make room for mbtiles");
    profile.release();

    featureGroup.prepare();

    MbtilesWriter.writeOutput(featureGroup, output, mbtilesMetadata, config, stats);

    overallTimer.stop();
    LOGGER.info("FINISHED!");
    stats.printSummary();
    stats.close();
  }

  private void checkDiskSpace() {
    Map<FileStore, Long> bytesRequested = new HashMap<>();
    long osmSize = osmInputFile.diskUsageBytes();
    long nodeMapSize = LongLongMap.estimateDiskUsage(config.nodeMapType(), config.nodeMapStorage(), osmSize);
    long featureSize = profile.estimateIntermediateDiskBytes(osmSize);
    long outputSize = profile.estimateOutputBytes(osmSize);

    try {
      bytesRequested.merge(Files.getFileStore(tmpDir), nodeMapSize, Long::sum);
      bytesRequested.merge(Files.getFileStore(tmpDir), featureSize, Long::sum);
      bytesRequested.merge(Files.getFileStore(output.getParent()), outputSize, Long::sum);
      for (var entry : bytesRequested.entrySet()) {
        var fs = entry.getKey();
        var requested = entry.getValue();
        long available = fs.getUnallocatedSpace();
        if (available < requested) {
          var format = Format.defaultInstance();
          String warning =
            "Planetiler needs ~" + format.storage(requested) + " on " + fs + " which only has "
              + format.storage(available) + " available";
          if (config.force() || requested < available * 1.25) {
            LOGGER.warn(warning + ", may fail.");
          } else {
            throw new IllegalArgumentException(warning + ", use the --force argument to continue anyway.");
          }
        }
      }
    } catch (IOException e) {
      LOGGER.warn("Unable to check disk space requirements, may run out of room " + e);
    }

  }

  private void checkMemory() {
    var format = Format.defaultInstance();
    long nodeMap = LongLongMap.estimateMemoryUsage(config.nodeMapType(), config.nodeMapStorage(),
      osmInputFile.diskUsageBytes());
    long profile = profile().estimateRamRequired(osmInputFile.diskUsageBytes());
    long requested = nodeMap + profile;
    long jvmMemory = ProcessInfo.getMaxMemoryBytes();

    if (jvmMemory < requested) {
      String warning =
        "Planetiler needs ~" + format.storage(requested) + " memory for the JVM, but only "
          + format.storage(jvmMemory) + " is available";
      if (config.force() || requested < jvmMemory * 1.25) {
        LOGGER.warn(warning + ", may fail.");
      } else {
        throw new IllegalArgumentException(warning + ", use the --force argument to continue anyway.");
      }
    }

    long nodeMapBytes = LongLongMap.estimateDiskUsage(config.nodeMapType(), config.nodeMapStorage(),
      osmInputFile.diskUsageBytes());
    if (nodeMapBytes > 0
      && ManagementFactory.getOperatingSystemMXBean() instanceof com.sun.management.OperatingSystemMXBean os) {
      long systemMemory = os.getTotalMemorySize();
      long availableForDiskCache = systemMemory - jvmMemory;
      if (nodeMapBytes > availableForDiskCache) {
        LOGGER.warn(
          """
            Planetiler will store node locations in a %s memory-mapped file. It is recommended to have at least that
            much free RAM available on the system for the OS to cache the memory-mapped file, or else the import may
            slow down substantially. There is %s total memory available and the JVM will use %s which only leaves %s.
            You may want to reduce the -Xmx JVM setting, run on a system with more RAM, or increase -Xmx to at least
            %s and use --nodemap-storage=ram instead.
            """.formatted(
            format.storage(nodeMapBytes),
            format.storage(systemMemory),
            format.storage(jvmMemory),
            format.storage(systemMemory - jvmMemory),
            format.storage(jvmMemory + nodeMapBytes)
          ));
      }
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
        LogUtil.setStage(name);
        LOGGER.info("Skipping since profile does not use it");
        LogUtil.clearStage();
      }
    };
  }

  private Path getPath(String name, String type, Path defaultPath, String defaultUrl) {
    Path path = arguments.file(name + "_path", name + " " + type + " path", defaultPath);
    if (downloadSources) {
      String url = arguments.getString(name + "_url", name + " " + type + " url", defaultUrl);
      if (!Files.exists(path) && url != null) {
        toDownload.add(new ToDownload(name, url, path));
      }
    }
    inputPaths.add(new InputPath(name, path));
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

  private static record Stage(String id, List<String> details, RunnableThatThrows task) {

    Stage(String id, String description, RunnableThatThrows task) {
      this(id, List.of(id + ": " + description), task);
    }
  }

  private static record ToDownload(String id, String url, Path path) {}

  private static record InputPath(String id, Path path) {}
}
