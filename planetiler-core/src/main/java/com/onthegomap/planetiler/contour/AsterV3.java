package com.onthegomap.planetiler.contour;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureMerge;
import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.collection.SortableFeature;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.ShapefileReader;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.render.FeatureRenderer;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.FileUtils;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.gdal.gdal.gdal;
import org.gdal.ogr.FieldDefn;
import org.gdal.ogr.ogr;
import org.gdal.ogr.ogrConstants;
import org.gdal.osr.SpatialReference;
import org.geotools.geometry.jts.GeometryClipper;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsterV3 implements Profile {
  private static final String ELE_KEY = "ele";
  private static final Logger LOGGER = LoggerFactory.getLogger(AsterV3.class);

  private final PlanetilerConfig config;
  private final Stats stats;
  private final int downloadThreads;
  private final int processThreads;
  private final Planetiler planetiler;
  private final int writeThreads;
  private final Unit unit;
  private final TreeMap<Integer, Integer> levels;
  private final int gcd;
  private final String attribute;
  private final String layer;
  private final double smoothness;

  enum Unit {
    FEET(0.3048),
    METERS(1);

    final double meters;

    Unit(double meters) {
      this.meters = meters;
    }
  }

  public AsterV3(PlanetilerConfig config, Stats stats, Planetiler planetiler) {
    gdal.AllRegister();
    this.stats = stats;
    this.config = config;
    this.downloadThreads = config.downloadThreads();
    this.processThreads = config.featureProcessThreads();
    this.writeThreads = config.featureWriteThreads();
    this.planetiler = planetiler;
    layer = config.arguments().getString("layer", "output layer name", "contours");
    attribute = config.arguments().getString("attribute", "output elevation attribute name", "ele");
    String unitString = config.arguments().getString("unit", "unit (feet or meters)", "meters");
    this.unit = Unit.valueOf(unitString.toUpperCase(Locale.ROOT));
    String levelsString = config.arguments().getString("levels", "zoom", "10:200,11:100,12:50,13:20,14:10");
    this.smoothness = config.arguments().getDouble("smoothness", "amount to smooth (0=tight 1=loose)", 0.5);
    levels = new TreeMap<>();
    for (var level : levelsString.split(",")) {
      String[] split = level.split(":");
      if (split.length != 2) {
        throw new IllegalArgumentException(
          "Bad levels " + levelsString + " expected zoom:threshold,zoom:threshold,...");
      }
      try {
        levels.put(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
          "Bad levels " + levelsString + " expected zoom:threshold,zoom:threshold,...");
      }
    }
    this.gcd = gcd(levels.values());
    if (gcd < levels.values().stream().mapToInt(d -> d).min().orElseThrow()) {
      LOGGER.warn("levels {} require generating contours every {} {}", levels, gcd,
        unit.name().toLowerCase(Locale.ROOT));
    }
  }

  private static int gcd(Collection<Integer> ints) {
    return ints.stream().reduce(AsterV3::gcd).orElseThrow();
  }

  private static int gcd(int a, int b) {
    return b == 0 ? a : gcd(b, a % b);
  }

  public static void main(String[] args) throws Exception {
    Arguments arguments = Arguments.fromArgsOrConfigFile(args).orElse(Arguments.of(
      "bounds", "-73.6518,41.1378,-69.6508,42.9775",
      "download-threads", "10"
    ));
    var planetiler = Planetiler.create(arguments);
    var aster = new AsterV3(planetiler.config(), planetiler.stats(), planetiler);

    planetiler
      .setProfile(aster)
      .addStage("aster", "Download and process Aster V3 geotiff's", aster::readAster)
      .overwriteOutput(Path.of("data", "contours.mbtiles"))
      .run();
  }

  @Override
  public List<VectorTile.Feature> postProcessLayerFeatures(String layer, int zoom, List<VectorTile.Feature> items)
    throws GeometryException {
    return FeatureMerge.mergeLineStrings(
      items,
      s -> config.minFeatureSize(zoom),
      config.tolerance(zoom),
      4,
      true,
      smoothness
    );
  }

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
    Coordinate coord = (Coordinate) sourceFeature.getTag("coord");
    try {
      Geometry line = sourceFeature.latLonGeometry();
      Geometry clipped = null;
      int minzoom = -1, maxzoom = -1;
      double eleMeters = sourceFeature.getDouble(ELE_KEY);
      long ele = (long) Math.rint(eleMeters / unit.meters);
      for (int zoom : levels.navigableKeySet()) {
        int level = levels.get(zoom);
        if (ele % level == 0) {
          minzoom = minzoom < 0 ? zoom : minzoom;
          maxzoom = levels.higherKey(zoom) == null ? config.maxzoom() : (levels.higherKey(zoom) - 1);
        } else {
          if (minzoom >= 0) {
            clipped = clipped != null ? clipped : GeoUtils.latLonToWorldCoords(coord.clip(line));
            if (!clipped.isEmpty()) {
              features.geometry(layer, clipped)
                .setZoomRange(minzoom, maxzoom)
                .setAttr(attribute, sourceFeature.getLong(ELE_KEY));
            }
          }
          minzoom = maxzoom = -1;
        }
        if (minzoom >= 0) {
          clipped = clipped != null ? clipped : GeoUtils.latLonToWorldCoords(coord.clip(line));
          if (!clipped.isEmpty()) {
            features.geometry(layer, clipped)
              .setZoomRange(minzoom, maxzoom)
              .setAttr(attribute, sourceFeature.getLong(ELE_KEY));
          }
        }
      }
    } catch (GeometryException e) {
      e.log(stats, "line", "line");
    }
  }

  @Override
  public String name() {
    return "Aster V3 Contour Lines";
  }

  @Override
  public String description() {
    return "Contour lines generated from https://asterweb.jpl.nasa.gov/gdem.asp";
  }

  @Override
  public boolean isOverlay() {
    return true;
  }

  private List<Coordinate> enumerate() {
    List<Coordinate> result = new ArrayList<>();
    var bounds = config.bounds().latLon();
    for (int x = (int) Math.floor(bounds.getMinX()); x < (int) Math.ceil(bounds.getMaxX()); x++) {
      for (int y = (int) Math.floor(bounds.getMinY()); y < (int) Math.ceil(bounds.getMaxY()); y++) {
        result.add(new Coordinate(x, y, planetiler.tmpDir));
      }
    }
    return result;
  }

  private void readAster() {
    var timer = stats.startStage("aster");
    var writer = planetiler.featureGroup;
    var todo = enumerate();
    AtomicLong tilesDownloaded = new AtomicLong(0);
    AtomicLong tilesProcessed = new AtomicLong(0);
    AtomicLong featuresWritten = new AtomicLong(0);
    var pipeline = WorkerPipeline.start("aster", stats)
      .readFromTiny("tiles", todo)
      .<Coordinate>addWorker("download", downloadThreads, (prev, next) -> {
        for (var item : prev) {
          try {
            download(item);
            next.accept(item);
          } catch (FileNotFoundException e) {
            // probably an ocean
            FileUtils.deleteDirectory(item.dir());
            tilesProcessed.incrementAndGet();
          } finally {
            tilesDownloaded.incrementAndGet();
          }
        }
      }).addBuffer("process_queue", 1_000, 1)
      .<SortableFeature>addWorker("process", processThreads, (prev, next) -> {
        var featureCollectors = new FeatureCollector.Factory(config, stats);
        try (FeatureRenderer renderer = newFeatureRenderer(writer, config, next)) {
          for (var coord : prev) {
            extractFromZip(coord);
            generateContours(coord);

            try (var reader = new ShapefileReader(null, "aster" + coord.filename(), coord.contourPath())) {
              reader.readFeatures(sourceFeature -> {
                sourceFeature.setTag("coord", coord);
                FeatureCollector features = featureCollectors.get(sourceFeature);
                try {
                  processFeature(sourceFeature, features);
                  for (FeatureCollector.Feature renderable : features) {
                    renderer.accept(renderable);
                  }
                } catch (Exception e) {
                  LOGGER.error("Error processing " + sourceFeature, e);
                }
              });
              tilesProcessed.incrementAndGet();
            } finally {
              FileUtils.deleteDirectory(coord.dir());
            }
          }
        }
      })
      .addBuffer("write_queue", 50_000, 1_000)
      .sinkTo("write", writeThreads, prev -> {
        try (var threadLocalWriter = writer.writerForThread()) {
          for (var item : prev) {
            threadLocalWriter.accept(item);
            featuresWritten.incrementAndGet();
          }
        }
      });

    var loggers = ProgressLoggers.create()
      .addRatePercentCounter("downloaded", todo.size(), tilesDownloaded, false)
      .addRatePercentCounter("processed", todo.size(), tilesProcessed, false)
      .addRateCounter("write", featuresWritten)
      .addFileSize(writer)
      .newLine()
      .addProcessStats()
      .newLine()
      .addPipelineStats(pipeline);

    pipeline.awaitAndLog(loggers, config.logInterval());
    timer.stop();
  }

  private void extractFromZip(Coordinate coord) throws IOException {
    try (
      var zipFs = FileSystems.newFileSystem(coord.downloadPath());
      var innerZipFs = FileSystems.newFileSystem(zipFs.getPath("ASTGTMV003_" + coord.filename() + ".zip"));
    ) {
      var path = innerZipFs.getPath("ASTGTMV003_" + coord.filename() + "_dem.tif");
      Files.copy(path, coord.demPath());
    }
  }

  private void generateContours(Coordinate coord) {
    var dest = coord.contourPath().toString();
    var dataset = gdal.Open(coord.demPath().toString(), 0 /* read-only */);
    if (dataset == null) {
      throw new IllegalStateException("GDALOpen failed - " + gdal.GetLastErrorNo() + ": " + gdal.GetLastErrorMsg());
    }
    var band = dataset.GetRasterBand(1);
    Vector<String> options = new Vector<>();
    var driver = ogr.GetDriverByName("ESRI Shapefile");
    var out = driver.CreateDataSource(dest);
    var layer = out.CreateLayer("ele", new SpatialReference(dataset.GetProjection()), ogr.wkbLineString, null);
    var field = new FieldDefn("ele", ogrConstants.OFTReal);
    field.SetWidth(12);
    field.SetPrecision(3);
    layer.CreateField(field, 0);

    options.add("LEVEL_BASE=0");
    options.add("LEVEL_INTERVAL=" + (gcd * unit.meters));
    options.add("POLYGONIZE=NO");
    options.add("ELEV_FIELD=" + ELE_KEY);
    options.add("NODATA=-32768");

    gdal.ContourGenerateEx(band, layer, options);

    dataset.FlushCache();
    dataset.delete();
    out.FlushCache();
  }

  private FeatureRenderer newFeatureRenderer(FeatureGroup writer, PlanetilerConfig config,
    Consumer<SortableFeature> next) {
    @SuppressWarnings("java:S2095") // closed by FeatureRenderer
    var encoder = writer.newRenderedFeatureEncoder();
    return new FeatureRenderer(
      config,
      rendered -> next.accept(encoder.apply(rendered)),
      stats,
      encoder
    );
  }

  private void download(Coordinate prev) throws IOException {
    var filename = prev.filename();
    var dest = prev.downloadPath();
    FileUtils.createParentDirectories(dest);
    var url = new URL("https://gdemdl.aster.jspacesystems.or.jp/download/Download_" + filename + ".zip");
    try (
      var in = Channels.newChannel(url.openStream());
      var out = FileChannel.open(dest, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    ) {
      out.transferFrom(in, 0, Long.MAX_VALUE);
    }
  }

  record Coordinate(int lon, int lat, Path tmpDir, Envelope envelope) {
    Coordinate(int lon, int lat, Path tmpDir) {
      this(lon, lat, tmpDir, new Envelope(lon, lon + 1d, lat, lat + 1d));
    }

    public String filename() {
      String latString = lat >= 0 ? "N%02d".formatted(lat) : "S%02d".formatted(-lat);
      String lonString = lon >= 0 ? "E%03d".formatted(lon) : "W%03d".formatted(-lon);
      return latString + lonString;
    }

    public Path downloadPath() {
      return dir().resolve("dem.zip");
    }

    public Path demPath() {
      return dir().resolve("dem.tiff");
    }

    public Path contourPath() {
      return dir().resolve("contour.shp");
    }

    @Override
    public String toString() {
      return filename();
    }

    public Path dir() {
      return tmpDir.resolve("aster").resolve(filename());
    }

    public Geometry clip(Geometry line) {
      return new GeometryClipper(envelope).clip(line, false);
    }
  }
}
