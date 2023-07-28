package com.onthegomap.planetiler.reader;

import blue.strategic.parquet.Hydrator;
import blue.strategic.parquet.HydratorSupplier;
import blue.strategic.parquet.ParquetReader;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.Downloader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OvertureReader extends SimpleReader<SimpleFeature> {

  private static final Logger LOGGER = LoggerFactory.getLogger(OvertureReader.class);
  //  private static final Configuration conf = new Configuration();
  //
  //  static {
  //    //    conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider");
  //    //    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
  //    //    conf.setBoolean("fs.s3a.path.style.access", true);
  //    conf.setBoolean(org.apache.parquet.avro.AvroReadSupport.READ_INT96_AS_FIXED, true);
  //  }

  private final Stream<Map<String, Object>> reader;
  private final String layer;
  private final long count;
  private static final PlanetilerConfig config = PlanetilerConfig.defaults();

  private static final Hydrator<Map<String, Object>, Map<String, Object>> hydrator = new Hydrator<>() {
    @Override
    public Map<String, Object> start() {
      return new HashMap<>();
    }

    @Override
    public HashMap<String, Object> add(Map<String, Object> target, String heading, Object value) {
      HashMap<String, Object> r = new HashMap<>(target);
      r.put(heading, value);
      return r;
    }

    @Override
    public Map<String, Object> finish(Map<String, Object> target) {
      return target;
    }
  };
  private final Iterator<Map<String, Object>> iter;

  OvertureReader(String sourceName, Path input) {
    super(sourceName);
    try {
      String url = input.toString().replaceAll("^.*theme=",
        "https://overturemaps-us-west-2.s3.amazonaws.com/release/2023-07-26-alpha.0/theme=");
      var size = Downloader.getUrlConnection(url, config).getContentLengthLong();
      var file = new InputFile() {
        @Override
        public long getLength() {
          return size;
        }

        @Override
        public SeekableInputStream newStream() throws IOException {
          return new SeekableHttpInputStream(url, config, size);
        }
      };

      //      var metadata = ParquetReader.readMetadata(file);
      //      this.count = metadata.getBlocks().stream().mapToLong(BlockMetaData::getRowCount).sum();
      this.count = 0;
      this.reader = ParquetReader.streamContent(input.toFile(), HydratorSupplier.constantly(hydrator),
        List.of("geometry"));
      this.iter = reader.iterator();
      var pattern = Pattern.compile("theme=([a-zA-Z]+)/type=([a-zA-Z]+)");
      var results = pattern.matcher(input.toString());
      if (!results.find()) {
        throw new UncheckedIOException(new IOException("Bad filename: " + input));
      }
      this.layer = results.group(1) + "/" + results.group(2);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Renders map features for all elements from a Natural Earth sqlite file, or zip file containing a sqlite file, based
   * on the mapping logic defined in {@code profile}.
   *
   * @param sourceName string ID for this reader to use in logs and stats
   * @param sourcePath path to the sqlite or zip file
   * @param writer     consumer for rendered features
   * @param config     user-defined parameters controlling number of threads and log interval
   * @param profile    logic that defines what map features to emit for each source feature
   * @param stats      to keep track of counters and timings
   * @throws IllegalArgumentException if a problem occurs reading the input file
   */
  public static void process(String sourceName, List<Path> sourcePath, FeatureGroup writer,
    PlanetilerConfig config, Profile profile, Stats stats) {
    SourceFeatureProcessor.processFiles(
      sourceName,
      sourcePath,
      path -> new OvertureReader(sourceName, path),
      writer, config, profile, stats
    );
  }

  @Override
  public long getFeatureCount() {
    return count;
  }

  @Override
  public void readFeatures(Consumer<SimpleFeature> next) throws Exception {
    //    long id = 0;
    while (iter.hasNext()) {
      Map<String, Object> map = iter.next();
      //      System.err.println(map);
      //      ByteBuffer buf = (ByteBuffer) r.get("geometry");
      //      byte[] bytes = new byte[buf.limit()];
      //      buf.get(bytes);
      //      Map<String, Object> tags = new HashMap<>();
      //
      //      for (var field : r.getSchema().getFields()) {
      //        if (!"geometry".equals(field.name())) {
      //          var value = r.get(field.pos());
      //          if (value != null) {
      //            //            System.err.println(field.name() + " " + value.getClass());
      //            tags.put(field.name(), value.toString());
      //          }
      //        }
      //      }

      //      var geometry = new WKBReader().read(bytes);
      //      var feature = SimpleFeature.create(geometry, tags, sourceName, layer, id++);
      //      next.accept(feature);
    }
  }

  @Override
  public void close() {
    reader.close();
  }
}
