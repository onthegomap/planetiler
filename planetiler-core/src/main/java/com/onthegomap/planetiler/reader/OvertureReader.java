package com.onthegomap.planetiler.reader;

import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.stats.Stats;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.locationtech.jts.io.WKBReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OvertureReader extends SimpleReader<SimpleFeature> {

  private static final Logger LOGGER = LoggerFactory.getLogger(OvertureReader.class);
  private static final Configuration conf = new Configuration();

  static {
    //    conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider");
    //    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    //    conf.setBoolean("fs.s3a.path.style.access", true);
    conf.setBoolean(org.apache.parquet.avro.AvroReadSupport.READ_INT96_AS_FIXED, true);
  }

  private final ParquetReader<GenericRecord> reader;
  private final String layer;
  private final long count;

  OvertureReader(String sourceName, Path input) {
    super(sourceName);
    try {
      InputFile file = HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(input.toString()), conf);
      try (var fr = ParquetFileReader.open(file)) {
        this.count = fr.getRecordCount();
      }
      reader = AvroParquetReader.genericRecordReader(file);
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
    GenericRecord r;
    long id = 0;
    while ((r = reader.read()) != null) {
      ByteBuffer buf = (ByteBuffer) r.get("geometry");
      byte[] bytes = new byte[buf.limit()];
      buf.get(bytes);
      Map<String, Object> tags = new HashMap<>();

      for (var field : r.getSchema().getFields()) {
        if (!"geometry".equals(field.name())) {
          var value = r.get(field.pos());
          if (value != null) {
            //            System.err.println(field.name() + " " + value.getClass());
            tags.put(field.name(), value.toString());
          }
        }
      }

      var geometry = new WKBReader().read(bytes);
      var feature = SimpleFeature.create(geometry, tags, sourceName, layer, id++);
      next.accept(feature);
    }
  }

  @Override
  public void close() {
    try {
      reader.close();
    } catch (IOException e) {
      LOGGER.error("Error closing parquet file", e);
    }
  }
}
