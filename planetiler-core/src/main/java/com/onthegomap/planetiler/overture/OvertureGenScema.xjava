package com.onthegomap.planetiler.overture;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.onthegomap.planetiler.util.FileUtils;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;

public class OvertureGenScema {

  public static void main(String[] args) throws IOException {
    Set<String> done = new HashSet<>();

    var avroRoot = Path.of("planetiler-core/src/main/avro");
    FileUtils.createDirectory(avroRoot);
    for (var s : OverturePaths.files) {
      Configuration conf = new Configuration();
      conf.set(Constants.ENDPOINT, "https://s3.us-west-2.amazonaws.com/");
      conf.set(Constants.AWS_CREDENTIALS_PROVIDER,
        AnonymousAWSCredentialsProvider.class.getName());
      conf.setBoolean(org.apache.parquet.avro.AvroReadSupport.READ_INT96_AS_FIXED, true);
      InputFile file = HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(URI.create(
        "s3a://overturemaps-us-west-2/release/2023-07-26-alpha.0/" + s)), conf);
      var pattern = Pattern.compile("type=([a-zA-Z]+)");
      var matcher = pattern.matcher(s.toString());
      if (matcher.find()) {
        String name = matcher.group(1);
        if (!done.contains(name)) {
          try (var fr = ParquetFileReader.open(file)) {
            var a = fr.getFooter().getFileMetaData().getSchema();
            var schema = new AvroSchemaConverter(conf).convert(a);
            Files.writeString(avroRoot.resolve(name + ".avsc"),
              schema.toString().replace("hive_schema", "com.onthegomap.planetiler.overture.avrogen." +
                PropertyNamingStrategies.UpperCamelCaseStrategy.INSTANCE.translate(name)));
          }
          done.add(name);
        }
      }
    }
  }
}
