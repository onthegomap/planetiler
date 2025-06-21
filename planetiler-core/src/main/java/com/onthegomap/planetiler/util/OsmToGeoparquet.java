package com.onthegomap.planetiler.util;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.collection.LongLongMap;
import com.onthegomap.planetiler.collection.LongLongMultimap;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmInputFile;
import com.onthegomap.planetiler.reader.osm.OsmReader;
import com.onthegomap.planetiler.reader.osm.OsmSourceFeature;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Locale;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

public class OsmToGeoparquet implements Profile, Closeable {
  private static MessageType SCHEMA =
    Types.buildMessage()
      .required(INT64)
      .named("id")
      .required(BINARY).as(LogicalTypeAnnotation.stringType())
      .named("geom")
      .map(Type.Repetition.REQUIRED)
      .key(BINARY).as(LogicalTypeAnnotation.stringType())
      .value(BINARY, Type.Repetition.REQUIRED).as(LogicalTypeAnnotation.stringType())
      .named("tags")
      .repeated(INT64).named("nodes")
      .named("root");

  //  TODO make encoded geometries
  private static MessageType NODE_SCHEMA =
    Types.buildMessage()
      .required(INT64)
      .named("id")
      .map(Type.Repetition.REQUIRED)
      .key(BINARY).as(LogicalTypeAnnotation.stringType())
      .value(BINARY, Type.Repetition.REQUIRED).as(LogicalTypeAnnotation.stringType())
      .named("tags")
      .required(BINARY).named("geometry")
      .named("root");
  private static MessageType WAY_SCHEMA =
    Types.buildMessage()
      .required(INT64)
      .named("id")
      .map(Type.Repetition.REQUIRED)
      .key(BINARY).as(LogicalTypeAnnotation.stringType())
      .value(BINARY, Type.Repetition.REQUIRED).as(LogicalTypeAnnotation.stringType())
      .named("tags")
      .repeated(INT64).named("nodes")
      .named("root");
  private static MessageType REL_SCHEMA =
    Types.buildMessage()
      .required(INT64)
      .named("id")
      .map(Type.Repetition.REQUIRED)
      .key(BINARY).as(LogicalTypeAnnotation.stringType())
      .value(BINARY, Type.Repetition.REQUIRED).as(LogicalTypeAnnotation.stringType())
      .named("tags")
      .repeatedGroup()
      .addField(Types.required(BINARY).as(LogicalTypeAnnotation.stringType()).named("role"))
      .addField(Types.required(BINARY).as(LogicalTypeAnnotation.stringType()).named("type"))
      .addField(Types.required(INT64).named("ref"))
      .named("members")
      .named("root");
  private final ParquetWriter<Group> nodeWriter;
  private final ParquetWriter<Group> wayWriter;
  private final ParquetWriter<Group> relWriter;

  public OsmToGeoparquet(Path file) throws IOException {
    FileUtils.deleteDirectory(file);
    FileUtils.createDirectory(file);
    this.nodeWriter = ExampleParquetWriter.builder(new LocalOutputFile(file.resolve("nodes.parquet")))
      .withType(NODE_SCHEMA)
      .withCompressionCodec(CompressionCodecName.ZSTD)
      .withRowGroupSize(100_000L)
      .build();
    this.wayWriter = ExampleParquetWriter.builder(new LocalOutputFile(file.resolve("ways.parquet")))
      .withType(WAY_SCHEMA)
      .withCompressionCodec(CompressionCodecName.ZSTD)
      .withRowGroupSize(100_000L)
      .build();
    this.relWriter = ExampleParquetWriter.builder(new LocalOutputFile(file.resolve("relations.parquet")))
      .withType(REL_SCHEMA)
      .withCompressionCodec(CompressionCodecName.ZSTD)
      .withRowGroupSize(100_000L)
      .build();
  }

  public static void main(String... args) throws Exception {
    Path dataDir = Path.of("data");
    Arguments arguments = Arguments.fromArgs(args);
    PlanetilerConfig config = PlanetilerConfig.from(arguments);
    Path output = arguments.file("output", "output dir", Path.of("parquet-out"));
    Path sourcesDir = arguments.file("download_dir", "download directory", dataDir.resolve("sources"));
    // use --area=... argument, AREA=... env var or area=... in config to set the region of the world to use
    // will be ignored if osm_path or osm_url are set
    String area = arguments.getString(
      "area",
      "name of the extract to download if osm_url/osm_path not specified (i.e. 'monaco' 'rhode island' 'australia' or 'planet')",
      "massachusetts"
    );
    var tmpDir = config.tmpDir();
    Path nodeDbPath = arguments.file("temp_nodes", "temp node db location", tmpDir.resolve("node.db"));
    Path multipolygonPath =
      arguments.file("temp_multipolygons", "temp multipolygon db location", tmpDir.resolve("multipolygon.db"));
    //    Path osmDefaultPath = sourcesDir.resolve(area.replaceAll("[^a-zA-Z]+", "_") + ".osm.pbf");
    //    // TODO download
    String osmDefaultUrl = "planet".equalsIgnoreCase(area) ? ("aws:latest") : ("geofabrik:" + area);
    Path path = arguments.inputFile("osm_path", "OSM input file", Path.of("data", "sources", "massachusetts.osm.pbf"));
    var thisInputFile = new OsmInputFile(path, config.osmLazyReads());
    var stats = arguments.getStats();

    try (
      var nodeLocations =
        LongLongMap.from(config.nodeMapType(), config.nodeMapStorage(), nodeDbPath, config.nodeMapMadvise());
      var multipolygonGeometries = LongLongMultimap.newReplaceableMultimap(
        config.multipolygonGeometryStorage(), multipolygonPath, config.multipolygonGeometryMadvise());
      var osmReader =
        new OsmReader("osm", thisInputFile, nodeLocations, multipolygonGeometries, (a, b) -> {
        }, stats)
    ) {
      osmReader.pass1(config);
      osmReader.pass2parquet(config, output);
    } finally {
      FileUtils.delete(nodeDbPath);
      FileUtils.delete(multipolygonPath);
    }

    stats.printSummary();
  }

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
    if (sourceFeature instanceof OsmSourceFeature osm) {
      switch (osm.originalElement()) {
        case OsmElement.Node node -> writeNode(node);
        case OsmElement.Way way -> writeWay(way);
        case OsmElement.Relation rel -> writeRel(rel);
        default -> throw new IllegalStateException("Unexpected value: " + osm.originalElement());
      }
    }
  }

  synchronized void writeNode(OsmElement.Node node) {
    var group = new SimpleGroup(NODE_SCHEMA);
    group.add("id", node.id());
    group.add("lat", node.lat());
    group.add("lon", node.lon());
    var tags = group.addGroup("tags");
    node.tags().forEach((k, v) -> {
      var tag = tags.addGroup("key_value");
      tag.add("key", k);
      tag.add("value", (String) v);
    });
    try {
      nodeWriter.write(group);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private synchronized void writeRel(OsmElement.Relation rel) {
    var group = new SimpleGroup(REL_SCHEMA);
    group.add("id", rel.id());
    var tags = group.addGroup("tags");
    rel.tags().forEach((k, v) -> {
      var tag = tags.addGroup("key_value");
      tag.add("key", k);
      tag.add("value", (String) v);
    });
    rel.members().forEach((member) -> {
      var m = group.addGroup("members");
      m.add("role", member.role());
      m.add("ref", member.ref());
      m.add("type", member.type().name().toLowerCase(Locale.ROOT));
    });
    try {
      relWriter.write(group);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private synchronized void writeWay(OsmElement.Way way) {
    var group = new SimpleGroup(WAY_SCHEMA);
    group.add("id", way.id());
    var tags = group.addGroup("tags");
    way.tags().forEach((k, v) -> {
      var tag = tags.addGroup("key_value");
      tag.add("key", k);
      tag.add("value", (String) v);
    });
    for (var node : way.nodes()) {
      group.add("nodes", node.value);
    }
    try {
      wayWriter.write(group);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    try (nodeWriter; wayWriter; relWriter) {
    }
  }
}
