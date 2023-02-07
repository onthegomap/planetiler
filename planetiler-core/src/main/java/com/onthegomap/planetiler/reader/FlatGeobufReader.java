package com.onthegomap.planetiler.reader;

import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.stats.Stats;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.wololo.flatgeobuf.GeometryConversions;
import org.wololo.flatgeobuf.HeaderMeta;
import org.wololo.flatgeobuf.LittleEndianDataInputStream;
import org.wololo.flatgeobuf.PackedRTree;
import org.wololo.flatgeobuf.generated.ColumnType;
import org.wololo.flatgeobuf.generated.Feature;
import org.wololo.flatgeobuf.generated.GeometryType;

public class FlatGeobufReader extends SimpleReader<SimpleFeature> {

  private final Envelope envelope;
  private final MathTransform coordinateTransform;
  private final CoordinateReferenceSystem latLonCRS;
  private final Path input;

  FlatGeobufReader(String sourceProjection, String sourceName, Path input, Envelope envelope) {
    super(sourceName);
    this.envelope = envelope;
    try {
      this.latLonCRS = CRS.decode("EPSG:4326", true);
    } catch (FactoryException e) {
      throw new IllegalStateException(e);
    }

    if (sourceProjection != null) {
      try {
        var sourceCRS = CRS.decode(sourceProjection);
        coordinateTransform = CRS.findMathTransform(sourceCRS, latLonCRS);
      } catch (FactoryException e) {
        throw new FileFormatException("Bad reference system", e);
      }
    } else {
      coordinateTransform = null;
    }

    this.input = input;
  }


  /**
   * Renders map features for all elements from an OGC GeoPackage based on the mapping logic defined in {@code profile}.
   *
   * @param sourceProjection code for the coordinate reference system of the input data, to be parsed by
   *                         {@link CRS#decode(String)}
   * @param sourceName       string ID for this reader to use in logs and stats
   * @param sourcePaths      paths to the {@code .gpkg} files on disk
   * @param writer           consumer for rendered features
   * @param config           user-defined parameters controlling number of threads and log interval
   * @param profile          logic that defines what map features to emit for each source feature
   * @param stats            to keep track of counters and timings
   * @throws IllegalArgumentException if a problem occurs reading the input file
   */
  public static void process(String sourceProjection, String sourceName, List<Path> sourcePaths,
    FeatureGroup writer, PlanetilerConfig config, Profile profile, Stats stats) {
    SourceFeatureProcessor.processFiles(
      sourceName,
      sourcePaths,
      path -> new FlatGeobufReader(sourceProjection, sourceName, path, config.bounds().latLon()),
      writer, config, profile, stats
    );
  }

  @Override
  public long getFeatureCount() {
    try (var is = read()) {
      var header = HeaderMeta.read(is);
      var fromSrid = CRS.decode("EPSG:" + header.srid);
      var transformedEnvelope = JTS.transform(envelope, CRS.findMathTransform(latLonCRS, fromSrid));
      var result =
        PackedRTree.search(is, header.offset, (int) header.featuresCount, header.indexNodeSize, transformedEnvelope);
      return result.hits.size();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (NoSuchAuthorityCodeException e) {
      throw new RuntimeException(e);
    } catch (FactoryException e) {
      throw new RuntimeException(e);
    } catch (TransformException e) {
      throw new RuntimeException(e);
    }
  }

  private BufferedInputStream read() throws IOException {
    return new BufferedInputStream(input.toUri().toURL().openStream());
  }

  private static String readString(ByteBuffer bb, String name) {
    int length = bb.getInt();
    byte[] stringBytes = new byte[length];
    bb.get(stringBytes, 0, length);
    return new String(stringBytes, StandardCharsets.UTF_8);
  }

  //  public static void skipNBytes(InputStream stream, long skip) throws IOException {
  //    long actual = 0;
  //    long remaining = skip;
  //    while (actual < remaining) {
  //      remaining -= stream.skip(remaining);
  //    }
  //  }

  @Override
  public void readFeatures(Consumer<SimpleFeature> next) throws Exception {
    long id = 0;
    try (var is = read()) {
      var header = HeaderMeta.read(is);
      String layer =
        header.name == null ? com.google.common.io.Files.getNameWithoutExtension(input.getFileName().toString()) :
          header.name;
      var fromSrid = CRS.decode("EPSG:" + header.srid);
      var transform = (coordinateTransform != null) ? coordinateTransform : CRS.findMathTransform(fromSrid, latLonCRS);
      var transformedEnvelope = JTS.transform(envelope, CRS.findMathTransform(latLonCRS, fromSrid));
      var result =
        PackedRTree.search(is, header.offset, (int) header.featuresCount, header.indexNodeSize, transformedEnvelope);
      int treeSize =
        header.featuresCount > 0 && header.indexNodeSize > 0 ? (int) PackedRTree.calcSize(
          (int) header.featuresCount, header.indexNodeSize) : 0;
      int skip = treeSize - result.pos;
      if (skip > 0) {
        is.skipNBytes(treeSize - result.pos);
      }
      //      org.wololo.flatgeobuf.generated.Geometry.get
      try (var lis = new LittleEndianDataInputStream(is)) {
        long offset = 0;
        for (var hit : result.hits) {
          if (hit.offset > offset) {
            lis.skipNBytes(hit.offset - offset);
            offset = hit.offset;
          }
          int len = lis.readInt();
          offset += 4;
          byte[] bytes = new byte[len];
          lis.readFully(bytes);
          offset += len;

          var bb2 = ByteBuffer.wrap(bytes);
          Feature f = Feature.getRootAsFeature(bb2);
          var geometry = f.geometry();
          var jts = GeometryConversions.deserialize(geometry,
            header.geometryType == GeometryType.Unknown ? geometry.type() : header.geometryType);
          Geometry latLonGeom = (transform.isIdentity()) ? jts : JTS.transform(jts, transform);
          SimpleFeature feature = SimpleFeature.create(latLonGeom, new HashMap<>(header.columns.size()),
            sourceName, layer, ++id);
          int propertiesLength = f.propertiesLength();
          if (propertiesLength > 0) {
            ByteBuffer bb = f.propertiesAsByteBuffer();
            while (bb.hasRemaining()) {
              short i = bb.getShort();
              var columnMeta = header.columns.get(i);
              String name = columnMeta.name;
              feature.setTag(name, switch (columnMeta.type) {
                case ColumnType.Bool -> bb.get() > 0;
                case ColumnType.Byte -> bb.get();
                case ColumnType.Short -> bb.getShort();
                case ColumnType.Int -> bb.getInt();
                case ColumnType.Long -> bb.getLong();
                case ColumnType.Double -> bb.getDouble();
                case ColumnType.DateTime, ColumnType.String -> readString(bb, name);
                default -> throw new IllegalStateException("Unknown type " + columnMeta.type);
              });
            }
          }
          next.accept(feature);
        }
      }
      //      System.err.println(result.hits.stream().map(d -> d.offset + "|" + d.index).toList());
      //      System.err.println(result.pos);
    }
    //    long id = 0;
    //    bb.position(0);
    //    var header = HeaderMeta.read(bb);
    //    String layer = header.name;
    //    var hits = PackedRTree.search(bb, header.offset, (int) header.featuresCount, header.indexNodeSize, envelope);
    //    int base = (int) (header.offset + header.featuresCount * header.indexNodeSize);
    //    bb.position(header.offset + header.indexNodeSize);
    //    //    var geometry = org.wololo.flatgeobuf.generated.Geometry.getRootAsGeometry(bb);
    //    var transform = (coordinateTransform != null) ? coordinateTransform :
    //      CRS.findMathTransform(CRS.decode("EPSG:" + header.srid), latLonCRS);
    //
    //    for (PackedRTree.SearchHit hit : hits) {
    //      var feature = org.wololo.flatgeobuf.generated.Feature
    //        .getRootAsFeature(bb.position((int) hit.offset + base));
    //      //      var part = geometry.parts(i);
    //      System.err.println(feature.geometry());
    //      var jts = GeometryConversions.deserialize(feature.geometry(), feature.geometry().type());
    //
    //      Geometry latLonGeom = (transform.isIdentity()) ? jts : JTS.transform(jts, transform);
    //
    //      //      part
    //      //
    //      SimpleFeature geom = SimpleFeature.create(latLonGeom, new HashMap<>(header.columns.size()),
    //        sourceName, layer, ++id);
    //
    //
    //      //
    //      //      for (var feature : features.queryForAll()) {
    //      //        GeoPackageGeometryData geometryData = feature.getGeometry();
    //      //        if (geometryData == null) {
    //      //          continue;
    //      //        }
    //      //
    //      //        Geometry featureGeom = (new WKBReader()).read(geometryData.getWkb());
    //      //        Geometry latLonGeom = (transform.isIdentity()) ? featureGeom : JTS.transform(featureGeom, transform);
    //      //
    //      //        FeatureColumns columns = feature.getColumns();
    //      //        SimpleFeature geom = SimpleFeature.create(latLonGeom, new HashMap<>(columns.columnCount()),
    //      //          sourceName, featureName, ++id);
    //      //
    //      //        for (int i = 0; i < columns.columnCount(); ++i) {
    //      //          if (i != columns.getGeometryIndex()) {
    //      //            geom.setTag(columns.getColumnName(i), feature.getValue(i));
    //      //          }
    //      //        }
    //
    //      next.accept(geom);
    //    }
  }

  @Override
  public void close() throws IOException {}
}
