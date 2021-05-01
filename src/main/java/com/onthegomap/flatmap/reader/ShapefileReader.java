package com.onthegomap.flatmap.reader;

import com.onthegomap.flatmap.CommonParams;
import com.onthegomap.flatmap.FeatureRenderer;
import com.onthegomap.flatmap.Profile;
import com.onthegomap.flatmap.SourceFeature;
import com.onthegomap.flatmap.collections.FeatureGroup;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.worker.Topology;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.geotools.data.FeatureSource;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.feature.FeatureCollection;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

public class ShapefileReader extends Reader implements Closeable {

  private final FeatureCollection<SimpleFeatureType, SimpleFeature> inputSource;
  private final String[] attributeNames;
  private final ShapefileDataStore dataStore;
  private MathTransform transform;

  public static void process(String sourceProjection, String name, Path input, FeatureRenderer renderer,
    FeatureGroup writer, CommonParams config, Profile profile, Stats stats) {
    try (var reader = new ShapefileReader(sourceProjection, input, profile, stats)) {
      reader.process(name, renderer, writer, config);
    }
  }

  public static void process(String name, Path input, FeatureRenderer renderer,
    FeatureGroup writer, CommonParams config, Profile profile, Stats stats) {
    process(null, name, input, renderer, writer, config, profile, stats);
  }

  public ShapefileReader(String sourceProjection, Path input, Profile profile, Stats stats) {
    super(profile, stats);
    dataStore = decode(input);
    try {
      String typeName = dataStore.getTypeNames()[0];
      FeatureSource<SimpleFeatureType, SimpleFeature> source =
        dataStore.getFeatureSource(typeName);

      inputSource = source.getFeatures(Filter.INCLUDE);
      CoordinateReferenceSystem src =
        sourceProjection == null ? source.getSchema().getCoordinateReferenceSystem() : CRS.decode(sourceProjection);
      CoordinateReferenceSystem dest = CRS.decode("EPSG:4326", true);
      transform = CRS.findMathTransform(src, dest);
      if (transform.isIdentity()) {
        transform = null;
      }
      attributeNames = new String[inputSource.getSchema().getAttributeCount()];
      for (int i = 0; i < attributeNames.length; i++) {
        attributeNames[i] = inputSource.getSchema().getDescriptor(i).getLocalName();
      }
    } catch (IOException | FactoryException e) {
      throw new RuntimeException(e);
    }
  }

  private ShapefileDataStore decode(Path path) {
    try {

      URI uri;

      if (path.toString().toLowerCase().endsWith(".zip")) {
        try (ZipFile zip = new ZipFile(path.toFile())) {
          String shapeFileInZip = zip.stream()
            .map(ZipEntry::getName)
            .filter(z -> z.endsWith(".shp"))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("No .shp file found inside " + path));
          uri = URI.create("jar:file:" + path.toAbsolutePath() + "!/" + shapeFileInZip);
        }
      } else if (path.toString().toLowerCase().endsWith(".shp")) {
        uri = path.toUri();
      } else {
        throw new IllegalArgumentException("Invalid shapefile input: " + path + " must be zip or shp");
      }
      return new ShapefileDataStore(uri.toURL());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public ShapefileReader(Path input, Profile profile, Stats stats) {
    this(null, input, profile, stats);
  }

  @Override
  public long getCount() {
    return inputSource.size();
  }

  @Override
  public Topology.SourceStep<SourceFeature> read() {
    return next -> {
      try (var iter = inputSource.features()) {
        while (iter.hasNext()) {
          SimpleFeature feature = iter.next();
          Geometry source = (Geometry) feature.getDefaultGeometry();
          Geometry transformed = source;
          if (transform != null) {
            transformed = JTS.transform(source, transform);
          }
          if (transformed != null) {
            SourceFeature geom = new ReaderFeature(transformed, attributeNames.length);
            for (int i = 1; i < attributeNames.length; i++) {
              geom.setTag(attributeNames[i], feature.getAttribute(i));
            }
            next.accept(geom);
          }
        }
      }
    };
  }

  @Override
  public void close() {
    dataStore.dispose();
  }
}
