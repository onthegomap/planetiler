package com.onthegomap.planetiler.reader.parquet;

import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.overture.Struct;
import com.onthegomap.planetiler.reader.SourceFeature;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.avro.generic.GenericRecord;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Lineal;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.geom.Puntal;

public class AvroParquetFeature extends SourceFeature {

  private final Function<GenericRecord, Geometry> geometryParser;
  private final GenericRecord unparsed;
  private final Path filename;
  private Geometry latLon;
  private Geometry world;

  AvroParquetFeature(GenericRecord unparsed, String source, String sourceLayer, Path filename,
    long id, Function<GenericRecord, Geometry> getGeometry, Map<String, Object> tags) {
    super(tags, source, sourceLayer, List.of(), id);
    this.unparsed = unparsed;
    this.geometryParser = getGeometry;
    this.filename = filename;
  }

  public GenericRecord getRecord() {
    return unparsed;
  }

  public Struct getStruct() {
    return Struct.of(Struct.convert(unparsed));
  }

  public Path getFilename() {
    return filename;
  }

  @Override
  public Geometry latLonGeometry() throws GeometryException {
    return latLon == null ? latLon = geometryParser.apply(unparsed) : latLon;
  }

  @Override
  public Geometry worldGeometry() throws GeometryException {
    return world != null ? world :
      (world = GeoUtils.sortPolygonsByAreaDescending(GeoUtils.latLonToWorldCoords(latLonGeometry())));
  }

  @Override
  public boolean isPoint() {
    try {
      return latLonGeometry() instanceof Puntal;
    } catch (GeometryException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public boolean canBePolygon() {
    try {
      return latLonGeometry() instanceof Polygonal;
    } catch (GeometryException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public boolean canBeLine() {
    try {
      return latLonGeometry() instanceof Lineal;
    } catch (GeometryException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public String toString() {
    return tags().toString();
  }
}
