package com.onthegomap.planetiler.reader;

import io.soabase.recordbuilder.core.RecordBuilder;
import java.nio.charset.Charset;

/**
 * Options to control how shapefiles are read.
 *
 * @param projection              Coordinate Reference System authority code of the input data if you need to override
 *                                what the shapefile defines, parsed with
 *                                {@link org.geotools.referencing.CRS#decode(String)}, or {@code null} to not override
 * @param globPattern             {@link java.nio.file.FileSystem#getPathMatcher(String) glob pattern} pointing to a
 *                                single or multiple shapefiles to include in a single source
 * @param lowercaseAttributeNames convert attribute names to lowercase, since {@code .dbf} field names are often
 *                                uppercase or mixed case
 * @param emptyStringsAsNull      leave out attributes with an empty string value so they read as {@code null}, since a
 *                                {@code .dbf} file can't store {@code null} text
 * @param charset                 charset of {@code .dbf} text, overriding the one a shapefile's {@code .cpg} file
 *                                names, or {@code null} to use the {@code .cpg} file (ISO-8859-1 without one)
 */
@RecordBuilder
public record ShapefileOptions(String projection, String globPattern, boolean lowercaseAttributeNames,
  boolean emptyStringsAsNull, Charset charset) implements ShapefileOptionsBuilder.With {

  public static final ShapefileOptions DEFAULT = new ShapefileOptions(null, "*.shp", false, false, null);

  /** Returns a builder that starts from {@link #DEFAULT}. */
  public static ShapefileOptionsBuilder builder() {
    return DEFAULT.with();
  }
}
