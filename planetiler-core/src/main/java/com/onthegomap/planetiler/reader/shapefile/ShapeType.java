package com.onthegomap.planetiler.reader.shapefile;

/**
 * Shape types of shapefile records, from the "Shape Types" table of the <a href=
 * "https://www.esri.com/content/dam/esrisites/sitecore-archive/Files/Pdfs/library/whitepapers/pdfs/shapefile.pdf">ESRI
 * Shapefile Technical Description</a>.
 */
enum ShapeType {
  NULL(0),
  POINT(1),
  POLYLINE(3),
  POLYGON(5),
  MULTIPOINT(8),
  POINTZ(11),
  POLYLINEZ(13),
  POLYGONZ(15),
  MULTIPOINTZ(18),
  POINTM(21),
  POLYLINEM(23),
  POLYGONM(25),
  MULTIPOINTM(28),
  MULTIPATCH(31);

  private static final ShapeType[] BY_ID = new ShapeType[32];

  static {
    for (var type : values()) {
      BY_ID[type.id] = type;
    }
  }

  /** The number that identifies this type in a {@code .shp} file. */
  final int id;

  ShapeType(int id) {
    this.id = id;
  }

  /** Returns the shape type with {@code id}, or throws {@link IllegalArgumentException} if there is none. */
  static ShapeType forId(int id) {
    ShapeType result = id >= 0 && id < BY_ID.length ? BY_ID[id] : null;
    if (result == null) {
      throw new IllegalArgumentException("Unknown shape type " + id);
    }
    return result;
  }

  boolean isPointType() {
    return this == POINT || this == POINTZ || this == POINTM;
  }

  boolean isMultiPointType() {
    return this == MULTIPOINT || this == MULTIPOINTZ || this == MULTIPOINTM;
  }

  boolean isLineType() {
    return this == POLYLINE || this == POLYLINEZ || this == POLYLINEM;
  }

  boolean hasZ() {
    return this == POINTZ || this == MULTIPOINTZ || this == POLYLINEZ || this == POLYGONZ;
  }

  boolean canHaveM() {
    // Z types can also have M, M types only have M
    return hasZ() || this == POINTM || this == MULTIPOINTM || this == POLYLINEM || this == POLYGONM;
  }

  boolean isPolygonType() {
    return this == POLYGON || this == POLYGONZ || this == POLYGONM;
  }
}
