package com.onthegomap.flatmap.openmaptiles.layers;

import static com.onthegomap.flatmap.openmaptiles.Utils.coalesce;
import static com.onthegomap.flatmap.openmaptiles.Utils.nullIfEmpty;
import static com.onthegomap.flatmap.openmaptiles.Utils.nullOrEmpty;

import com.onthegomap.flatmap.Arguments;
import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.SourceFeature;
import com.onthegomap.flatmap.Translations;
import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.geo.PolygonIndex;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.openmaptiles.LanguageUtils;
import com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile;
import com.onthegomap.flatmap.openmaptiles.generated.OpenMapTilesSchema;
import com.onthegomap.flatmap.openmaptiles.generated.Tables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Place implements
  OpenMapTilesSchema.Place,
  OpenMapTilesProfile.NaturalEarthProcessor,
  Tables.OsmContinentPoint.Handler,
  Tables.OsmCountryPoint.Handler,
  Tables.OsmStatePoint.Handler,
  Tables.OsmIslandPoint.Handler,
  Tables.OsmIslandPolygon.Handler {

  private static final Logger LOGGER = LoggerFactory.getLogger(Place.class);

  private final Translations translations;

  private static record NaturalEarthCountry(String name, double scalerank, double labelrank) {}

  private PolygonIndex<NaturalEarthCountry> countries = PolygonIndex.create();

  public Place(Translations translations, Arguments args, Stats stats) {
    this.translations = translations;
  }

  @Override
  public void process(Tables.OsmIslandPolygon element, FeatureCollector features) {
    // TODO
  }

  @Override
  public void process(Tables.OsmIslandPoint element, FeatureCollector features) {
    // TODO
  }

  @Override
  public void process(Tables.OsmStatePoint element, FeatureCollector features) {
    // TODO
  }

  @Override
  public void processNaturalEarth(String table, SourceFeature feature, FeatureCollector features) {
    try {
      switch (table) {
        case "ne_10m_admin_0_countries" -> countries.put(feature.worldGeometry(), new NaturalEarthCountry(
          feature.getString("name"),
          feature.getLong("scalerank"),
          feature.getLong("labelrank")
        ));
      }
    } catch (GeometryException e) {
      LOGGER
        .warn("Error getting geometry for natural earth feature " + table + " " + feature.getTag("ogc_fid") + " " + e);
    }
  }

  @Override
  public void process(Tables.OsmContinentPoint element, FeatureCollector features) {
    if (!nullOrEmpty(element.name())) {
      features.point(LAYER_NAME).setBufferPixels(BUFFER_SIZE)
        .setAttrs(LanguageUtils.getNames(element.source().properties(), translations))
        .setAttr(Fields.CLASS, FieldValues.CLASS_CONTINENT)
        .setAttr(Fields.RANK, 1)
        .setAttrs(LanguageUtils.getNames(element.source().properties(), translations))
        .setZoomRange(0, 3);
    }
  }

  @Override
  public void process(Tables.OsmCountryPoint element, FeatureCollector features) {
    if (nullOrEmpty(element.name())) {
      return;
    }
    String isoA2 = coalesce(
      nullIfEmpty(element.countryCodeIso31661Alpha2()),
      nullIfEmpty(element.iso31661Alpha2()),
      nullIfEmpty(element.iso31661())
    );
    if (isoA2 == null) {
      return;
    }
    try {
      int rank = 7;
      NaturalEarthCountry country = countries.get(element.source().worldGeometry().getCentroid());
      var names = LanguageUtils.getNames(element.source().properties(), translations);

      if (country != null) {
        if (nullOrEmpty(names.get(Fields.NAME_EN))) {
          names.put(Fields.NAME_EN, country.name);
        }
        rank = (int) Math.min(6, Math.ceil((country.scalerank + country.labelrank) / 2d));
      }

      rank = Math.min(6, Math.max(1, rank));

      features.point(LAYER_NAME).setBufferPixels(BUFFER_SIZE)
        .setAttrs(names)
        .setAttr(Fields.ISO_A2, isoA2)
        .setAttr(Fields.CLASS, FieldValues.CLASS_COUNTRY)
        .setAttr(Fields.RANK, rank)
        .setZoomRange(rank - 1, 14);
    } catch (GeometryException e) {
      LOGGER.warn("Unable to get point for OSM country point " + element.source().id());
    }
  }
}
