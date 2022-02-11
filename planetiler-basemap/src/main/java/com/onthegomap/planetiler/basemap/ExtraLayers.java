package com.onthegomap.planetiler.basemap;

import java.util.List;

public class ExtraLayers {

  // register extra layers here
  public static final List<Layer.Constructor> EXTRA_LAYER_CONSTRUCTORS = List.of(
    com.onthegomap.planetiler.basemap.layers.Power::new
  );
}
