package com.onthegomap.planetiler.basemap;

import com.onthegomap.planetiler.ForwardingProfile;

/** Interface for all vector tile layer implementations that {@link BasemapProfile} delegates to. */
public interface Layer extends
  ForwardingProfile.Handler,
  ForwardingProfile.HandlerForLayer {}
