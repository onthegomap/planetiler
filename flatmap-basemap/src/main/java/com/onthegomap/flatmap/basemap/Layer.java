package com.onthegomap.flatmap.basemap;

import com.onthegomap.flatmap.ForwardingProfile;

/** Interface for all vector tile layer implementations that {@link BasemapProfile} delegates to. */
public interface Layer extends
  ForwardingProfile.Handler,
  ForwardingProfile.HandlerForLayer {}
