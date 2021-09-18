package com.onthegomap.flatmap.openmaptiles;

import com.onthegomap.flatmap.ForwardingProfile;

/** Interface for all vector tile layer implementations that {@link OpenMapTilesProfile} delegates to. */
public interface Layer extends
  ForwardingProfile.Handler,
  ForwardingProfile.HandlerForLayer {}
