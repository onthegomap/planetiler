package com.onthegomap.flatmap.openmaptiles;

import com.onthegomap.flatmap.Profile;

/** Interface for all vector tile layer implementations that {@link OpenMapTilesProfile} delegates to. */
public interface Layer extends
  Profile.ForwardingProfile.Handler,
  Profile.ForwardingProfile.HandlerForLayer {}
