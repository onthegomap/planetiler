package com.onthegomap.flatmap.reader.osm;

import com.graphhopper.reader.ReaderElement;
import com.onthegomap.flatmap.worker.WorkerPipeline;

public interface OsmSource {

  /**
   * Returns a source that initiates a {@link WorkerPipeline} with raw OSM elements.
   *
   * @param poolName string ID used when creating worker threads to decode OSM blocks
   * @param threads  maximum number of threads to use when processing elements in parallel
   * @return work for a source thread
   */
  WorkerPipeline.SourceStep<ReaderElement> read(String poolName, int threads);
}
