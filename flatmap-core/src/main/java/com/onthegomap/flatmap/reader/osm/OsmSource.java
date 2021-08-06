package com.onthegomap.flatmap.reader.osm;

import com.graphhopper.reader.ReaderElement;
import com.onthegomap.flatmap.worker.WorkerPipeline;

public interface OsmSource {

  WorkerPipeline.SourceStep<ReaderElement> read(String poolName, int threads);
}
