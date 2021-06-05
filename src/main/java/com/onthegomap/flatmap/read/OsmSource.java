package com.onthegomap.flatmap.read;

import com.graphhopper.reader.ReaderElement;
import com.onthegomap.flatmap.worker.Topology;

public interface OsmSource {

  Topology.SourceStep<ReaderElement> read(String poolName, int threads);
}
