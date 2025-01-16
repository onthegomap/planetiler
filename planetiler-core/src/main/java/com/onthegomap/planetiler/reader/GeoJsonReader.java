package com.onthegomap.planetiler.reader;

import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.stats.Stats;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;
import org.geotools.data.geojson.store.GeoJSONDataStore;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.locationtech.jts.geom.Geometry;

/**
 * Utility that reads {@link SourceFeature SourceFeatures} from the vector geometries contained in a GeoJSON file.
 */
public class GeoJsonReader extends SimpleReader<SimpleFeature> {

    private final GeoJSONDataStore store;
    private final String layer;

    GeoJsonReader(String sourceName, Path input) {
        super(sourceName);
        store = new GeoJSONDataStore(input.toFile());
        layer = input.getFileName().toString().replaceFirst("\\.[^.]+$", ""); // remove file extention.
    }

    /**
   * Renders map features for all elements from an GeoJSON on the mapping logic defined in {@code
   * profile}.
   *
   * @param sourceName  string ID for this reader to use in logs and stats
   * @param sourcePaths paths to the {@code .geojson} files on disk
   * @param writer      consumer for rendered features
   * @param config      user-defined parameters controlling number of threads and log interval
   * @param profile     logic that defines what map features to emit for each source feature
   * @param stats       to keep track of counters and timings
   * @throws IllegalArgumentException if a problem occurs reading the input file
   */
   public static void process(String sourceName, List<Path> sourcePaths, FeatureGroup writer, PlanetilerConfig config,
      Profile profile, Stats stats) {
      SourceFeatureProcessor.processFiles(
        sourceName,
        sourcePaths,
        path -> new GeoJsonReader(sourceName, path),
        writer, config, profile, stats
      );
    }

    @Override
    public void close() throws IOException {
        store.dispose();
    }

    @Override
    public long getFeatureCount() {
        String typeName;
        try {
            typeName = store.getTypeNames()[0];
            SimpleFeatureCollection features = store.getFeatureSource(typeName).getFeatures();
            return Long.valueOf(features.size());
        } catch (IOException e) {
            return 0;
        }
    }

    @Override
    public void readFeatures(Consumer<SimpleFeature> next) throws Exception {
        long id = 0;
        String typeName = store.getTypeNames()[0];
        SimpleFeatureCollection features = store.getFeatureSource(typeName).getFeatures();

        try (var iter = features.features()) {
            while (iter.hasNext()) {
                var feature = iter.next();
                var properties = feature.getProperties();
                SimpleFeature simpleFeature = SimpleFeature.create((Geometry) feature.getDefaultGeometry(), HashMap.newHashMap(properties.size()),
                sourceName, layer, id++);
                properties.forEach(property -> simpleFeature.setTag(property.getName().toString(), property.getValue()));
                next.accept(simpleFeature);
            }
        }
    }
}
