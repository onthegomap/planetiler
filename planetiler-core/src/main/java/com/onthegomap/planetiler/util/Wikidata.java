package com.onthegomap.planetiler.util;

import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.onthegomap.planetiler.util.Exceptions.throwFatalException;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongObjectMap;
import com.carrotsearch.hppc.LongSet;
import com.carrotsearch.hppc.cursors.LongObjectCursor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.collection.Hppc;
import com.onthegomap.planetiler.reader.osm.OsmBlockSource;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmInputFile;
import com.onthegomap.planetiler.stats.Counter;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.stats.Timer;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility to download name translations from wikidata for all OSM elements with a
 * <a href="https://wiki.openstreetmap.org/wiki/Key:wikidata">wikidata tag</a>.
 */
public class Wikidata {

  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final Logger LOGGER = LoggerFactory.getLogger(Wikidata.class);
  private static final Pattern wikidataIRIMatcher = Pattern.compile("http://www.wikidata.org/entity/Q(\\d+)");
  private static final Pattern qidPattern = Pattern.compile("Q(\\d+)");
  private final Counter.Readable blocks = Counter.newMultiThreadCounter();
  private final Counter.Readable nodes = Counter.newMultiThreadCounter();
  private final Counter.Readable ways = Counter.newMultiThreadCounter();
  private final Counter.Readable rels = Counter.newMultiThreadCounter();
  private final Counter.Readable wikidatas = Counter.newMultiThreadCounter();
  private final Counter.Readable batches = Counter.newMultiThreadCounter();
  private final LongSet visited = new LongHashSet();
  private final List<Long> qidsToFetch;
  private final Writer writer;
  private final Downloader downloader;
  private final int batchSize;
  private final Profile profile;

  Wikidata(Writer writer, Downloader downloader, int batchSize, Profile profile) {
    this.writer = writer;
    this.downloader = downloader;
    this.batchSize = batchSize;
    this.profile = profile;
    qidsToFetch = new ArrayList<>(batchSize);
  }

  /** Parses persisted name translations and returns a map from QID to language to name. */
  private static LongObjectMap<Map<String, String>> parseResults(InputStream results) throws IOException {
    JsonNode node = objectMapper.readTree(results);
    ArrayNode bindings = (ArrayNode) node.get("results").get("bindings");
    LongObjectMap<Map<String, String>> resultMap = Hppc.newLongObjectHashMap();
    bindings.elements().forEachRemaining(row -> {
      long id = extractIdFromWikidataIRI(row.get("id").get("value").asText());
      Map<String, String> map = resultMap.get(id);
      if (map == null) {
        resultMap.put(id, map = new TreeMap<>());
      }
      JsonNode label = row.get("label");
      map.put(
        label.get("xml:lang").asText(),
        label.get("value").asText()
      );
    });
    return resultMap;
  }

  /**
   * Loads any existing translations from {@code outfile}, then downloads translations for any wikidata element in
   * {@code infile} that have not already been downloaded and writes the results to {@code outfile}.
   *
   * @throws UncheckedIOException if an error occurs
   */
  public static void fetch(OsmInputFile infile, Path outfile, Downloader downloader, Profile profile, Stats stats) {
    fetch(infile, outfile, downloader, profile, stats, Duration.ofSeconds(0), 0);
  }

  /**
   * Loads any existing translations from {@code outfile}, then downloads translations for any wikidata element in
   * {@code infile} that have not already been downloaded and writes the results to {@code outfile}.
   *
   * @throws UncheckedIOException if an error occurs
   */
  public static void fetch(OsmInputFile infile, Path outfile, Downloader downloader, Profile profile, Stats stats,
    Duration maxAge, int updateLimit) {
    var config = downloader.config();

    var timer = stats.startStage("wikidata");
    int processThreads = Math.max(1, config.threads() - 1);
    LOGGER.info("Starting with {} process threads", processThreads);

    WikidataTranslations oldMappings = load(outfile, maxAge, updateLimit);
    try (
      Writer writer = Files.newBufferedWriter(outfile);
      OsmBlockSource osmSource = infile.get()
    ) {
      Wikidata fetcher = new Wikidata(writer, downloader, 5_000, profile);
      fetcher.loadExisting(oldMappings);

      String pbfParsePrefix = "pbfwikidata";
      var pipeline = WorkerPipeline.start("wikidata", stats)
        .fromGenerator("pbf", osmSource::forEachBlock)
        .addBuffer("pbf_blocks", processThreads * 2)
        .addWorker("filter", processThreads, fetcher::filter)
        .addBuffer("fetch_queue", 1_000_000, 100)
        .sinkTo("fetch", 1, prev -> {
          for (Long id : prev) {
            fetcher.fetch(id);
          }
          fetcher.flush();
        });

      ProgressLoggers loggers = ProgressLoggers.create()
        .addRateCounter("blocks", fetcher.blocks)
        .addRateCounter("nodes", fetcher.nodes, true)
        .addRateCounter("ways", fetcher.ways, true)
        .addRateCounter("rels", fetcher.rels, true)
        .addRateCounter("wiki", fetcher.wikidatas)
        .addFileSize(outfile)
        .newLine()
        .addProcessStats()
        .newLine()
        .addThreadPoolStats("parse", pbfParsePrefix + "-pool")
        .addPipelineStats(pipeline);

      pipeline.awaitAndLog(loggers, config.logInterval());
      LOGGER.info("DONE fetched: {}", fetcher.wikidatas.get());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    timer.stop();
  }

  /**
   * Returns translations parsed from {@code path} that was written by a previous run of the downloader.
   */
  public static WikidataTranslations load(Path path) {
    var translationsProvider = Wikidata.load(path, Duration.ZERO, 0);
    translationsProvider.clearUpdateTimes();
    return translationsProvider;
  }

  private static WikidataTranslations load(Path path, Duration maxAge, int updateLimit) {
    Timer timer = Timer.start();
    if (!Files.exists(path)) {
      LOGGER.info("no wikidata translations found, run with --fetch-wikidata to download");
      return new WikidataTranslations();
    } else {
      try (BufferedReader fis = Files.newBufferedReader(path)) {
        WikidataTranslations result = load(fis, maxAge, updateLimit, Clock.systemUTC());
        LOGGER.info(
          "loaded from {} mappings from {} in {}",
          result.getAll().size(),
          path.toAbsolutePath(),
          timer.stop()
        );
        return result;
      } catch (IOException e) {
        LOGGER.info("error loading {}: {}", path.toAbsolutePath(), e);
        return new WikidataTranslations();
      }
    }
  }

  /**
   * Returns translations parsed from {@code reader} where each line is a JSON array where first element is the ID and
   * second element is a map from language to translation.
   */
  static WikidataTranslations load(BufferedReader reader) throws IOException {
    return load(reader, Duration.ZERO, 0, Clock.systemUTC());
  }

  protected static WikidataTranslations load(BufferedReader reader, Duration maxAge, int updateLimit, Clock clock)
    throws IOException {
    WikidataTranslations mappings = new WikidataTranslations();
    String line;
    Instant updateTimeLimit = maxAge.isZero() ? null : Instant.now(clock).minus(maxAge);
    int updateCounter = 0;
    while ((line = reader.readLine()) != null) {
      JsonNode node = objectMapper.readTree(line);
      long id = Long.parseLong(node.get(0).asText());

      Instant updateTime = Instant.EPOCH;
      if (node.has(2)) {
        updateTime = Instant.ofEpochMilli(node.get(2).asLong());
      }
      if (updateTimeLimit != null && updateTime.isBefore(updateTimeLimit) &&
        (updateLimit <= 0 || updateCounter < updateLimit)) {
        // do not load old entries => new translations will be fetched later
        updateCounter++;
        continue;
      }
      mappings.putUpdateTime(id, updateTime);

      ObjectNode theseMappings = (ObjectNode) node.get(1);
      theseMappings.fields().forEachRemaining(entry -> mappings.put(id, entry.getKey(), entry.getValue().asText()));
    }
    if (updateCounter > 0) {
      LOGGER.info("{} translations dropped as too old, will be re-fetched", updateCounter);
    }
    return mappings;
  }

  /** Returns a numeric ID from a wikidata IRI like {@code https://www.wikidata.org/wiki/Q9141}. */
  private static long extractIdFromWikidataIRI(String iri) {
    Matcher matcher = wikidataIRIMatcher.matcher(iri);
    if (matcher.matches()) {
      String idText = matcher.group(1);
      return Long.parseLong(idText);
    } else {
      throw new IllegalStateException("Unexpected response IRI: " + iri);
    }
  }

  /** Returns a numeric ID from a wikidata ID starting with a "Q". */
  private static long parseQid(Object qid) {
    long result = 0;
    if (qid instanceof String qidString) {
      Matcher matcher = qidPattern.matcher(qidString);
      if (matcher.matches()) {
        String idString = matcher.group(1);
        result = Parse.parseLong(idString);
      }
    }
    return result;
  }

  /** Only pass elements that the profile cares about to next step in pipeline. */
  private void filter(Iterable<OsmBlockSource.Block> prev, Consumer<Long> next) {
    for (var block : prev) {
      int blockNodes = 0, blockWays = 0, blockRelations = 0;
      for (var elem : block.decodeElements()) {
        if (elem instanceof OsmElement.Node) {
          blockNodes++;
        } else if (elem instanceof OsmElement.Way) {
          blockWays++;
        } else if (elem instanceof OsmElement.Relation) {
          blockRelations++;
        }
        Object wikidata = elem.getString("wikidata");
        if (wikidata instanceof String wikidataString && profile.caresAboutWikidataTranslation(elem)) {
          long qid = parseQid(wikidataString);
          if (qid > 0) {
            next.accept(qid);
          }
        }

      }
      blocks.inc();
      nodes.incBy(blockNodes);
      ways.incBy(blockWays);
      rels.incBy(blockRelations);
    }
  }

  void flush() {
    try {
      Timer timer = Timer.start();
      LongObjectMap<Map<String, String>> results = queryWikidata(qidsToFetch);
      batches.inc();
      LOGGER.info("Fetched batch {} ({} qids) {}", batches.get(), qidsToFetch.size(), timer.stop());
      writeTranslations(results, Hppc.newLongObjectHashMap());
    } catch (IOException e) {
      throwFatalException(e);
    }
    wikidatas.incBy(qidsToFetch.size());
    qidsToFetch.clear();
  }

  void fetch(long id) {
    if (!visited.contains(id)) {
      visited.add(id);
      qidsToFetch.add(id);
    }
    if (qidsToFetch.size() >= batchSize) {
      flush();
    }
  }

  /**
   * Make an HTTP request to wikidata's <a href="https://www.wikidata.org/wiki/Wikidata:SPARQL_query_service">sparql
   * endpoint</a> to fetch name translations for a set of QIDs.
   */
  private LongObjectMap<Map<String, String>> queryWikidata(List<Long> qidsToFetch) throws IOException {
    if (qidsToFetch.isEmpty()) {
      return Hppc.newLongObjectHashMap();
    }
    String qidList = qidsToFetch.stream().map(id -> "wd:Q" + id).collect(Collectors.joining(" "));
    String query = """
      SELECT ?id ?label where {
        VALUES ?id { %s } ?id (owl:sameAs* / rdfs:label) ?label
      }
      """.formatted(qidList).replaceAll("\\s+", " ").trim();

    var request = new Downloader.Request("https://query.wikidata.org/bigdata/namespace/wdq/sparql",
      Map.of(ACCEPT, "application/sparql-results+json", CONTENT_TYPE, "application/sparql-query"), query);

    return downloader.sendWithRetries(request, body -> parseResults(new BufferedInputStream(body)));
  }

  void loadExisting(WikidataTranslations oldMappings) throws IOException {
    LongObjectMap<Map<String, String>> alreadyHave = oldMappings.getAll();
    LongObjectMap<Instant> alreadyHaveUpdateTimes = oldMappings.getUpdateTimes();
    if (!alreadyHave.isEmpty()) {
      LOGGER.info("skipping {} mappings we already have", alreadyHave.size());
      writeTranslations(alreadyHave, alreadyHaveUpdateTimes);
      for (LongObjectCursor<Map<String, String>> cursor : alreadyHave) {
        visited.add(cursor.key);
      }
    }
  }

  /** Flushes a batch of translations to disk. */
  private void writeTranslations(LongObjectMap<Map<String, String>> results, LongObjectMap<Instant> updateTimes)
    throws IOException {
    final long updateTimeDefault = Instant.now().toEpochMilli();
    for (LongObjectCursor<Map<String, String>> cursor : results) {
      long updateTime =
        updateTimes.containsKey(cursor.key) ? updateTimes.get(cursor.key).toEpochMilli() : updateTimeDefault;
      writer.write(objectMapper.writeValueAsString(List.of(
        Long.toString(cursor.key),
        cursor.value,
        updateTime
      )));
      writer.write(System.lineSeparator());
    }
    writer.flush();
  }

  public static class WikidataTranslations implements Translations.TranslationProvider {

    private final LongObjectMap<Map<String, String>> data = Hppc.newLongObjectHashMap();
    private final LongObjectMap<Instant> updateTimes = Hppc.newLongObjectHashMap();

    /** Returns a map from language code to translated name for {@code qid}. */
    public Map<String, String> get(long qid) {
      return data.get(qid);
    }

    public void clearUpdateTimes() {
      updateTimes.clear();
    }

    /** Returns all maps from language code to translated name for {@code qid}. */
    public LongObjectMap<Map<String, String>> getAll() {
      return data;
    }

    /** Returns all maps from language code to translated name for {@code qid}. */
    public LongObjectMap<Instant> getUpdateTimes() {
      return updateTimes;
    }

    /** Stores a update date+time for {@code qid}. */
    public void putUpdateTime(long qid, Instant updateTime) {
      updateTimes.put(qid, updateTime);
    }

    /** Stores a name translation for {@code qid} in {@code lang}. */
    public void put(long qid, String lang, String value) {
      Map<String, String> map = data.get(qid);
      if (map == null) {
        data.put(qid, map = new TreeMap<>());
      }
      map.put(lang, value);
    }

    @Override
    public Map<String, String> getNameTranslations(Map<String, Object> tags) {
      long wikidataId = parseQid(tags.get("wikidata"));
      if (wikidataId > 0) {
        return get(wikidataId);
      }
      return null;
    }
  }
}
