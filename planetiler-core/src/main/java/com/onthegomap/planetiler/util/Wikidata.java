package com.onthegomap.planetiler.util;

import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.HttpHeaders.USER_AGENT;
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
import com.onthegomap.planetiler.config.PlanetilerConfig;
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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility to download name translations from wikidata for all OSM elements with a
 * <a href="https://wiki.openstreetmap.org/wiki/Key:wikidata">wikidata tag</a>.
 */
public class Wikidata {

  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final Logger LOGGER = LoggerFactory.getLogger(Wikidata.class);
  private static final Pattern wikidataIRIMatcher = Pattern.compile("http://www.wikidata.org/entity/Q([0-9]+)");
  private static final Pattern qidPattern = Pattern.compile("Q([0-9]+)");
  private final Counter.Readable blocks = Counter.newMultiThreadCounter();
  private final Counter.Readable nodes = Counter.newMultiThreadCounter();
  private final Counter.Readable ways = Counter.newMultiThreadCounter();
  private final Counter.Readable rels = Counter.newMultiThreadCounter();
  private final Counter.Readable wikidatas = Counter.newMultiThreadCounter();
  private final Counter.Readable batches = Counter.newMultiThreadCounter();
  private final LongSet visited = new LongHashSet();
  private final List<Long> qidsToFetch;
  private final Writer writer;
  private final Client client;
  private final int batchSize;
  private final Profile profile;
  private final PlanetilerConfig config;

  Wikidata(Writer writer, Client client, int batchSize, Profile profile, PlanetilerConfig config) {
    this.writer = writer;
    this.client = client;
    this.batchSize = batchSize;
    this.profile = profile;
    this.config = config;
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
  public static void fetch(OsmInputFile infile, Path outfile, PlanetilerConfig config, Profile profile, Stats stats) {
    var timer = stats.startStage("wikidata");
    int processThreads = Math.max(1, config.threads() - 1);
    LOGGER.info("Starting with " + processThreads + " process threads");

    WikidataTranslations oldMappings = load(outfile);
    try (
      Writer writer = Files.newBufferedWriter(outfile);
      OsmBlockSource osmSource = infile.get()
    ) {
      HttpClient client = HttpClient.newBuilder().connectTimeout(config.httpTimeout()).build();
      Wikidata fetcher = new Wikidata(writer, Client.wrap(client), 5_000, profile, config);
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
      LOGGER.info("DONE fetched:" + fetcher.wikidatas.get());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    timer.stop();
  }

  /**
   * Returns translations parsed from {@code path} that was written by a previous run of the downloader.
   */
  public static WikidataTranslations load(Path path) {
    Timer timer = Timer.start();
    if (!Files.exists(path)) {
      LOGGER.info("no wikidata translations found, run with --fetch-wikidata to download");
      return new WikidataTranslations();
    } else {
      try (BufferedReader fis = Files.newBufferedReader(path)) {
        WikidataTranslations result = load(fis);
        LOGGER.info(
          "loaded from " + result.getAll().size() + " mappings from " + path.toAbsolutePath() + " in " + timer.stop());
        return result;
      } catch (IOException e) {
        LOGGER.info("error loading " + path.toAbsolutePath() + ": " + e);
        return new WikidataTranslations();
      }
    }
  }

  /**
   * Returns translations parsed from {@code reader} where each line is a JSON array where first element is the ID and
   * second element is a map from language to translation.
   */
  static WikidataTranslations load(BufferedReader reader) throws IOException {
    WikidataTranslations mappings = new WikidataTranslations();
    String line;
    while ((line = reader.readLine()) != null) {
      JsonNode node = objectMapper.readTree(line);
      long id = Long.parseLong(node.get(0).asText());
      ObjectNode theseMappings = (ObjectNode) node.get(1);
      theseMappings.fields().forEachRemaining(entry -> mappings.put(id, entry.getKey(), entry.getValue().asText()));
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
      throw new RuntimeException("Unexpected response IRI: " + iri);
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
        if (wikidata instanceof String wikidataString) {
          if (profile.caresAboutWikidataTranslation(elem)) {
            long qid = parseQid(wikidataString);
            if (qid > 0) {
              next.accept(qid);
            }
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
      writeTranslations(results);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throwFatalException(e);
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
  private LongObjectMap<Map<String, String>> queryWikidata(List<Long> qidsToFetch)
    throws IOException, InterruptedException {
    if (qidsToFetch.isEmpty()) {
      return Hppc.newLongObjectHashMap();
    }
    String qidList = qidsToFetch.stream().map(id -> "wd:Q" + id).collect(Collectors.joining(" "));
    String query = """
      SELECT ?id ?label where {
        VALUES ?id { %s } ?id (owl:sameAs* / rdfs:label) ?label
      }
      """.formatted(qidList).replaceAll("\\s+", " ").trim();

    HttpRequest request = HttpRequest.newBuilder(URI.create("https://query.wikidata.org/bigdata/namespace/wdq/sparql"))
      .timeout(config.httpTimeout())
      .header(USER_AGENT, config.httpUserAgent())
      .header(ACCEPT, "application/sparql-results+json")
      .header(CONTENT_TYPE, "application/sparql-query")
      .POST(HttpRequest.BodyPublishers.ofString(query, StandardCharsets.UTF_8))
      .build();

    LongObjectMap<Map<String, String>> result = null;
    for (int i = 0; i <= config.httpRetries() && result == null; i++) {
      try {
        var response = client.send(request);
        if (response != null) {
          try (var bis = new BufferedInputStream(response)) {
            result = parseResults(bis);
          }
        }
      } catch (IOException e) {
        boolean lastTry = i == config.httpRetries();
        if (!lastTry) {
          LOGGER.warn("sparql query failed, retrying: " + e);
        } else {
          LOGGER.error("sparql query failed, exhausted retries: " + e);
          throw e;
        }
        if (e.getMessage() != null && e.getMessage().contains("GOAWAY")) {
          try {
            LOGGER.debug("GOAWAY received, waiting {} ms to give server some room", config.httpRetryWait());
            Thread.sleep(config.httpRetryWait());
          } catch (InterruptedException e2) {
            Thread.currentThread().interrupt();
          }
        }
      }
    }

    if (result != null) {
      return result;
    } else {
      throw new IllegalStateException("No response or exception"); // should never happen
    }
  }

  void loadExisting(WikidataTranslations oldMappings) throws IOException {
    LongObjectMap<Map<String, String>> alreadyHave = oldMappings.getAll();
    if (!alreadyHave.isEmpty()) {
      LOGGER.info("skipping " + alreadyHave.size() + " mappings we already have");
      writeTranslations(alreadyHave);
      for (LongObjectCursor<Map<String, String>> cursor : alreadyHave) {
        visited.add(cursor.key);
      }
    }
  }

  /** Flushes a batch of translations to disk. */
  private void writeTranslations(LongObjectMap<Map<String, String>> results) throws IOException {
    for (LongObjectCursor<Map<String, String>> cursor : results) {
      writer.write(objectMapper.writeValueAsString(List.of(
        Long.toString(cursor.key),
        cursor.value
      )));
      writer.write(System.lineSeparator());
    }
    writer.flush();
  }

  /** Abstraction over HTTP client so tests can easily mock-out the client's response. */
  interface Client {

    static Client wrap(HttpClient client) {
      return (req) -> {
        var response = client.send(req, BodyHandlers.ofInputStream());
        if (response.statusCode() >= 400) {
          String body;
          try {
            body = new String(response.body().readAllBytes(), StandardCharsets.UTF_8);
          } catch (IOException e) {
            body = "Error reading body: " + e;
          }
          throw new IOException("Error reading " + response.statusCode() + ": " + body);
        }
        String encoding = response.headers().firstValue("Content-Encoding").orElse("");
        InputStream is = response.body();
        is = switch (encoding) {
          case "gzip" -> new GZIPInputStream(is);
          case "deflate" -> new InflaterInputStream(is, new Inflater(true));
          default -> is;
        };
        return is;
      };
    }

    InputStream send(HttpRequest req) throws IOException, InterruptedException;
  }

  public static class WikidataTranslations implements Translations.TranslationProvider {

    private final LongObjectMap<Map<String, String>> data = Hppc.newLongObjectHashMap();

    public WikidataTranslations() {}

    /** Returns a map from language code to translated name for {@code qid}. */
    public Map<String, String> get(long qid) {
      return data.get(qid);
    }

    /** Returns all maps from language code to translated name for {@code qid}. */
    public LongObjectMap<Map<String, String>> getAll() {
      return data;
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
