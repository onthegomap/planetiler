package com.onthegomap.flatmap;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongObjectHashMap;
import com.carrotsearch.hppc.LongObjectMap;
import com.carrotsearch.hppc.LongSet;
import com.carrotsearch.hppc.cursors.LongObjectCursor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.graphhopper.coll.GHLongObjectHashMap;
import com.graphhopper.reader.ReaderElement;
import com.graphhopper.util.StopWatch;
import com.onthegomap.flatmap.monitoring.ProgressLoggers;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.read.OsmInputFile;
import com.onthegomap.flatmap.worker.Topology;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Wikidata {

  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final Logger LOGGER = LoggerFactory.getLogger(Wikidata.class);
  private static final Pattern wikidataIRIMatcher = Pattern.compile("http://www.wikidata.org/entity/Q([0-9]+)");
  private static final Pattern qidPattern = Pattern.compile("Q([0-9]+)");
  private final AtomicLong nodes = new AtomicLong(0);
  private final AtomicLong ways = new AtomicLong(0);
  private final AtomicLong rels = new AtomicLong(0);
  private final AtomicLong wikidatas = new AtomicLong(0);
  private final AtomicLong batches = new AtomicLong(0);
  private final LongSet visited = new LongHashSet();
  private final List<Long> qidsToFetch;
  private final Writer writer;
  private final Client client;
  private final int batchSize;

  public Wikidata(Writer writer, Client client, int batchSize) {
    this.writer = writer;
    this.client = client;
    this.batchSize = batchSize;
    qidsToFetch = new ArrayList<>(batchSize);
  }

  public interface Client {

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

  public static LongObjectMap<Map<String, String>> parseResults(InputStream results) throws IOException {
    JsonNode node = objectMapper.readTree(results);
    ArrayNode bindings = (ArrayNode) node.get("results").get("bindings");
    LongObjectMap<Map<String, String>> resultMap = new LongObjectHashMap<>();
    bindings.elements().forEachRemaining(row -> {
      long id = extractIdFromWikidataIRI(row.get("id").get("value").asText());
      Map<String, String> map = resultMap.get(id);
      if (map == null) {
        resultMap.put(id, map = new TreeMap<>());
      }
      JsonNode label = row.get("label");
      map.put(
        "name:" + label.get("xml:lang").asText(),
        label.get("value").asText()
      );
    });
    return resultMap;
  }

  public static void fetch(OsmInputFile infile, Path outfile, CommonParams config, Profile profile, Stats stats) {
    int threads = config.threads();
    stats.startTimer("wikidata");
    int readerThreads = Math.max(1, Math.min(4, threads * 3 / 4));
    int processThreads = Math.max(1, Math.min(4, threads / 4));
    LOGGER
      .info("[wikidata] Starting with " + readerThreads + " reader threads and " + processThreads + " process threads");

    WikidataTranslations oldMappings = load(outfile);
    try (Writer writer = Files.newBufferedWriter(outfile)) {
      HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
      Wikidata fetcher = new Wikidata(writer, Client.wrap(client), 5_000);
      fetcher.loadExisting(oldMappings);

      var topology = Topology.start("wikidata", stats)
        .fromGenerator("pbf", infile.read(readerThreads))
        .addBuffer("reader_queue", 50_000, 10_000)
        .addWorker("filter", processThreads, fetcher::filter)
        .addBuffer("fetch_queue", 50_000)
        .sinkTo("fetch", 1, prev -> {
          Long id;
          while ((id = prev.get()) != null) {
            fetcher.fetch(id);
          }
          fetcher.flush();
        });

      ProgressLoggers loggers = new ProgressLoggers("wikidata")
        .addRateCounter("nodes", fetcher.nodes)
        .addRateCounter("ways", fetcher.ways)
        .addRateCounter("rels", fetcher.rels)
        .addRateCounter("wiki", fetcher.wikidatas)
        .addFileSize(outfile)
        .addProcessStats()
        .addThreadPoolStats("parse", "pool-")
        .addTopologyStats(topology);

      topology.awaitAndLog(loggers, config.logInterval());
      LOGGER.info("[wikidata] DONE fetched:" + fetcher.wikidatas.get());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    stats.stopTimer("wikidata");
  }

  public static WikidataTranslations load(Path path) {
    StopWatch watch = new StopWatch().start();
    try (BufferedReader fis = Files.newBufferedReader(path)) {
      WikidataTranslations result = load(fis);
      LOGGER.info(
        "[wikidata] loaded from " + result.getAll().size() + " mappings from " + path.toAbsolutePath() + " in " + watch
          .stop());
      return result;
    } catch (IOException e) {
      LOGGER.info("[wikidata] error loading " + path.toAbsolutePath() + ": " + e);
      return new WikidataTranslations();
    }
  }

  public static WikidataTranslations load(BufferedReader reader) throws IOException {
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

  private static long extractIdFromWikidataIRI(String iri) {
    Matcher matcher = wikidataIRIMatcher.matcher(iri);
    if (matcher.matches()) {
      String idText = matcher.group(1);
      return Long.parseLong(idText);
    } else {
      throw new RuntimeException("Unexpected response IRI: " + iri);
    }
  }

  public static long parseQid(String qid) {
    long result = 0;
    if (qid != null) {
      Matcher matcher = qidPattern.matcher(qid);
      if (matcher.matches()) {
        String idString = matcher.group(1);
        result = Parse.parseLong(idString);
      }
    }
    return result;
  }

  private void filter(Supplier<ReaderElement> prev, Consumer<Long> next) {
    TrackUsageMapping qidTracker = new TrackUsageMapping();
    ReaderElement elem;
    while ((elem = prev.get()) != null) {
      switch (elem.getType()) {
        case ReaderElement.NODE -> nodes.incrementAndGet();
        case ReaderElement.WAY -> ways.incrementAndGet();
        case ReaderElement.RELATION -> rels.incrementAndGet();
      }
      if (elem.hasTag("wikidata")) {
        qidTracker.qid = 0;
        // TODO send reader element through profile
        qidTracker.getNameTranslations(elem);
        if (qidTracker.qid > 0) {
          next.accept(qidTracker.qid);
        }
      }
    }
  }

  public void flush() {
    try {
      StopWatch timer = new StopWatch().start();
      LongObjectMap<Map<String, String>> results = queryWikidata(qidsToFetch);
      LOGGER.info("Fetched batch " + batches.incrementAndGet() + " (" + qidsToFetch.size() + " qids) " + timer.stop());
      writeTranslations(results);
    } catch (IOException | InterruptedException | URISyntaxException e) {
      throw new RuntimeException(e);
    }
    wikidatas.addAndGet(qidsToFetch.size());
    qidsToFetch.clear();
  }

  public void fetch(long id) {
    if (!visited.contains(id)) {
      visited.add(id);
      qidsToFetch.add(id);
    }
    if (qidsToFetch.size() >= batchSize) {
      flush();
    }
  }

  private LongObjectMap<Map<String, String>> queryWikidata(List<Long> qidsToFetch)
    throws IOException, InterruptedException, URISyntaxException {
    if (qidsToFetch.isEmpty()) {
      return new GHLongObjectHashMap<>();
    }
    String qidList = qidsToFetch.stream().map(id -> "wd:Q" + id).collect(Collectors.joining(" "));
    String query = """
      SELECT ?id ?label where {
        VALUES ?id { %s } ?id (owl:sameAs* / rdfs:label) ?label
      }
      """.formatted(qidList).replaceAll("\\s+", " ");

    HttpRequest request = HttpRequest.newBuilder(URI.create("https://query.wikidata.org/bigdata/namespace/wdq/sparql"))
      .timeout(Duration.ofSeconds(30))
      .header("User-Agent", "OpenMapTiles OSM name resolver (https://github.com/openmaptiles/openmaptiles)")
      .header("Accept", "application/sparql-results+json")
      .header("Content-Type", "application/sparql-query")
      .POST(HttpRequest.BodyPublishers.ofString(query, StandardCharsets.UTF_8))
      .build();
    InputStream response = client.send(request);

    try (var bis = new BufferedInputStream(response)) {
      return parseResults(bis);
    }
  }

  public void loadExisting(WikidataTranslations oldMappings) throws IOException {
    LongObjectMap<Map<String, String>> alreadyHave = oldMappings.getAll();
    if (!alreadyHave.isEmpty()) {
      LOGGER.info("[wikidata] skipping " + alreadyHave.size() + " mappings we already have");
      writeTranslations(alreadyHave);
      for (LongObjectCursor<Map<String, String>> cursor : alreadyHave) {
        visited.add(cursor.key);
      }
    }
  }

  public void writeTranslations(LongObjectMap<Map<String, String>> results) throws IOException {
    for (LongObjectCursor<Map<String, String>> cursor : results) {
      writer.write(objectMapper.writeValueAsString(List.of(
        Long.toString(cursor.key),
        cursor.value
      )));
      writer.write("\n");
    }
    writer.flush();
  }

  public static class WikidataTranslations implements Translations.TranslationProvider {

    private final LongObjectMap<Map<String, String>> data = new GHLongObjectHashMap<>();

    public WikidataTranslations() {
    }

    public Map<String, String> get(long qid) {
      return data.get(qid);
    }

    public LongObjectMap<Map<String, String>> getAll() {
      return data;
    }

    public void put(long qid, String lang, String value) {
      Map<String, String> map = data.get(qid);
      if (map == null) {
        data.put(qid, map = new TreeMap<>());
      }
      map.put(lang, value);
    }

    @Override
    public Map<String, String> getNameTranslations(ReaderElement elem) {
      long wikidataId = parseQid(elem.getTag("wikidata"));
      if (wikidataId > 0) {
        return get(wikidataId);
      }
      return null;
    }
  }

  private static class TrackUsageMapping extends WikidataTranslations {

    public long qid = 0;

    @Override
    public Map<String, String> get(long qid) {
      this.qid = qid;
      return null;
    }
  }
}
