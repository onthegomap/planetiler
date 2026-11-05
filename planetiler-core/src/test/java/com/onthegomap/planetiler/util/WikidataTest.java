package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

class WikidataTest {

  final PlanetilerConfig config = PlanetilerConfig.from(Arguments.fromArgs("--http-retries=1"));
  final Profile profile = new Profile.NullProfile();
  static final String URL = "https://query.wikidata.org/bigdata/namespace/wdq/sparql";
  final String response = """
    {
      "head" : {
        "vars" : [ "id", "label" ]
      },
      "results" : {
        "bindings" : [ {
          "id" : {
            "type" : "uri",
            "value" : "http://www.wikidata.org/entity/Q1"
          },
          "label" : {
            "xml:lang" : "en",
            "type" : "literal",
            "value" : "en name"
          }
        }, {
          "id" : {
            "type" : "uri",
            "value" : "http://www.wikidata.org/entity/Q1"
          },
          "label" : {
            "xml:lang" : "es",
            "type" : "literal",
            "value" : "es name"
          }
        }, {
          "id" : {
            "type" : "uri",
            "value" : "http://www.wikidata.org/entity/Q2"
          },
          "label" : {
            "xml:lang" : "es",
            "type" : "literal",
            "value" : "es name2"
          }
        } ]
      }
    }
    """;
  final String wikidataNamesLegacyJson = """
    ["1",{"en":"English 1","de":"Deutch 1"}]
    ["2",{"en":"English 2","de":"Deutch 2"}]
    ["3",{"en":"English 3","de":"Deutch 3"}]
    """;
  final Clock clock = Clock.fixed(Instant.ofEpochMilli(60_000), ZoneId.of("UTC"));
  final String wikidataNamesJson = """
    ["1",{"en":"English 1","de":"Deutch 1"},55000]
    ["2",{"en":"English 2","de":"Deutch 2"},30000]
    ["3",{"en":"English 3","de":"Deutch 3"},30000]
    """;

  @Test
  void testWikidataTranslations() {
    var expected = Map.of("en", "en value", "es", "es value");
    Wikidata.WikidataTranslations translations = new Wikidata.WikidataTranslations();
    assertNull(translations.get(1));
    translations.put(1, "en", "en value");
    translations.put(1, "es", "es value");
    assertEquals(expected, translations.get(1));
    Map<String, Object> elem = new HashMap<>();
    assertNull(translations.getNameTranslations(elem));
    elem.put("wikidata", "Qgarbage");
    assertNull(translations.getNameTranslations(elem));
    elem.put("wikidata", "Q1");
    assertEquals(expected, translations.getNameTranslations(elem));
  }

  @TestFactory
  List<DynamicTest> testFetchWikidata() throws IOException {
    StringWriter writer = new StringWriter();
    var downloader = new StubDownloader(config);
    Wikidata fixture = new Wikidata(writer, downloader, 2, profile);
    fixture.fetch(1L);
    assertEquals(List.of(), downloader.sent);
    downloader.put(URL, response.getBytes(StandardCharsets.UTF_8));
    fixture.fetch(2L);

    return List.of(
      dynamicTest("verify http response", () -> {
        assertEquals(1, downloader.sent.size());
        Downloader.Request request = downloader.sent.getFirst();
        assertEquals(URL, request.url());
        assertEquals("application/sparql-results+json", request.headers().get("Accept"));
        assertEquals("application/sparql-query", request.headers().get("Content-Type"));
        String body = request.body();
        assertEqualsIgnoringWhitespace("""
          SELECT ?id ?label where {
            VALUES ?id { wd:Q1 wd:Q2 } ?id (owl:sameAs* / rdfs:label) ?label
          }
          """, body);
      }),
      dynamicTest("can load serialized data", () -> {
        var translations = Wikidata.load(new BufferedReader(new StringReader(writer.toString())));
        assertEquals(Map.of("en", "en name", "es", "es name"), translations.get(1));
        assertEquals(Map.of("es", "es name2"), translations.get(2));
      }),
      dynamicTest("do not re-request on subsequent loads", () -> {
        StringWriter writer2 = new StringWriter();
        var downloader2 = new StubDownloader(config);
        Wikidata fixture2 = new Wikidata(writer2, downloader2, 2, profile);
        fixture2.loadExisting(Wikidata.load(new BufferedReader(new StringReader(writer.toString()))));
        fixture2.fetch(1L);
        fixture2.fetch(2L);
        fixture2.fetch(1L);
        fixture2.flush();
        assertEquals(List.of(), downloader2.sent);
      })
    );
  }

  @Test
  void testRetryFailedRequestOnce() throws IOException {
    StringWriter writer = new StringWriter();
    var downloader = new StubDownloader(config);
    downloader.put(URL, response.getBytes(StandardCharsets.UTF_8));
    Wikidata fixture = new Wikidata(writer, downloader, 1, profile);
    // fail once then succeed
    downloader.failNext(1, 503);
    fixture.fetch(1L);
    var translations = Wikidata.load(new BufferedReader(new StringReader(writer.toString())));
    assertEquals(Map.of("en", "en name", "es", "es name"), translations.get(1));
    assertEquals(Map.of("es", "es name2"), translations.get(2));

    // fail all subsequent requests
    downloader.failNext(Integer.MAX_VALUE, 503);
    var outerException = assertThrows(RuntimeException.class, () -> fixture.fetch(2L));
    var innerException = outerException.getCause();
    assertInstanceOf(IOException.class, innerException);
  }

  @Test
  void testLegacyWikidataNamesJson() throws IOException {
    try (var reader = new BufferedReader(new StringReader(wikidataNamesLegacyJson))) {
      // no timestamp + age limit set => all old => all should be dropped
      var translationsProvider = Wikidata.load(reader, Duration.ofSeconds(1), 0, clock);
      assertEquals(0, translationsProvider.getAll().size());
    }
  }

  @Test
  void testWikidataNamesJsonMaxAge() throws IOException {
    // 10s => item 1 is 5s old hence fresh, the rest is 30s old hence outdated
    Duration maxAge = Duration.ofSeconds(10);

    var reader = new BufferedReader(new StringReader(wikidataNamesJson));
    var translationsProvider = Wikidata.load(reader, maxAge, 0, clock);
    assertEquals(1, translationsProvider.getAll().size());
  }

  @Test
  void testWikidataNamesJsonUpdateLimit() throws IOException {
    Duration maxAge = Duration.ofSeconds(1);

    var reader = new BufferedReader(new StringReader(wikidataNamesJson));
    var translationsProvider = Wikidata.load(reader, maxAge, 1, clock);
    assertEquals(2, translationsProvider.getAll().size());
  }

  private static void assertEqualsIgnoringWhitespace(String expected, String actual) {
    assertEquals(ignoreWhitespace(expected), ignoreWhitespace(actual));
  }

  private static String ignoreWhitespace(String in) {
    return in == null ? null : in.replaceAll("\\s+", " ").trim();
  }
}
