package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodySubscriber;
import java.net.http.HttpResponse.BodySubscribers;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscription;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class WikidataTest {

  final PlanetilerConfig config = PlanetilerConfig.defaults();
  final Profile profile = new Profile.NullProfile();

  @Test
  public void testWikidataTranslations() {
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
  public List<DynamicTest> testFetchWikidata() throws IOException, InterruptedException {
    StringWriter writer = new StringWriter();
    Wikidata.Client client = Mockito.mock(Wikidata.Client.class, Mockito.RETURNS_SMART_NULLS);
    Wikidata fixture = new Wikidata(writer, client, 2, profile, config);
    fixture.fetch(1L);
    Mockito.verifyNoInteractions(client);
    Mockito.when(client.send(Mockito.any())).thenReturn(new ByteArrayInputStream("""
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
      """.getBytes(StandardCharsets.UTF_8)));
    fixture.fetch(2L);

    return List.of(
      dynamicTest("verify http response", () -> {
        ArgumentCaptor<HttpRequest> captor = ArgumentCaptor.forClass(HttpRequest.class);
        Mockito.verify(client).send(captor.capture());
        HttpRequest request = captor.getValue();
        assertEquals("https://query.wikidata.org/bigdata/namespace/wdq/sparql", request.uri().toString());
        assertEquals("POST", request.method());
        assertEquals(Optional.of("application/sparql-results+json"), request.headers().firstValue("Accept"));
        assertEquals(Optional.of("application/sparql-query"), request.headers().firstValue("Content-Type"));
        String body = getHttpRequestBody(request);
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
        Wikidata.Client client2 = Mockito.mock(Wikidata.Client.class, Mockito.RETURNS_SMART_NULLS);
        Wikidata fixture2 = new Wikidata(writer2, client2, 2, profile, config);
        fixture2.loadExisting(Wikidata.load(new BufferedReader(new StringReader(writer.toString()))));
        fixture2.fetch(1L);
        fixture2.fetch(2L);
        fixture2.fetch(1L);
        fixture2.flush();
        Mockito.verifyNoInteractions(client2);
      })
    );
  }

  private static void assertEqualsIgnoringWhitespace(String expected, String actual) {
    assertEquals(ignoreWhitespace(expected), ignoreWhitespace(actual));
  }

  private static String ignoreWhitespace(String in) {
    return in == null ? null : in.replaceAll("\\s+", " ").trim();
  }

  private String getHttpRequestBody(HttpRequest request) {
    BodySubscriber<String> stringSubscriber = BodySubscribers.ofString(StandardCharsets.UTF_8);
    request.bodyPublisher().ifPresent(p -> p.subscribe(new Flow.Subscriber<>() {
      @Override
      public void onSubscribe(Subscription subscription) {
        stringSubscriber.onSubscribe(subscription);
      }

      @Override
      public void onNext(ByteBuffer item) {
        stringSubscriber.onNext(List.of(item));
      }

      @Override
      public void onError(Throwable throwable) {
        stringSubscriber.onError(throwable);
      }

      @Override
      public void onComplete() {
        stringSubscriber.onComplete();
      }
    }));
    return stringSubscriber.getBody().toCompletableFuture().join();
  }
}
