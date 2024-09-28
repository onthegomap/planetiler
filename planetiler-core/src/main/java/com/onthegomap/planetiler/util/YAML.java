package com.onthegomap.planetiler.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;

/**
 * Utility for parsing YAML files into java objects using snakeyaml to handle aliases and anchors and jackson to map
 * into java model objects.
 */
public class YAML {

  private YAML() {}

  private static final Load snakeYaml = new Load(LoadSettings.builder().setCodePointLimit(Integer.MAX_VALUE).build());
  public static final ObjectMapper jackson = new ObjectMapper();

  public static <T> T load(Path file, Class<T> clazz) {
    try (var schemaStream = Files.newInputStream(file)) {
      return load(schemaStream, clazz);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static <T> T load(InputStream stream, Class<T> clazz) {
    try (stream) {
      Object parsed = snakeYaml.loadFromInputStream(stream);
      handleMergeOperator(parsed);
      return convertValue(parsed, clazz);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void handleMergeOperator(Object parsed) {
    if (parsed instanceof Map map) {
      Object toMerge = map.remove("<<");
      if (toMerge != null) {
        mergeInto(map, toMerge);
      }
      for (var value : map.values()) {
        handleMergeOperator(value);
      }
    } else if (parsed instanceof List<?> list) {
      for (var item : list) {
        handleMergeOperator(item);
      }
    }
  }

  private static void mergeInto(Map dest, Object source) {
    if (source instanceof Map<?, ?> map) {
      for (var entry : map.entrySet()) {
        dest.putIfAbsent(entry.getKey(), entry.getValue());
      }
    } else if (source instanceof List<?> nesteds) {
      for (var nested : nesteds.reversed()) {
        mergeInto(dest, nested);
      }
    }
  }

  public static <T> T load(String config, Class<T> clazz) {
    try (var stream = new ByteArrayInputStream(config.getBytes(StandardCharsets.UTF_8))) {
      return load(stream, clazz);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static <T> T loadResource(String resourceName, Class<T> clazz) {
    try (var stream = YAML.class.getResourceAsStream(resourceName)) {
      return load(stream, clazz);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static <T> T convertValue(Object parsed, Class<T> clazz) {
    return jackson.convertValue(parsed, clazz);
  }
}
