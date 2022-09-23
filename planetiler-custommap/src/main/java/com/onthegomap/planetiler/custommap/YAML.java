package com.onthegomap.planetiler.custommap;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;

/**
 * Utility for parsing YAML files into java objects using snakeyaml to handle aliases and anchors and jackson to map
 * into java model objects.
 */
public class YAML {

  private YAML() {}

  private static final Load snakeYaml = new Load(LoadSettings.builder().build());
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
      return convertValue(parsed, clazz);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
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
