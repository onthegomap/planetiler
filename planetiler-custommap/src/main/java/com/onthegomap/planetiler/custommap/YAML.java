package com.onthegomap.planetiler.custommap;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.yaml.snakeyaml.Yaml;

public class YAML {

  private static final Yaml snakeYaml = new Yaml();
  private static final ObjectMapper jackson = new ObjectMapper();

  public static <T> T load(Path file, Class<T> clazz) {
    try (var schemaStream = Files.newInputStream(file)) {
      return load(schemaStream, clazz);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static <T> T load(InputStream stream, Class<T> clazz) {
    try (stream) {
      Map<String, Object> parsed = snakeYaml.load(stream);
      return jackson.convertValue(parsed, clazz);
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
}
