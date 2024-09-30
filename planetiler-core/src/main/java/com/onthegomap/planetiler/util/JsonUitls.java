package com.onthegomap.planetiler.util;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_ABSENT;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

/**
 * @author: xmm
 * @date: 2024/9/6 17:48
 */
public class JsonUitls {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
    .registerModules(new Jdk8Module())
    .setSerializationInclusion(NON_ABSENT);

  private static final ObjectWriter PRETTY_WRITER = OBJECT_MAPPER.writerWithDefaultPrettyPrinter();

  /**
   * 将对象转换为 JSON 字符串。
   *
   * @return JSON 格式的字符串表示
   */
  public static String toJsonString(Object o) {
    try {
      return OBJECT_MAPPER.writeValueAsString(o);
    } catch (Exception e) {
      throw new RuntimeException("Error converting TileArchiveMetadataJson to JSON string", e);
    }
  }

  /**
   * 将对象转换为格式化的（美化的）JSON 字符串。
   *
   * @return 格式化的 JSON 字符串表示
   */
  public static String toPrettyJsonString(Object o) {
    try {
      return PRETTY_WRITER.writeValueAsString(o);
    } catch (Exception e) {
      throw new RuntimeException("Error converting TileArchiveMetadataJson to pretty JSON string", e);
    }
  }
}
