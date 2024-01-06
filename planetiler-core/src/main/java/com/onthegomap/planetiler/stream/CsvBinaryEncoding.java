package com.onthegomap.planetiler.stream;

import com.google.common.base.Suppliers;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

enum CsvBinaryEncoding {

  BASE64("base64", () -> Base64.getEncoder()::encodeToString, () -> Base64.getDecoder()::decode),
  HEX("hex", () -> HexFormat.of()::formatHex, () -> HexFormat.of()::parseHex);

  private final String id;
  private final Supplier<Function<byte[], String>> encoder;
  private final Supplier<Function<String, byte[]>> decoder;

  private CsvBinaryEncoding(String id, Supplier<Function<byte[], String>> encoder,
    Supplier<Function<String, byte[]>> decoder) {
    this.id = id;
    this.encoder = Suppliers.memoize(encoder::get);
    this.decoder = Suppliers.memoize(decoder::get);
  }

  String encode(byte[] b) {
    return encoder.get().apply(b);
  }

  byte[] decode(String s) {
    return decoder.get().apply(s);
  }

  static List<String> ids() {
    return Stream.of(CsvBinaryEncoding.values()).map(CsvBinaryEncoding::id).toList();
  }

  static CsvBinaryEncoding fromId(String id) {
    return Stream.of(CsvBinaryEncoding.values())
      .filter(de -> de.id().equals(id))
      .findFirst()
      .orElseThrow(() -> new IllegalArgumentException(
        "unexpected binary encoding - expected one of " + ids() + " but got " + id));
  }

  String id() {
    return id;
  }
}
