package com.onthegomap.planetiler.overture;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;

public class StructSerializer extends StdSerializer<Struct> {

  public StructSerializer() {
    this(null);
  }

  public StructSerializer(Class<Struct> t) {
    super(t);
  }

  @Override
  public void serialize(
    Struct value, JsonGenerator jgen, SerializerProvider provider)
    throws IOException {
    jgen.writePOJO(value.rawValue());
  }
}

