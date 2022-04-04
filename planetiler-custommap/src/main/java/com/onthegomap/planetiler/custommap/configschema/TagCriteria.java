package com.onthegomap.planetiler.custommap.configschema;

import java.util.Collection;

public class TagCriteria {
  private String key;
  private Collection<String> value;

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public Collection<String> getValue() {
    return value;
  }

  public void setValue(Collection<String> value) {
    this.value = value;
  }
}
