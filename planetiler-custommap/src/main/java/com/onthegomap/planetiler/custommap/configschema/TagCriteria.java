package com.onthegomap.planetiler.custommap.configschema;

import com.onthegomap.planetiler.reader.SourceFeature;
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

  /**
   * Determines whether a source feature matches this specification
   * 
   * @param sf source feature
   * @return true if this filter matches
   */
  public boolean match(SourceFeature sf) {
    return sf.hasTag(key) && value.contains(sf.getTag(key));
  }
}
