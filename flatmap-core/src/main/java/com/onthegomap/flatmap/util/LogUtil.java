package com.onthegomap.flatmap.util;

import java.util.regex.Pattern;
import org.slf4j.MDC;

public class LogUtil {

  public static void setStage(String stage) {
    MDC.put("stage", stage);
  }

  public static void clearStage() {
    MDC.remove("stage");
  }

  public static String getStage() {
    return MDC.get("stage");
  }

  public static void setStage(String parent, String childThread) {
    if (parent == null) {
      setStage(childThread);
    } else {
      setStage(parent + ":" + childThread.replaceFirst("^" + Pattern.quote(parent) + "_?", ""));
    }
  }
}
