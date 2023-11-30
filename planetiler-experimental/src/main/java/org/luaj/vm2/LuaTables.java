package org.luaj.vm2;

public class LuaTables {

  /**
   * Returns true if the lua table is a list, and not a map.
   */
  public static boolean isArray(LuaValue v) {
    return v instanceof LuaTable table && table.getArrayLength() > 0;
  }
}
