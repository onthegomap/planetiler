package com.onthegomap.planetiler.experimental.lua;

import static com.onthegomap.planetiler.experimental.lua.LuaConversions.toJava;
import static com.onthegomap.planetiler.experimental.lua.LuaConversions.toJavaMap;
import static com.onthegomap.planetiler.experimental.lua.LuaConversions.toLua;

import com.onthegomap.planetiler.FeatureMerge;
import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.expression.Expression;
import com.onthegomap.planetiler.expression.MultiExpression;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.BuildInfo;
import com.onthegomap.planetiler.util.LanguageUtils;
import com.onthegomap.planetiler.util.Parse;
import com.onthegomap.planetiler.util.SortKey;
import com.onthegomap.planetiler.util.Translations;
import com.onthegomap.planetiler.util.ZoomFunction;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.lib.jse.ExtraPlanetilerCoercions;
import org.luaj.vm2.lib.jse.JsePlatform;
import org.luaj.vm2.lib.jse.LuaBindMethods;
import org.luaj.vm2.lib.jse.LuaFunctionType;
import org.luaj.vm2.lib.jse.LuaGetter;
import org.luaj.vm2.lib.jse.LuaSetter;
import org.luaj.vm2.lib.jse.LuaType;
import org.luaj.vm2.luajc.LuaJC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Global variables exposed to lua scripts.
 * <p>
 * All instance fields annotated with {@link ExposeToLua} will be exposed to lua as global variables.
 */
@SuppressWarnings({"java:S1104", "java:S116", "unused", "java:S100"})
public class LuaEnvironment {

  private static final Logger LOGGER = LoggerFactory.getLogger(LuaEnvironment.class);
  static final Set<Class<?>> CLASSES_TO_EXPOSE = Set.of(
    ZoomFunction.class,
    FeatureMerge.class,
    Parse.class,
    LanguageUtils.class,
    Expression.class,
    MultiExpression.class,
    GeoUtils.class,
    SortKey.class
  );
  @ExposeToLua
  public final PlanetilerNamespace planetiler;
  final Planetiler runner;
  public LuaProfile profile;
  public LuaValue main;

  public LuaEnvironment(Planetiler runner) {
    this.runner = runner;
    this.planetiler = new PlanetilerNamespace();
  }

  public static LuaEnvironment loadScript(Arguments arguments, Path script) throws IOException {
    return loadScript(arguments, Files.readString(script), script.getFileName().toString(), Map.of(),
      ConcurrentHashMap.newKeySet(), script);
  }

  public static LuaEnvironment loadScript(Arguments args, Path scriptPath, Set<Path> pathsToWatch) throws IOException {
    return loadScript(args, Files.readString(scriptPath), scriptPath.getFileName().toString(), Map.of(), pathsToWatch,
      scriptPath);
  }

  public static LuaEnvironment loadScript(Arguments arguments, String script, String fileName) {
    return loadScript(arguments, script, fileName, Map.of(), ConcurrentHashMap.newKeySet(), Path.of("."));
  }

  public static LuaEnvironment loadScript(Arguments arguments, String script, String fileName, Map<String, ?> extras,
    Set<Path> filesLoaded, Path scriptPath) {
    ExtraPlanetilerCoercions.install();
    boolean luajc = arguments.getBoolean("luajc", "compile lua to java bytecode", true);
    Globals globals = JsePlatform.standardGlobals();
    if (luajc) {
      LuaJC.install(globals);
    }
    Planetiler runner = Planetiler.create(arguments);
    LuaEnvironment env = new LuaEnvironment(runner);
    env.install(globals);
    extras.forEach((name, java) -> globals.set(name, toLua(java)));
    var oldFilder = globals.finder;
    globals.finder = filename -> {
      Path path = Path.of(filename);
      if (!Files.exists(path)) {
        path = scriptPath.resolveSibling(filename);
      }
      filesLoaded.add(path);
      return oldFilder.findResource(path.toString());
    };
    // ensure source is treated as UTF-8
    try (var in = new ByteArrayInputStream(script.getBytes(StandardCharsets.UTF_8))) {
      globals.load(in, fileName, "t", globals).call();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    LuaProfile profile = new LuaProfile(env);
    env.profile = new LuaProfile(env);
    env.main = globals.get("main");
    return env;
  }

  public static LuaEnvironment loadScript(Arguments args, String script, String filename, Map<String, ?> map) {
    return loadScript(args, script, filename, map, ConcurrentHashMap.newKeySet(), Path.of(filename));
  }

  public void run() throws Exception {
    runner.setProfile(profile);
    if (main != null && main.isfunction()) {
      main.call(toLua(runner));
    } else {
      runner.overwriteOutput(planetiler.output.path).run();
    }
  }

  public void install(Globals globals) {
    for (var field : getClass().getDeclaredFields()) {
      var annotation = field.getAnnotation(ExposeToLua.class);
      if (annotation != null) {
        String name = annotation.value().isBlank() ? field.getName() : annotation.value();
        try {
          globals.set(name, toLua(field.get(this)));
        } catch (IllegalAccessException e) {
          throw new IllegalStateException(e);
        }
      }
    }
    for (var clazz : CLASSES_TO_EXPOSE) {
      globals.set(clazz.getSimpleName(), toLua(clazz));
    }
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.FIELD)
  public @interface ExposeToLua {

    String value() default "";
  }

  public static class PlanetilerOutput {
    public Path path = Path.of("data", "output.mbtiles");
    public String name;
    public String description;
    public String attribution;
    public String version;
    public boolean is_overlay;
  }

  @LuaBindMethods
  public class PlanetilerNamespace {
    public final BuildInfo build = BuildInfo.get();
    public final PlanetilerConfig config = runner.config();
    public final Stats stats = runner.stats();
    public final Arguments args = runner.arguments();
    public final PlanetilerOutput output = new PlanetilerOutput();
    @LuaFunctionType(target = LuaProfile.class)
    public LuaValue process_feature;
    @LuaFunctionType(target = LuaProfile.class)
    public LuaValue cares_about_source;
    @LuaFunctionType(target = LuaProfile.class)
    public LuaValue cares_about_wikidata_translation;
    @LuaFunctionType(target = LuaProfile.class)
    public LuaValue estimate_ram_required;
    @LuaFunctionType(target = LuaProfile.class)
    public LuaValue estimate_intermediate_disk_bytes;
    @LuaFunctionType(target = LuaProfile.class)
    public LuaValue estimate_output_bytes;
    @LuaFunctionType(target = LuaProfile.class)
    public LuaValue finish;
    @LuaFunctionType(target = LuaProfile.class)
    public LuaValue preprocess_osm_node;
    @LuaFunctionType(target = LuaProfile.class)
    public LuaValue preprocess_osm_way;
    @LuaFunctionType(target = LuaProfile.class)
    public LuaValue preprocess_osm_relation;
    @LuaFunctionType(target = LuaProfile.class)
    public LuaValue release;
    @LuaFunctionType(target = LuaProfile.class, method = "postProcessLayerFeatures")
    public LuaValue post_process;
    public String examples;

    private static <T> T get(LuaValue map, String key, Class<T> clazz) {
      LuaValue value = map.get(key);
      return value.isnil() ? null : toJava(value, clazz);
    }

    @LuaGetter
    public Translations translations() {
      return runner.translations();
    }

    @LuaGetter
    public List<String> languages() {
      return runner.getDefaultLanguages();
    }

    @LuaSetter
    public void languages(List<String> languages) {
      runner.setDefaultLanguages(languages);
    }

    public void fetch_wikidata_translations(Path defaultPath) {
      runner.fetchWikidataNameTranslations(defaultPath);
    }

    public void fetch_wikidata_translations() {
      runner.fetchWikidataNameTranslations(Path.of("data", "sources", "wikidata_names.json"));
    }

    public void add_source(
      String name,
      @LuaType("{type: 'osm'|'shapefile'|'geopackage'|'naturalearth', path: string|string[], url: string, projection: string, glob: string}") LuaValue map) {
      String type = get(map, "type", String.class);
      Path path = get(map, "path", Path.class);
      if (name == null || type == null) {
        throw new IllegalArgumentException("Sources must have 'type', got: " + toJavaMap(map));
      }
      String url = get(map, "url", String.class);
      String projection = get(map, "projection", String.class);
      String glob = get(map, "glob", String.class);
      if (path == null) {
        if (url == null) {
          throw new IllegalArgumentException(
            "Sources must have either a 'url' or local 'path', got: " + toJavaMap(map));
        }
        String filename = url
          .replaceFirst("^https?://", "")
          .replaceAll("[\\W&&[^.]]+", "_");
        if (type.equals("osm") && !filename.endsWith(".pbf")) {
          filename = filename + ".osm.pbf";
        }
        path = Path.of("data", "sources", filename);
      }
      switch (type) {
        case "osm" -> runner.addOsmSource(name, path, url);
        case "shapefile" -> {
          if (glob != null) {
            runner.addShapefileGlobSource(projection, name, path, glob, url);
          } else {
            runner.addShapefileSource(projection, name, path, url);
          }
        }
        case "geopackage" -> runner.addGeoPackageSource(projection, name, path, url);
        case "natural_earth" -> runner.addNaturalEarthSource(name, path, url);
        default -> throw new IllegalArgumentException("Unrecognized source type: " + type);
      }
    }
  }
}
