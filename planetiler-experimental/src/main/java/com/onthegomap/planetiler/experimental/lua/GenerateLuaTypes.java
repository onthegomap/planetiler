package com.onthegomap.planetiler.experimental.lua;

import static java.util.Map.entry;

import com.google.common.base.CaseFormat;
import com.google.common.base.Converter;
import com.google.common.reflect.Invokable;
import com.google.common.reflect.TypeToken;
import com.onthegomap.planetiler.util.Format;
import java.lang.reflect.Executable;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.luaj.vm2.LuaDouble;
import org.luaj.vm2.LuaInteger;
import org.luaj.vm2.LuaNumber;
import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.lib.jse.LuaBindMethods;
import org.luaj.vm2.lib.jse.LuaFunctionType;
import org.luaj.vm2.lib.jse.LuaGetter;
import org.luaj.vm2.lib.jse.LuaSetter;
import org.luaj.vm2.lib.jse.LuaType;

/**
 * Generates a lua file with type definitions for the lua environment exposed by planetiler.
 *
 * <pre>
 * java -jar planetiler.jar lua-types > types.lua
 * </pre>
 *
 * @see <a href="https://luals.github.io/wiki/annotations/">Lua Language Server type annotations</a>
 */
public class GenerateLuaTypes {
  private static final Map<Class<?>, String> TYPE_NAMES = Map.ofEntries(
    entry(Object.class, "any"),
    entry(LuaInteger.class, "integer"),
    entry(LuaDouble.class, "number"),
    entry(LuaNumber.class, "number"),
    entry(LuaString.class, "string"),
    entry(LuaTable.class, "table"),
    entry(Class.class, "userdata"),
    entry(String.class, "string"),
    entry(Number.class, "number"),
    entry(byte[].class, "string"),
    entry(Integer.class, "integer"),
    entry(int.class, "integer"),
    entry(Long.class, "integer"),
    entry(long.class, "integer"),
    entry(Short.class, "integer"),
    entry(short.class, "integer"),
    entry(Byte.class, "integer"),
    entry(byte.class, "integer"),
    entry(Double.class, "number"),
    entry(double.class, "number"),
    entry(Float.class, "number"),
    entry(float.class, "number"),
    entry(boolean.class, "boolean"),
    entry(Boolean.class, "boolean"),
    entry(Void.class, "nil"),
    entry(void.class, "nil")
  );
  private static final Converter<String, String> CAMEL_TO_SNAKE_CASE =
    CaseFormat.LOWER_CAMEL.converterTo(CaseFormat.LOWER_UNDERSCORE);
  private static final TypeToken<List<?>> LIST_TYPE = new TypeToken<>() {};
  private static final TypeToken<Map<?, ?>> MAP_TYPE = new TypeToken<>() {};
  private final Deque<String> debugStack = new LinkedList<>();
  private final Set<String> handled = new HashSet<>();
  private final StringBuilder builder = new StringBuilder();
  private static final String NEWLINE = System.lineSeparator();

  GenerateLuaTypes() {
    write("""
      ---@meta
      local types = {}
      """);
  }

  public static void main(String[] args) {
    var generator = new GenerateLuaTypes().generatePlanetiler();
    System.out.println(generator);
  }

  private static String luaClassName(Class<?> clazz) {
    return clazz.getName().replaceAll("[\\$\\.]", "_");
  }

  private static boolean differentFromParents2(Invokable<?, ?> invokable, TypeToken<?> superType) {
    if (!invokable.getReturnType().equals(superType.resolveType(invokable.getReturnType().getType()))) {
      return true;
    }
    var orig =
      invokable.getParameters().stream()
        .map(t -> invokable.getOwnerType().resolveType(t.getType().getType()))
        .toList();
    var resolved = invokable.getParameters().stream().map(t -> superType.resolveType(t.getType().getType())).toList();
    return !orig.equals(resolved);
  }

  private static boolean hasMethod(Class<?> clazz, Method method) {
    try {
      clazz.getMethod(method.getName(), method.getParameterTypes());
      return true;
    } catch (NoSuchMethodException e) {
      return false;
    }
  }

  private static String transformMemberName(String fieldName) {
    if (isLowerCamelCase(fieldName)) {
      fieldName = CAMEL_TO_SNAKE_CASE.convert(fieldName);
    }
    if (LuaConversions.LUA_KEYWORDS.contains(fieldName)) {
      fieldName = Objects.requireNonNull(fieldName).toUpperCase(Locale.ROOT);
    }
    return fieldName;
  }

  private static boolean isLowerCamelCase(String fieldName) {
    var chars = fieldName.toCharArray();
    if (!Character.isLowerCase(chars[0])) {
      return false;
    }
    boolean upper = false, lower = false, underscore = false;
    for (char c : chars) {
      upper |= Character.isUpperCase(c);
      lower |= Character.isLowerCase(c);
      underscore |= c == '_';
    }
    return upper && lower && !underscore;
  }

  private void write(String line) {
    builder.append(line).append(NEWLINE);
  }

  GenerateLuaTypes generatePlanetiler() {
    exportGlobalInstance("planetiler", LuaEnvironment.PlanetilerNamespace.class);
    for (var clazz : LuaEnvironment.CLASSES_TO_EXPOSE) {
      exportGlobalType(clazz);
    }
    return this;
  }

  void exportGlobalInstance(String name, Class<?> clazz) {
    write("---@class (exact) " + getInstanceTypeName(clazz));
    write(name + " = {}");
  }

  void exportGlobalType(Class<?> clazz) {
    write("---@class (exact) " + getStaticTypeName(clazz));
    write(clazz.getSimpleName() + " = {}");
  }

  private String getStaticTypeName(Class<?> clazz) {
    String name = luaClassName(clazz) + "__class";
    debugStack.push(" -> " + clazz.getSimpleName());
    try {
      if (handled.add(name)) {
        write(getStaticTypeDefinition(clazz));
      }
      return name;
    } finally {
      debugStack.pop();
    }
  }

  private String getInstanceTypeName(TypeToken<?> type) {
    if (LIST_TYPE.isSupertypeOf(type)) {
      return getInstanceTypeName(type.resolveType(LIST_TYPE.getRawType().getTypeParameters()[0])) + "[]";
    } else if (MAP_TYPE.isSupertypeOf(type)) {
      return "{[%s]: %s}".formatted(
        getInstanceTypeName(type.resolveType(MAP_TYPE.getRawType().getTypeParameters()[0])),
        getInstanceTypeName(type.resolveType(MAP_TYPE.getRawType().getTypeParameters()[1]))
      );
    }
    return getInstanceTypeName(type.getRawType());
  }

  private String getInstanceTypeName(Class<?> clazz) {
    if (clazz.getPackageName().startsWith("com.google.protobuf")) {
      return "any";
    }
    if (LuaValue.class.equals(clazz)) {
      throw new IllegalArgumentException("Unhandled LuaValue: " + String.join("", debugStack));
    }
    debugStack.push(" -> " + clazz.getSimpleName());
    try {
      if (TYPE_NAMES.containsKey(clazz)) {
        return TYPE_NAMES.get(clazz);
      }
      if (clazz.isArray()) {
        return getInstanceTypeName(clazz.getComponentType()) + "[]";
      }
      if (LuaValue.class.isAssignableFrom(clazz)) {
        return "any";
      }

      String name = luaClassName(clazz);
      if (handled.add(name)) {
        write(getTypeDefinition(clazz));
      }
      return name;
    } finally {
      debugStack.pop();
    }
  }

  String getTypeDefinition(Class<?> clazz) {
    return generateLuaInstanceTypeDefinition(clazz).generate();
  }

  String getStaticTypeDefinition(Class<?> clazz) {
    return generateLuaTypeDefinition(clazz, "__class", true).generate();
  }

  private LuaTypeDefinition generateLuaInstanceTypeDefinition(Class<?> clazz) {
    return generateLuaTypeDefinition(clazz, "", false);
  }

  private LuaTypeDefinition generateLuaTypeDefinition(Class<?> clazz, String suffix, boolean isStatic) {
    TypeToken<?> type = TypeToken.of(clazz);
    var definition = new LuaTypeDefinition(type, suffix, isStatic);

    Type superclass = clazz.getGenericSuperclass();
    if (superclass != null && superclass != Object.class) {
      definition.addParent(type.resolveType(superclass));
    }
    for (var iface : clazz.getGenericInterfaces()) {
      definition.addParent(type.resolveType(iface));
    }

    for (var field : clazz.getFields()) {
      TypeToken<?> rawType = TypeToken.of(field.getDeclaringClass()).resolveType(field.getGenericType());
      TypeToken<?> typeOnThisClass = type.resolveType(field.getGenericType());
      if (Modifier.isPublic(field.getModifiers()) && isStatic == Modifier.isStatic(field.getModifiers()) &&
        (field.getDeclaringClass() == clazz || !rawType.equals(typeOnThisClass))) {
        definition.addField(field);
      }
    }

    Set<Method> recordFields = clazz.isRecord() ? Arrays.stream(clazz.getRecordComponents())
      .map(RecordComponent::getAccessor)
      .collect(Collectors.toSet()) : Set.of();

    // - declare public (static and nonstatic) methods
    for (var method : clazz.getMethods()) {
      if (Modifier.isPublic(method.getModifiers()) && isStatic == Modifier.isStatic(method.getModifiers())) {
        Invokable<?, ?> invokable = type.method(method);
        if (!invokable.getOwnerType().equals(type) && !differentFromParents(invokable, type)) {
          continue;
        }
        if (hasMethod(Object.class, method)) {
          // skip object methods
        } else if (method.isAnnotationPresent(LuaGetter.class) ||
          (method.getParameterCount() == 0 && recordFields.contains(method))) {
          definition.addField(method, method.getGenericReturnType());
        } else if (method.isAnnotationPresent(LuaSetter.class)) {
          definition.addField(method, method.getParameterTypes()[0]);
        } else {
          definition.addMethod(method);
        }
      }
    }

    if (isStatic) {
      for (var constructor : clazz.getConstructors()) {
        if (Modifier.isPublic(constructor.getModifiers())) {
          definition.addMethod("new", constructor, clazz);
        }
      }
    }
    return definition;
  }

  private boolean differentFromParents(Invokable<?, ?> invokable, TypeToken<?> type) {
    Class<?> superclass = type.getRawType().getSuperclass();
    if (superclass != null) {
      var superType = TypeToken.of(superclass);
      if (differentFromParents2(invokable, superType)) {
        return true;
      }
    }
    for (var iface : type.getTypes().interfaces()) {
      if (differentFromParents2(invokable, iface)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String toString() {
    return builder.toString();
  }

  private record LuaFieldDefinition(String name, String type) {

    void write(StringBuilder builder) {
      builder.append("---@field %s %s%n".formatted(name, type));
    }
  }

  private record LuaParameterDefinition(String name, String type) {}

  private record LuaTypeParameter(String name, String superType) {

    @Override
    public String toString() {
      return name +
        ((superType.equals(luaClassName(Object.class)) || "any".equals(superType)) ? "" : (" : " + superType));
    }
  }

  private record LuaMethodDefinitions(String name, List<LuaTypeParameter> typeParameters,
    List<LuaParameterDefinition> params, String returnType) {

    void write(String typeName, StringBuilder builder) {
      for (var typeParam : typeParameters) {
        builder.append("---@generic %s%n".formatted(typeParam));
      }
      for (var param : params) {
        builder.append("---@param %s %s%n".formatted(param.name, param.type));
      }
      builder.append("---@return %s%n".formatted(returnType));
      builder.append("function types.%s:%s(%s) end%n".formatted(
        typeName,
        name,
        params.stream().map(p -> p.name).collect(Collectors.joining(", "))
      ));
    }

    public String functionTypeString() {
      return "fun(%s): %s".formatted(
        params.stream().map(p -> p.name + ": " + p.type).collect(Collectors.joining(", ")),
        returnType
      );
    }

    public void writeAsField(StringBuilder builder) {
      builder.append("---@field %s %s%n".formatted(
        name,
        functionTypeString()
      ));
    }
  }

  private class LuaTypeDefinition {

    private final TypeToken<?> type;
    private final boolean isStatic;
    String name;
    Set<String> parents = new LinkedHashSet<>();
    Map<String, LuaFieldDefinition> fields = new TreeMap<>();
    Set<LuaMethodDefinitions> methods = new TreeSet<>(Comparator.comparing(Record::toString));

    LuaTypeDefinition(TypeToken<?> type, String suffix, boolean isStatic) {
      this.type = type;
      this.name = luaClassName(type.getRawType()) + suffix;
      this.isStatic = isStatic;
    }

    LuaTypeDefinition(TypeToken<?> type) {
      this(type, "", false);
    }

    void addParent(TypeToken<?> type) {
      parents.add(getInstanceTypeName(type));
    }

    LuaFieldDefinition addField(Member field, Type fieldType) {
      try {
        debugStack.push("." + field.getName());
        String fieldName = transformMemberName(field.getName());
        var fieldDefinition =
          new LuaFieldDefinition(fieldName, getInstanceTypeName(type.resolveType(fieldType)));
        fields.put(fieldName, fieldDefinition);
        return fieldDefinition;
      } finally {
        debugStack.pop();
      }
    }

    LuaFieldDefinition addField(Field field) {
      if (field.getType().equals(LuaValue.class) && field.isAnnotationPresent(LuaFunctionType.class)) {
        var functionDetails = field.getAnnotation(LuaFunctionType.class);
        var target = functionDetails.target();
        var targetMethod = functionDetails.method().isBlank() ?
          CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, field.getName()) :
          functionDetails.method();
        var matchingMethods = Arrays.stream(target.getDeclaredMethods())
          .filter(m -> m.getName().equals(targetMethod))
          .toList();
        if (matchingMethods.size() != 1) {
          throw new IllegalArgumentException("Expected exactly 1 method named " + targetMethod +
            " on " + target.getSimpleName() + ", found " + matchingMethods.size() + " " + String.join("", debugStack));
        }
        var definition = new LuaTypeDefinition(type);
        var method = definition.createMethod(matchingMethods.get(0));
        var fieldName = transformMemberName(field.getName());
        var fieldDefinition = new LuaFieldDefinition(
          fieldName,
          method.functionTypeString()
        );
        fields.put(fieldName, fieldDefinition);
        return fieldDefinition;
      }
      return addField(field, field.getGenericType());
    }

    void addMethod(Method method) {
      methods.add(createMethod(method));
    }

    void addMethod(String methodName, Executable method, Type returnType) {
      methods.add(createMethod(methodName, method, returnType));
    }

    private LuaMethodDefinitions createMethod(Method method) {
      return createMethod(method.getName(), method, method.getGenericReturnType());
    }

    private LuaMethodDefinitions createMethod(String methodName, Executable method, Type returnType) {
      methodName = transformMemberName(methodName);
      List<LuaTypeParameter> typeParameters = new ArrayList<>();
      for (var param : method.getTypeParameters()) {
        typeParameters.add(new LuaTypeParameter(
          param.getName(),
          getInstanceTypeName(type.resolveType(param.getBounds()[0]))
        ));
      }
      List<LuaParameterDefinition> parameters = new ArrayList<>();
      for (var param : method.getParameters()) {
        parameters.add(new LuaParameterDefinition(
          transformMemberName(param.getName()),
          param.isAnnotationPresent(LuaType.class) ? param.getAnnotation(LuaType.class).value() :
            param.getType() == Path.class ? "%s|string|string[]".formatted(getInstanceTypeName(Path.class)) :
            resolveType(param.getParameterizedType(), method.getTypeParameters())
        ));
      }
      return new LuaMethodDefinitions(
        methodName,
        typeParameters,
        parameters,
        resolveType(returnType, method.getTypeParameters())
      );
    }

    private String resolveType(Type elementType, TypeVariable<?>[] typeParameters) {
      var resolvedType = type.resolveType(elementType);
      // only return type parameter name when it is parameterized at the method level, see:
      // https://github.com/LuaLS/lua-language-server/issues/734
      // https://github.com/LuaLS/lua-language-server/issues/1861
      if (resolvedType.getType() instanceof TypeVariable<?> variable) {
        for (var typeParam : typeParameters) {
          if (typeParam.getName().equals(variable.getName())) {
            return typeParam.getName();
          }
        }
      }
      return getInstanceTypeName(resolvedType);
    }

    void write(StringBuilder builder) {
      String nameToUse = this.name;
      if (type.getRawType().isEnum() && !isStatic) {
        nameToUse += "__enum";
        builder.append("---@alias %s%n".formatted(name));
        builder.append("---|%s%n".formatted(nameToUse));
        builder.append("---|integer").append(NEWLINE);
        for (var constant : type.getRawType().getEnumConstants()) {
          builder.append("---|%s%n".formatted(Format.quote(constant.toString())));
        }
      }
      builder.append("---@class (exact) %s".formatted(nameToUse));
      if (!parents.isEmpty()) {
        builder.append(" : ").append(String.join(", ", parents));
      }
      builder.append(NEWLINE);
      for (var field : fields.values()) {
        field.write(builder);
      }
      boolean bindMethods = type.getRawType().isAnnotationPresent(LuaBindMethods.class);
      if (bindMethods) {
        for (var method : methods) {
          method.writeAsField(builder);
        }
      }
      builder.append("types.%s = {}%n".formatted(nameToUse));
      if (!bindMethods) {
        for (var method : methods) {
          method.write(nameToUse, builder);
        }
      }
    }

    public String generate() {
      StringBuilder tmp = new StringBuilder();
      write(tmp);
      return tmp.toString();
    }

  }
}
