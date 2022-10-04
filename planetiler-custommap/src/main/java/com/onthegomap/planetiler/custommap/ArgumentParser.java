package com.onthegomap.planetiler.custommap;

import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.custommap.expression.ParseException;
import com.onthegomap.planetiler.expression.DataType;
import com.onthegomap.planetiler.util.Try;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parses user-defined arguments from YAML.
 */
public class ArgumentParser {
  private static final Logger LOGGER = LoggerFactory.getLogger(ArgumentParser.class);

  private ArgumentParser() {}

  public static Contexts.Root buildRootContext(Arguments origArguments, Map<String, Object> schemaArgs) {
    boolean loggingEnabled = !origArguments.silenced();
    origArguments.silence();
    Map<String, String> argDescriptions = new LinkedHashMap<>();
    Map<String, Object> unparsedSchemaArgs = new HashMap<>(schemaArgs);
    Map<String, Object> parsedSchemaArgs = new HashMap<>(origArguments.toMap());
    Contexts.Root result = Contexts.root(origArguments, parsedSchemaArgs);
    Arguments arguments = origArguments;
    int iters = 0;
    while (!unparsedSchemaArgs.isEmpty()) {
      final var root = result;
      final var args = arguments;
      Map<String, Exception> failures = new HashMap<>();

      Map.copyOf(unparsedSchemaArgs).forEach((key, value) -> {
        boolean builtin = root.builtInArgs.contains(key);
        String description;
        Object defaultValueObject;
        DataType type = null;
        if (value instanceof Map<?, ?> map) {
          if (builtin) {
            throw new ParseException("Cannot override built-in argument: " + key);
          }
          var typeObject = map.get("type");
          if (typeObject != null) {
            type = DataType.from(Objects.toString(typeObject));
          }
          var descriptionObject = map.get("description");
          description = descriptionObject == null ? "no description provided" : descriptionObject.toString();
          defaultValueObject = map.get("default");
          if (type != null) {
            var fromArgs = args.getString(key, description, null);
            if (fromArgs != null) {
              parsedSchemaArgs.put(key, type.convertFrom(fromArgs));
            }
          }
        } else {
          defaultValueObject = value;
          description = "no description provided";
        }
        argDescriptions.put(key, description);
        Try<Object> defaultValue = ConfigExpressionParser.tryStaticEvaluate(root, defaultValueObject, Object.class);
        if (defaultValue.isSuccess()) {
          Object raw = defaultValue.get();
          String asString = Objects.toString(raw);
          if (type == null) {
            type = DataType.typeOf(raw);
          }
          var stringResult = args.getString(key, description, asString);
          Object castedResult = type.convertFrom(stringResult);
          if (stringResult == null) {
            throw new ParseException("Missing required parameter: " + key + "(" + description + ")");
          } else if (castedResult == null) {
            throw new ParseException("Cannot convert value for " + key + " to " + type.id() + ": " + stringResult);
          }
          parsedSchemaArgs.put(key, castedResult);
          unparsedSchemaArgs.remove(key);
        } else {
          failures.put(key, defaultValue.exception());
        }
      });

      arguments = origArguments.orElse(Arguments.of(parsedSchemaArgs.entrySet().stream().collect(Collectors.toMap(
        Map.Entry::getKey,
        e -> Objects.toString(e.getValue()))
      )));
      result = Contexts.root(arguments, parsedSchemaArgs);
      if (iters++ > 100) {
        failures
          .forEach(
            (key, failure) -> LOGGER.error("Error computing {}:\n{}", key,
              ExceptionUtils.getRootCause(failure).toString().indent(4)));
        throw new ParseException("Infinite loop while processing arguments: " + unparsedSchemaArgs.keySet());
      }
    }
    var finalArguments = loggingEnabled ? arguments.withExactlyOnceLogging() : arguments.silence();
    if (loggingEnabled) {
      argDescriptions.forEach((key, description) -> finalArguments.getString(key, description, null));
    }
    return Contexts.root(finalArguments, parsedSchemaArgs);
  }
}
