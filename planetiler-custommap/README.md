# Configurable Planetiler Schema

You can define how planetiler turns input sources into vector tiles by running planetiler with a YAML configuration
file as the first argument:

```bash
# from a java build
java -jar planetiler.jar schema.yml
# or with docker (put the schema in data/schema.yml to include in the attached volume)
docker run -v "$(pwd)/data":/data ghcr.io/onthegomap/planetiler:latest /data/schema.yml
```

Schema files are in [YAML 1.2](https://yaml.org) format and this page and
accompanying [JSON schema](planetiler.schema.json) describe the required format and available
options. See the [samples](src/main/resources/samples) directory for working examples.

:construction: The configuration schema is under active development so the format may change between releases.
Only a subset of the Java API is currently exposed so for more complex schemas you should switch to the Java API (see
the [examples project](../planetiler-examples)). Feedback is welcome to help shape the final product!

## Root

The root of the schema has the following attributes:

- `schema_name` - A descriptive name for the schema
- `schema_description` - A longer description of the schema
- `attribution` - An attribution string, which may include HTML such as links
- `sources` - An object where key is the source ID and object is the [Source](#source) definition that points to a file
  containing geographic features to process
- `tag_mappings` - Specifies that certain tag key should have their values treated as a certain data type.
  See [Tag Mappings](#tag-mappings).
- `layers` - A list of vector tile [Layers](#layer) to emit and their definitions
- `examples` - A list of [Test Case](#test-case) input features and the vector tile features they should map to, or a
  relative path to a file with those examples in it. Run planetiler with `verify schema_file.yml` to see
  if they work as expected.
- `definitions` - An unparsed spot where you can
  define [anchor labels](https://en.wikipedia.org/wiki/YAML#Advanced_components) to be used in other parts of the
  schema

For example:

```yaml
schema_name: Power Lines
schema_description: A map of power lines from OpenStreetMap
attribution: <a href="https://www.openstreetmap.org/copyright" target="_blank">&copy; OpenStreetMap contributors</a>
sources: { ... }
tag_mappings: { ... }
layers: [...]
examples: [...]
definitions: # anything ...
```

## Source

A description that tells planetiler how to read geospatial objects with tags from an input file.

- `type` - Enum representing the file format of the data source, one
  of [`osm`](https://wiki.openstreetmap.org/wiki/PBF_Format) or [`shapefile`](https://en.wikipedia.org/wiki/Shapefile)
- `local_path` - Local path to the file to use, inferred from `url` if missing
- `url` - Location to download the file from if not present at `local_path`.
  For [geofabrik](https://download.geofabrik.de/) named areas, use `geofabrik:`
  prefixes, for example `geofabrik:rhode-island`.

For example:

```yaml
sources:
  osm:
    type: osm
    url: geofabrik:switzerland
```

## Tag Mappings

Specifies that certain tags should have their values parsed to a certain data type. This can be specified as an object
where key is the tag name and value is the [data type](#data-type), for example:

```yaml
tag_mappings:
  population: integer
```

If you still want to be able to access the original value, then you can remap the parsed value into a new tag
using `type` and `input` fields:

```yaml
tag_mappings:
  population_as_int:
    input: population
    type: integer
```

## Layer

A layer contains a thematically-related set of features from one or more input sources.

- `id` - Unique name of this layer
- `features` - A list of features contained in this layer. See [Layer Features](#layer-feature)

For example:

```yaml
layers:
  - id: power
    features:
      - { ... }
      - { ... }
```

## Layer Feature

A feature is a defined set of objects that meet a specified filter criteria.

- `source` - A string [source](#source) ID, or list of source IDs from which features should be extracted
- `geometry` - A string enum that indicates which geometry types to include, and how to transform them. Can be one
  of:
  - `point` `line` or `polygon` to pass the original feature through
  - `polygon_centroid` to match on polygons, and emit a point at the center
  - `polygon_point_on_surface` to match on polygons, and emit an interior point
  - `polygon_centroid_if_convex` to match on polygons, and if the polygon is convex emit the centroid, otherwise emit an
    interior point
- `min_tile_cover_size` - Include objects of a certain geometry size, where 1.0 means "is
  the same size as a tile at this zoom"
- `include_when` - A [Boolean Expression](#boolean-expression) which determines the features to include.
  If unspecified, all features from the specified sources are included.
- `exclude_when` - A [Boolean Expression](#boolean-expression) which determines if a feature that matched the include
  expression should be skipped. If unspecified, no exclusion filter is applied.
- `min_zoom` - An [Expression](#expression) that returns the minimum zoom to render this feature at.
- `attributes` - An array of [Feature Attribute](#feature-attribute) objects that specify the attributes to be included
  on this output feature.

For example:

```yaml
source: osm
geometry: line
min_zoom: 7
include_when:
  power:
    - line
attributes:
  - { ... }
  - { ... }
```

## Feature Attribute

Defines an attribute to include on an output vector tile feature and how to compute its value.

- `key` - ID of this attribute in the tile
- `include_when` - A [Boolean Expression](#boolean-expression) which determines whether to include
  this attribute. If unspecified, the attribute will be included unless
  excluded by `excludeWhen`.
- `exclude_when` - A [Boolean Expression](#boolean-expression) which determines whether to exclude
  this attribute. This rule is applied after `include_when`. If unspecified,
  no exclusion filter is applied.
- `min_zoom` - The minimum zoom at which to render this attribute
- `min_zoom_by_value` - Minimum zoom to render this attribute depending on the
  value. Contains an object with `<value>: zoom` entries that indicate the
  minimum zoom for each output value.
- `type` - The [Data Type](#data-type) to coerce the value to, or `match_key` to set this attribute to the key that
  triggered the match in the include expression, or `match_value` to set it to the value for the matching key.

To define the value, use one of:

- `value` - A constant string/number/boolean value, or an [Expression](#expression) that computes the value for this key
  for each input element.
- `coalesce` - A [Coalesce Expression](#coalesce-expression) that sets this attribute to the first non-null match from a
  list of expressions.
- `tag_value` - A [Tag Value Expression](#tag-value-expression) that sets this attribute to the value for a tag.

For example:

```yaml
key: voltage
min_zoom: 10
include_when: "${ double(feature.tags.voltage) > 1000 }"
tag_value: voltage
type: integer
```

## Data Type

A string enum that defines how to map from an input. Allowed values:

- `boolean` - Map 0, "no", or "false" to false and everything else to true
- `string` - Returns the string representation of the input value
- `direction` - Maps "-1" to -1, "1" "yes" or "true" to 1, and everything else to 0.
  See [Key:oneway](https://wiki.openstreetmap.org/wiki/Key:oneway#Data_consumers).
- `long` - Parses an input as a 64-bit signed number
- `integer` - Parses an input as a 32-bit signed number
- `double` - Parses an input as a floating point number

## Expression

Expressions let you define how to dynamically compute a value (attribute value, min zoom, etc.) at runtime. You can
structure data-heavy expressions in YAML (ie. [match](#match-expression) or [coalesce](#coalesce-expression)) or
simpler expressions that require more flexibility as an [inline script](#inline-script) using `${ expression }` syntax.

### Constant Value Expression

The simplest expression just returns a constant value from a string, number or boolean, for example:

```yaml
value: 1
value: 'string'
value: true
```

### Tag Value Expression

Use `tag_value:` to return the value for each feature's tag at runtime:

```yaml
# return value for "natural" tag
value:
  tag_value: natural
```

### Coalesce Expression

Use `coalesce: [expression, expression, ...]` to make the expression evaluate to the first non-null result of a list of
expressions at runtime:

```yaml
value:
  coalesce:
    - tag_value: highway
    - tag_value: aerialway
    - tag_value: railway
    - "fallback value"
```

### Match Expression

Use `{ value1: condition1, value2: condition2, ... }` to make the expression evaluate to the value associated
with the first matching [boolean expression](#boolean-expression) at runtime:

```yaml
value:
  # returns "farmland" if subclass is farmland, farm, or orchard
  farmland:
    subclass:
      - farmland
      - farm
      - orchard
  ice:
    subclass:
      - glacier
      - ice_shelf
  # "otherwise" keyword means this is the fallback value
  water: otherwise
```

If the values are not simple strings, then you can use an array of objects with `if` / `value` / `else` conditions:

```yaml
value:
  - value: 100000
    if:
      place: city
  - value: 5000
    if:
      place: town
  - value: 100
    if:
      place: [village, neighborhood]
  # fallback value
  - else: 0
```

In some cases it is more straightforward to express match logic as a `default_value` with `overrides`, for example:

```yaml
min_zoom:
  default_value: 13
  overrides:
    5:
      # match motorway or motorway_link
      highway: motorway%
    6:
      highway: trunk%
    8:
      highway: primary%
```

Default values, and values associated with conditions can themselves be an [Expression](#expression).

### Type

Add the `type` property to any expression to coerce the result to a particular [data type](#data-type):

```yaml
value:
  tag_value: oneway
  type: direction
```

### Inline Script

Use `${ expression }` syntax to compute a value dynamically at runtime using an
embedded [Common Expression Language (CEL)](https://github.com/google/cel-spec) script.

For example, to normalize highway values like "motorway_link" to "motorway":

```yaml
value: '${ feature.tags.highway.replace("_link", "") }'
```

If a script's value will never change, planetiler evaluates it once ahead of time, so you can also use this to
compute a complex value with no runtime overhead:

```yaml
value: "${ 8 * 24 - 2 }"
```

#### Inline Script Contexts

Scripts are parsed and evaluated inside a "context" that defines the variables available to that script. Contexts are
nested, so each child context can also access the variables from its parent.

> ##### root context
>
> defines no variables
>
>> ##### process feature context
>>
>> Context available when processing an input feature, for example testing whether to include it from `include_when`.
>> Available variables:
>>
>> - `feature.tags` - map with key/value tags from the input feature
>> - `feature.id` - numeric ID of the input feature
>> - `feature.source` - string source ID this feature came from
>> - `feature.source_layer` - optional layer within the source the feature came from
>>
>>> ##### post-match context
>>>
>>> Context available after a feature has matched, for example computing an attribute value. Adds variables:
>>>
>>> - `match_key` - string tag that triggered a match to include the feature in this layer
>>> - `match_value` - the tag value associated with that key
>>>
>>>> ##### configure attribute context
>>>>
>>>> Context available after the value of an attribute has been computed, for example: set min zoom to render an
>>>> attribute. Adds variables:
>>>>
>>>> - `value` the value that was computed for this key

For example:

```yaml
# return the value associated with the matching tag, converted to lower case:
value: '${ match_value.lowerAscii() }'
```

#### Built-In Functions

Inline scripts can use
the [standard CEL built-in functions](https://github.com/google/cel-spec/blob/master/doc/langdef.md#list-of-standard-definitions)
plus the following added by planetiler (defined
in [PlanetilerStdLib](src/main/java/com/onthegomap/planetiler/custommap/expression/stdlib/PlanetilerStdLib.java)).

- `coalesce(any, any, ...)` returns the first non-null argument
- `nullif(arg1, arg2)` returns null if arg1 is the same as arg2, otherwise arg1
- `min(list<number>)` returns the minimum value from a list
- `max(list<number>)` returns the maximum value from a list
- map extensions:
  - `<map>.has(key)` returns true if the map contains a key
  - `<map>.has(key, value)` returns true if the map contains a key and the value for that key is value
  - `<map>.has(key, value1, value2, ...)` returns true if the map contains a key and the value for that key is in the
    list provided
  - `<map>.get(key)` similar to `map[key]` except it returns null instead of throwing an error if the map is missing
    that key
  - `<map>.getOrDefault(key, default)` returns the value for key if it is present, otherwise default
- string extensions:
  - `<string>.charAt(number)` returns the character at an index from a string
  - `<string>.indexOf(string)` returns the first index of a substring or -1 if not found
  - `<string>.lastIndexOf(string)` returns the last index of a substring or -1 if not found
  - `<list>.join(separator)` returns a string that joins elements together separated by the provided string
  - `<string>.lowerAscii()` returns the input string transformed to lower-case
  - `<string>.upperAscii()` returns the input string transformed to upper-case
  - `<string>.replace(from, to)` returns the input string with all occurrences of from replaced by to
  - `<string>.replace(from, to, limit)` returns the input string with the first N occurrences of from replaced by to
  - `<string>.replaceRegex(pattern, value)` replaces every occurrence of regular expression with value from the string
    it was called on using java's
    built-in [replaceAll](<https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/regex/Matcher.html#replaceAll(java.lang.String)>)
    behavior
  - `<string>.split(separator)` returns a list of strings split from the input by a separator
  - `<string>.split(separator, limit)` splits the list into up to N parts
  - `<string>.substring(n)` returns a copy of the string with first N characters omitted
  - `<string>.substring(a, b)` returns a substring from index [a, b)
  - `<string>.trim()` trims leading and trailing whitespace

## Boolean Expression

A boolean expression evaluates to true or false for a given input feature. It can be specified as
a [tag-based boolean expression](#tag-based-boolean-expressions),
a [complex boolean expression](#complex-boolean-expressions), or
an [inline script](#inline-boolean-expression-script).

### Tag-Based Boolean Expressions

Boolean expressions can be specified as a map from key to value or list of values. For example:

```yaml
# match features where natural=glacier, waterway=riverbank, OR waterway=canal
include_when:
  natural: water
  waterway:
    - riverbank
    - canal
```

Planetiler optimizes runtime performance by pre-processing all of the `include_when` boolean expressions in
each [match expression](#match-expression) and `include_when` block in order to evaluate the minimum set of them at
runtime based on the tags present on the feature.

To match when a tag is present, use the `__any__` keyword:

```yaml
# match when the feature has a building tag
include_when:
  building: __any__
```

To match when a feature does _not_ have a tag use `''` as the value:

```yaml
# exclude features without a name tag
exclude_when:
  name: ""
```

To match when the value for a key matches a pattern, use the `%` wildcard character:

```yaml
# include features where highway tag ends in "_link"
include_when:
  highway: "%_link"
```

When a feature matches a boolean expression in the `include_when` field, the first key that triggered the match is
available to other expressions as `match_key` and its value is available as `match_value`
(See [Post-Match Context](#post-match-context)):

```yaml
include_when:
  highway:
    - motorway%
    - trunk%
    - primary%
  railway: rail
attributes:
  # set "kind" attribute to the value for highway or railway, with trailing "_link" stripped off
  - key: kind
    value: '${ match_value.replace("_link", ") }'
```

### Complex Boolean Expressions

The [tag-based boolean expressions](#tag-based-boolean-expressions) above match when _any_ of the tag conditions are
true, but to match only when all of them are true, you can nest them under an `__all__` key:

```yaml
# match when highway=pedestrian or highway=service AND area=yes
__all__:
  highway:
    - pedestrian
    - service
  area: yes
```

`__all__` can take an array as well. By default, each array item matches if _any_ of its children match, and you can
make that explicit with the `__any__` keyword:

```yaml
# match when highway=pedestrian OR foot=yes, and area=yes
__all__:
- highway: pedestrian
  foot: yes
- area: yes

# equivalent to:
__all__:
- __any__:
    highway: pedestrian
    foot: yes
- area: yes
```

You can also match when the subexpression is false using the `__not__` keyword:

```yaml
# match when place=city AND capital is not 'yes' or '4'
__all__:
  place: city
  __not__:
    capital: [yes, "4"]
```

### Inline Boolean Expression Script

You can also specify boolean logic with an [inline script](#inline-script) that evaluates to `true` or `false` using
the `${ expression }` syntax. For example:

```yaml
# set the `min_zoom` attribute to:
# 2 if area > 20 million, 3 if > 7 million, 4 if > 1 million, or 5 otherwise
min_zoom:
  default_value: 5
  overrides:
    2: "${ double(feature.tags.area) >= 2e8 }"
    3: "${ double(feature.tags.area) >= 7e7 }"
    4: "${ double(feature.tags.area) >= 1e7 }"
```

:warning: If you use an expression script in `include_when`, it will get evaluated against every input element
and will not set the `match_key` or `match_value` variables. When possible,
use [structured tag expressions](#tag-based-boolean-expressions) which are optimized for runtime matching performance.

You can, however combine a post-filter in an `__all__` block which will only get evaluated if
the [structured tag expressions](#tag-based-boolean-expressions) matches first:

```yaml
# Include a feature when place=city or place=town
# AND it has a population tag
# AND the population value is greater than 10000
include_when:
  __all__:
    - place: [city, town]
    - population: __any__
    # only evaluated if previous conditions are true
    - "${ double(feature.tags.population) > 10000 }"
```

## Test Case

An example input source feature, and the expected vector tile features that it produces. Run planetiler
with `verify schema.yml` to test your schema against each of the examples. Or you can add the `--watch` argument watch
the input file(s) for changes and validate the test cases on each change:

```yaml
# from a java build
java -jar planetiler.jar verify schema.yml --watch
# or with docker (put the schema in data/schema.yml to include in the attached volume)
docker run -v "$(pwd)/data":/data ghcr.io/onthegomap/planetiler:latest verify /data/schema.yml --watch
```

- `name` - Unique name for this test case.
- `input` - The input feature from a source, with the following attributes:
  - `source` - ID of the source this feature comes from.
  - `geometry` - Geometry type of the input feature, one of `point` `line` `polygon` or
    a [WKT](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry) encoding of a specific geometry.
  - `tags` - Key/value attributes on the source feature.
- `output` - The output vector tile feature(s) this map to, or `[]` for no features. Allowed attributes:
  - `layer` - Vector tile layer of the expected output feature.
  - `geometry` - Geometry type of the expected output feature.
  - `min_zoom` - Min zoom level that the output feature appears in.
  - `max_zoom` - Max zoom level that the output feature appears in.
  - `tags` - Attributes expected on the output vector tile feature, or `null` if the attribute should not be set. Use
    `allow_extra_tags: true` to fail if any other tags appear besides the ones specified here.
  - `allow_extra_tags` - If `true`, then fail when extra attributes besides tags appear on the output feature.
    If `false` or unset then ignore them.
  - `at_zoom` - Some attributes change by zoom level, so get values at this zoom level for comparison.

For example:

```yaml
name: Example power=line
input:
  geometry: line
  source: osm
  tags:
    power: line
    voltage: "1200"
output:
  - layer: power
    geometry: line
    min_zoom: 7
    tags:
      power: line
      voltage: 1200
```

See [shortbread.spec.yml](src/main/resources/samples/shortbread.spec.yml) for more examples.
