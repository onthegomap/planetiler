# Configurable Planetiler Schema

It is possible to customize planetiler's output from configuration files.  This is done using the parameter:
`--schema=schema_file.yml`

The schema file provides information to planetiler about how to construct the tiles and which layers, features, and attributes will be posted to the file.  Schema files are in [YAML](https://yaml.org) format.

For examples, see [samples](src/main/resources/samples) or [test cases](src/test/resources/validSchema).

## Schema file definition

The root of the schema has the following attributes:
* `schema_name` - A descriptive name for the schema
* `schema_description` - A longer description of the schema
* `attribution` - An attribution statement, which may include HTML such as links
* `sources` - A list of sources from which features should be extracted, specified as a list of names.  See [Tag Mappings](#tag-mappings).
* `dataTypes` - A map of tag keys that should be treated as a certain data type, with strings being the default.  See [Tag Mappings](#tag-mappings).
* `layers` - A list of vector tile layers and their definitions.  See [Layers](#layers)

### Data Sources

A data source contains geospatial objects with tags that are consumed by planetiler.  The configured data sources in the schema provide complete information on how to access those data sources.
* `type` - Either `shapefile` or `osm`
* `url` - Location to download the shapefile from.  For geofabrik named areas, use `geofabrik:` prefixes, for example `geofabrik:rhode-island`

### Layers

A layer contains a thematically-related set of features.
* `name` - Name of this layer
* `features` - A list of features contained in this layer.  See [Features](#features)

### Features

A feature is a defined set of objects that meet specified filter criteria.
* `geometry` - Include objects of a certain geometry type.  Options are `polygon`, `line`, or `point`.
* `min_tile_cover_size` - include objects of a certain geometry size, where 1.0 means "is the same size as a tile at this zoom".
* `include_when` - A tag specification which determines which features to include.  If unspecified, all features from the specified sources are included.  See [Tag Filters](#tag-filters)
* `exclude_when` - A tag specification which determines which features to exclude.  This rule is applied after `includeWhen`.  If unspecified, no exclusion filter is applied.  See [Tag Filters](#tag-filters)
* `min_zoom` - Minimum zoom to show the feature that matches the filter specifications.
* `attributes` - Specifies the attributes that should be rendered into the tiles for this feature, and how they are constructed.  See [Attributes](#attributes)

### Tag Mappings

Specifies that certain tag key should have their values treated as being a certain data type.
* `<key>: data_type` - A key, along with one of `boolean`, `string`, `direction`, or `long`
* `<key>: mapping` - A mapping which produces a new attribute by retrieving from a different key.  See [Tag Input and Output Mappings](#tag-input-and-output-mappings)

### Tag Input and Output Mappings

* `type`: One of `boolean`, `string`, `direction`, or `long`
* `output`: The name of the typed key that will be presented to the attribute logic

### Feature Zoom Specification

Specifies the zoom inclusion rules for this feature.
* `min_zoom` - Minimum zoom to render this feature
* `max_zoom` - Maximum zoom to render this feature

### Attributes

* `key` - Name of this attribute in the tile.
* `constant_value` - Value of the attribute in the tile, as a constant
* `tag_value` - Value of the attribute in the tile, as copied from the value of the specified tag key.  If neither constantValue nor tagValue are specified, the default behavior is to set the tag value equal to the input value (pass-through)
* `include_when` - A filter specification which determines whether to include this attribute.  If unspecified, the attribute will be included unless excluded by `excludeWhen`.  See [Tag Filters](#tag-filters)
* `exclude_when` - A filter specification which determines whether to exclude this attribute.  This rule is applied after `includeWhen`.  If unspecified, no exclusion filter is applied.  See [Tag Filters](#tag-filters)
* `min_zoom` - The minimum zoom at which to render this attribute.
* `min_zoom_by_value` - Minimum zoom to render this attribute depending on the value.  Contains a map of `value: zoom` entries that indicate the minimum zoom for each possible value.

### Tag Filters

A tag filter matches an object based on its tagging.  Multiple key entries may be specified:
* `<key>:` - Match objects that contain this key.
* `  <value>` - A single value or a list of values.  Match objects in the specified key that contains one of these values.  If no values are specified, this will match any value tagged with the specified key.

Example: match all `natural=water`:

        natural: water

Example: match residential, commercial, and industrial land use:

        landuse:
        - residential
        - commercial
        - industrial

