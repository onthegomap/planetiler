# Configurable Planetiler Schema

It is possible to customize planetiler's output from configuration files.  This is done using the parameter:
`--schema=schema_file.yml`

The schema file provides information to planetiler about how to construct the tiles and which layers, features, and attributes will be posted to the file.  Schema files are in [YAML](https://yaml.org) format.

## Schema file definition

The root of the schema has the following attributes:
* `schemaName` - A descriptive name for the schema
* `schemaDescription` - A longer description of the schema
* `attribution` - An attribution statement, which may include HTML such as links
* `sources` - A list of sources from which features should be extracted, specified as a list of names.  See [Data Sources](#data-sources).
* `dataTypes` - A map of tag keys that should be treated as a certain data type, with strings being the default.  See [Data Types](#data-types).
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
* `zoom` - Specifies the zoom inclusion rules for this feature.  See [Zoom Specification](#feature-zoom-specification).
* `geometry` - Include objects of a certain geometry type.  Options are `polygon`, `line`, or `point`.
* `minTileCoverSize` - include objects of a certain geometry size, where 1.0 means "is the same size as a tile at this zoom".
* `includeWhen` - A tag specification which determines which features to include.  If unspecified, all features from the specified sources are included.  See [Tag Filters](#tag-filters)
* `excludeWhen` - A tag specification which determines which features to exclude.  This rule is applied after `includeWhen`.  If unspecified, no exclusion filter is applied.  See [Tag Filters](#tag-filters)
* `attributes` - Specifies the attributes that should be rendered into the tiles for this feature, and how they are constructed.  See [Attributes](#attributes)

### Data Types

Specifies that certain tag key should have their values treated as being a certain data type.
* `<data type>` - One of `boolean`, `string`, `direction`, or `long`
* `<list of values>` - A list of strings corresponding to keys that are treated as this data type.

### Feature Zoom Specification

Specifies the zoom inclusion rules for this feature.
* `minZoom` - Minimum zoom to render this feature
* `maxZoom` - Maximum zoom to render this feature
* `zoomFilter` - A list of tag-specific zoom filter overrides.  The first matching filter will apply.  See [Zoom Filter Specification](#zoom-filter-specification)

### Zoom Filter Specification

Specifies tag-based rules for setting the zoom range for a feature.
* `tag` - A filter specification which determines to which features this zoom limit applies.  See [Tag Filters](#tag-filters)
* `minZoom` - Minimum zoom to show the feature that matches the filter specification.

### Attributes

* `key` - Name of this attribute in the tile.
* `constantValue` - Value of the attribute in the tile, as a constant
* `tagValue` - Value of the attribute in the tile, as copied from the value of the specified tag key.  If neither constantValue nor tagValue are specified, the default behavior is to set the tag value equal to the input value (pass-through)
* `includeWhen` - A filter specification which determines whether to include this attribute.  If unspecified, the attribute will be included unless excluded by `excludeWhen`.  See [Tag Filters](#tag-filters)
* `excludeWhen` - A filter specification which determines whether to exclude this attribute.  This rule is applied after `includeWhen`.  If unspecified, no exclusion filter is applied.  See [Tag Filters](#tag-filters)
* `minZoom` - The minimum zoom at which to render this attribute.

### Tag Filters

A tag filter matches an object based on its tagging.  Multiple key entries may be specified:
* `<key>:` - Match objects that contain this key.
* `  <value>` - A single value or a list of values.  Match objects in the specified key that contains one of these values.  If no values are specified, this will match any value tagged with the specified key.
