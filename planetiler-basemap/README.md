# Planetiler Basemap Profile

This basemap profile is based on [OpenMapTiles](https://github.com/openmaptiles/openmaptiles) v3.13.
See [README.md](../README.md) in the parent directory for instructions on how to run.

## Differences from OpenMapTiles

- Road name abbreviations are not implemented yet in the `transportation_name` layer
- `agg_stop` tag not implemented yet in the `poi` layer
- `brunnel` tag is excluded from `transportation_name` layer to avoid breaking apart long `transportation_name`
  lines, to revert this behavior set `--transportation-name-brunnel=true`
- `rank` field on `mountain_peak` linestrings only has 3 levels (1: has wikipedia page and name, 2: has name, 3: no name
  or wikipedia page or name)

## Code Layout

[Generate.java](./src/main/java/com/onthegomap/planetiler/basemap/Generate.java) generates code in
the [generated](./src/main/java/com/onthegomap/planetiler/basemap/generated) package from an OpenMapTiles tag in GitHub:

- [OpenMapTilesSchema](./src/main/java/com/onthegomap/planetiler/basemap/generated/OpenMapTilesSchema.java)
  contains an interface for each layer with constants for the name, attributes, and allowed values for each tag in that
  layer
- [Tables](./src/main/java/com/onthegomap/planetiler/basemap/generated/Tables.java)
  contains a record for each table that OpenMapTiles [imposm3](https://github.com/omniscale/imposm3) configuration
  generates (along with the tag-filtering expression) so layers can listen on instances of those records instead of
  doing the tag filtering and parsing themselves

The [layers](./src/main/java/com/onthegomap/planetiler/basemap/layers) package contains a port of the SQL logic to
generate each layer from OpenMapTiles. Layers define how source features (or parsed imposm3 table rows) map to vector
tile features, and logic for post-processing tile geometries.

[BasemapProfile](./src/main/java/com/onthegomap/planetiler/basemap/BasemapProfile.java) dispatches source features to
layer handlers and merges the results.

[BasemapMain](./src/main/java/com/onthegomap/planetiler/basemap/BasemapMain.java) is the main driver that registers
source data and output location.

## Regenerating Code

To run `Generate.java`, use [scripts/regenerate-openmaptiles.sh](../scripts/regenerate-openmaptiles.sh) script with the
OpenMapTiles release tag:

```bash
./scripts/regenerate-openmaptiles.sh v3.13 https://raw.githubusercontent.com/openmaptiles/openmaptiles/
```

Then follow the instructions it prints for reformatting generated code.

## License and Attribution

OpenMapTiles code is licensed under the BSD 3-Clause License, which appears at the top of any file ported from
OpenMapTiles.

The OpenMapTiles schema (or "look and feel") is licensed under [CC-BY 4.0](http://creativecommons.org/licenses/by/4.0/),
so any map derived from that schema
must [visibly credit OpenMapTiles](https://github.com/openmaptiles/openmaptiles/blob/master/LICENSE.md#design-license-cc-by-40)
. It also uses OpenStreetMap data, so you
must [visibly credit OpenStreetMap contributors](https://www.openstreetmap.org/copyright).
