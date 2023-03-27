# Raster DEM status

## Contour Lines

This appears to work fine with the following caveat:

- AsterV3.java generates one contour shape from hi-res DEM then scales it down to lower zoom levels, this can result in
  bad looking contour lines at low zooms. A better approach would be to downsample the DEM data to something appropriate
  for each zoom level, then generate contour lines for that zoom.

## Vector Hillshade

Sort of works, caveats:

- The polygons don't look right where the 1 degree tiles meet.
- It would probably be better to adopt maplibre GL JS's hillshading algorithm which adds shadows based on the slope more
  so than the aspect so you can see details better on south and southwest sides when the illumination is from
  northwest: https://github.com/maplibre/maplibre-gl-js/blob/main/src/shaders/hillshade.fragment.glsl
- It's _very_ slow

## Landcover

`EsaWorldcover.java` Doesn't do anything, but I did find out:

- ESA worldcover dataset is hosted in a public S3 dataset so it's easy to download the 3 degree x 3 degree tiff
  files https://registry.opendata.aws/esa-worldcover-vito/index.html
- The values are encoded by unique colors - see `EsaWorldcover.java` for an example parsing them
- We probably want to create "stacked" output polygons to create a greening effect when going from desert to grass to
  forest, something like:
  - omit built-up, moss, and water
  - emit "ice" polygons
  - emit a "greening stack" with:
    - level 1 includes shrub/grass/herbacious wetland
    - level 2 include all of level 1 plus also crop
    - level 3 includes level 1 and 2 plus also tree and mangrove
- Most vector landcover datasets I see in the wild don't include features <300m wide, so we can downsample this 10m
  dataset at least 30x to get started with.
- When downsampling, we probably want a bitmap for each output polygon, then consider a downsampled pixel a 1 if % of
  matching raw input pixels within that output pixel is > 50% or so.

The rough approach would look like:

- generate bitmaps corresponding to each output polygon from raw input
- downsample initial bitmaps 30x
- vectorize, smooth, emit polygons
- repeat until you get down to z0:
  - downsample another 2x, then vectorize, smooth, and emit polygons at the lower zoom level
