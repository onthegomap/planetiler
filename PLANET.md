# Generating a Map of the World

To generate a map of the world using the built-in [basemap profile](flatmap-basemap), you will need a machine with Java
16 or later installed and at least 10x as much disk space and 1.5x as much RAM as the `planet.osm.pbf` file you start
from. All testing has been done using Digital Ocean droplets with dedicated
vCPUs ([referral link](https://m.do.co/c/a947e99aab25)) and OpenJDK 17 installed through `apt`. Flatmap splits work
among available CPUs so the more you have, the less time it takes.

### 1) Choose the Data Source

First decide where to get the `planet.osm.pbf` file:

- One of the [official mirrors](https://wiki.openstreetmap.org/wiki/Planet.osm)
- The [AWS Registry of Open Data](https://registry.opendata.aws/osm/) public S3 mirror (default)
- Or a [Daylight Distribution](https://daylightmap.org/) snapshot from Facebook that includes extra quality/consistency
  checks, and add-ons like ML-detected roads and buildings. Combine add-ons and re-number
  using [osmium-tool](https://osmcode.org/osmium-tool/):
  ```bash
  osmium apply-changes daylight.osm.pbf admin.osc.bz2 <buildings.osc.bz2, ...> -o everything.osm.pbf
  osmium renumber everything.osm.pbf -o planet.osm.pbf
  ```
  NOTE: you need at least `admin.osc.bz2` for the `boundary` layer to show. This takes about 2.5 hours and needs as much
  RAM as the `planet.osm.pbf` size.

### 2) Run Flatmap

Download the [latest release](https://github.com/onthegomap/flatmap/releases/latest) of `flatmap.jar`.

Then run `java -Xms100g -Xmx100g -jar flatmap.jar` (replacing `100g` with 1.5x the `planet.osm.pbf` size)
with these options:

- `--bounds=world` to set bounding box to the entire planet
- `--nodemap-type=sparsearray` to store node locations in a sparse array instead of a sorted table - `sortedtable` is
  more efficient when there are large gaps in ID spaces (i.e. extracts) and `sparsearray` is more efficient with no/few
  ID gaps (planet, or renumbered extracts).
- `--nodemap-storage=ram` to store all node locations in RAM instead of a memory-mapped file - when using `ram` give the
  JVM 1.5x the input file size instead of 0.5x when using `mmap`
- `--download` to fetch [other data sources](NOTICE.md#data) automatically
- One of these to point flatmap at your data source:
  - `--osm-path=path/to/planet.osm.pbf` to point Flatmap at a file you downloaded
  - `--osm-url=http://url/of/planet.osm.pbf` to download automatically
  - `--osm-url=s3:211011` to download a specific snapshot from the AWS Registry of Open Data or `--osm-url=s3:latest` to
    download the latest snapshot
  - `--area=planet` to use the file in `./data/sources/planet.osm.pbf` or download the latest snapshot from AWS S3
    mirror if missing.

Run with `--help` to see all available arguments.

NOTE: The default basemap profile merges nearby buildings at zoom-level 13 (for example,
see [Boston](https://onthegomap.github.io/flatmap-demo/#13.08/42.35474/-71.06597)). This adds about 14 CPU hours (~50
minutes with 16 CPUs) to planet generation time and can be disabled using `--building-merge-z13=false`.

## Example

To generate the tiles shown on https://onthegomap.github.io/flatmap-demo/ I used the `planet-211011.osm.pbf` (64.7GB) S3
snapshot, then ran Flatmap on a Digital Ocean Memory-Optimized droplet with 16 CPUs, 128GB RAM, and 1.17TB disk running
Ubuntu 21.04 x64 in the nyc3 location.

First, I installed java 17 jre and screen:

```bash
apt-get update && apt-get install -y openjdk-17-jre-headless screen
```

Then I added a script `runworld.sh` to run with 100GB of RAM:

```bash
#!/usr/bin/env bash
set -e
java -Xmx100g -Xms100g \
  -XX:OnOutOfMemoryError="kill -9 %p" \
  -jar flatmap.jar \
  `# Download the latest planet.osm.pbf from s3://osm-pds bucket` \
  --area=planet --bounds=world --download \
  `# Accelerate the download by fetching the 10 1GB chunks at a time in parallel` \
  --download-threads=10 --download-chunk-size-mb=1000 \
  `# Also download name translations from wikidata` \
  --fetch-wikidata \
  --mbtiles=output.mbtiles \
  --nodemap-type=sparsearray --nodemap-storage=ram 2>&1 | tee logs.txt
```

Then I ran this in the background using screen, so it would continue if my shell exited:

```bash
screen -d -m "./runworld.sh"
tail -f logs.txt
```

It took 3h21m (including 12 minutes downloading source data) to generate a 99GB `output.mbtiles` file. See
the [full logs](planet-logs/v0.1.0-planet-do-16cpu-128gb.txt) from this run or this summary that it printed at the end:

```
3:21:03 DEB [mbtiles] - Tile stats:
3:21:03 DEB [mbtiles] - z0 avg:71k max:71k
3:21:03 DEB [mbtiles] - z1 avg:171k max:192k
3:21:03 DEB [mbtiles] - z2 avg:258k max:449k
3:21:03 DEB [mbtiles] - z3 avg:117k max:479k
3:21:03 DEB [mbtiles] - z4 avg:51k max:541k
3:21:03 DEB [mbtiles] - z5 avg:23k max:537k
3:21:03 DEB [mbtiles] - z6 avg:14k max:354k
3:21:03 DEB [mbtiles] - z7 avg:11k max:451k
3:21:03 DEB [mbtiles] - z8 avg:6.5k max:356k
3:21:03 DEB [mbtiles] - z9 avg:6k max:485k
3:21:03 DEB [mbtiles] - z10 avg:2.7k max:285k
3:21:03 DEB [mbtiles] - z11 avg:1.3k max:168k
3:21:03 DEB [mbtiles] - z12 avg:741 max:247k
3:21:03 DEB [mbtiles] - z13 avg:388 max:286k
3:21:03 DEB [mbtiles] - z14 avg:340 max:1.7M
3:21:03 DEB [mbtiles] - all avg:395 max:0
3:21:03 DEB [mbtiles] -  # features: 2,832,396,934
3:21:03 DEB [mbtiles] -     # tiles: 264,204,266
3:21:03 INF [mbtiles] - Finished in 4,668s cpu:66,977s avg:14.3

3:21:03 INF - Finished in 12,064s cpu:156,169s avg:12.9

3:21:03 INF - FINISHED!
3:21:03 INF - ----------------------------------------
3:21:03 INF - 	overall	12,064s cpu:156,169s avg:12.9
3:21:03 INF - 	download	169s cpu:1,070s avg:6.3
3:21:03 INF - 	wikidata	553s cpu:3,825s avg:6.9
3:21:03 INF - 	lake_centerlines	0.9s cpu:2s avg:1.8
3:21:03 INF - 	water_polygons	96s cpu:1,150s avg:12
3:21:03 INF - 	natural_earth	6s cpu:21s avg:3.7
3:21:03 INF - 	osm_pass1	921s cpu:5,177s avg:5.6
3:21:03 INF - 	osm_pass2	5,234s cpu:73,527s avg:14
3:21:03 INF - 	boundaries	14s cpu:18s avg:1.3
3:21:03 INF - 	sort	407s cpu:4,403s avg:10.8
3:21:03 INF - 	mbtiles	4,668s cpu:66,977s avg:14.3
3:21:03 INF - ----------------------------------------
3:21:03 INF - 	features	192GB
3:21:03 INF - 	mbtiles	99GB
```

To generate the extract for [the demo](https://onthegomap.github.io/flatmap-demo/) I ran:

```bash
# install node and tilelive-copy
curl -fsSL https://deb.nodesource.com/setup_16.x | sudo -E bash -
apt-get install -y nodejs
npm install -g @mapbox/tilelive @mapbox/mbtiles
# Extract z0-4 for the world
tilelive-copy --minzoom=0 --maxzoom=4 --bounds=-180,-90,180,90 output.mbtiles demo.mbtiles
# Extract z0-14 for just southern New England
tilelive-copy --minzoom=0 --maxzoom=14 --bounds=-73.6346,41.1055,-69.5464,42.9439 output.mbtiles demo.mbtiles
```

Then I ran [extract.sh](https://github.com/onthegomap/flatmap-demo/blob/main/extract.sh) in the flatmap-demo repo to
extract tiles from the mbtiles file to disk.
