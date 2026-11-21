# Self-extracting planetiler launchers.
#   make -j      all platforms  -> dist/
#   make host    this machine   -> build/<platform>/planetiler
#   make smoke   run `planetiler setup` on it (trains the warm-up AOT cache on the bundled Monaco data)
#   make clean | distclean (also drops the JDK cache)
# Needs: curl tar unzip zip go, plus planetiler.jar (or JAR=path/to/it)
# Doesn't build on Intel Macs: jlink must match the JDK 27 jmods, and Temurin has no JDK 27 for them.
# NOTE: recipe lines must start with a TAB.

JAR           ?= ../planetiler.jar
JAR           := $(abspath $(JAR))
VERSION       ?= $(shell git describe --tags --always 2>/dev/null || echo dev)
JDK_TAG       ?= jdk-27.0.1.0.2+1
CACHE         ?= .cache

# One row per platform (<GOOS>_<GOARCH>): Adoptium os/arch, Temurin JDK tag, and which native libraries in the jar to
# keep, and which native libraries to keep (drops ~20MB per platform).
#               Adoptium       JDK tag          native libraries to keep
linux_amd64   := linux/x64     $(JDK_TAG)       linux/(amd64|x86_64)/|x86_64-linux/
linux_arm64   := linux/aarch64 $(JDK_TAG)       linux/aarch64/|aarch64-linux/
darwin_amd64  := mac/x64       jdk-26.0.2.1+1   (darwin|mac)/x86_64/|jni/darwin/  # Temurin has no JDK 27 for Intel macs
darwin_arm64  := mac/aarch64   $(JDK_TAG)       (darwin|mac)/aarch64/|jni/darwin/
windows_amd64 := windows/x64   $(JDK_TAG)       (^|/)win(dows)?/(amd64|x86_64)/|x86_64-windows/
PLATFORMS     := linux_amd64 linux_arm64 darwin_amd64 darwin_arm64 windows_amd64
adoptium = $(word 1,$($1))
tag      = $(word 2,$($1))
natives  = $(word 3,$($1))

# Monaco test data that `planetiler setup` trains the warm-up AOT cache on, offline
TRAINING      := $(addprefix ../planetiler-core/src/test/resources/,monaco-latest.osm.pbf water_polygons.parquet \
                   lake_centerline.parquet natural_earth_shapefiles.zip)

# By default we include java modules in the jlink runtime that jdeps says are needed by planetiler.jar,
# but we exclude a few that aren't actually needed and add a few extra that jdeps misses:
# Referenced only from code planetiler never runs:
EXCLUDE := java.rmi java.scripting  # log4j-core JMX / JNDI / script support
EXCLUDE += java.sql.rowset          # ormlite (geopackage) rowset support
EXCLUDE += jdk.javadoc              # ICU's javadoc taglets
# Loaded by name at runtime, so jdeps can't see them:
INCLUDE := jdk.zipfs     # zip file systems, for sources inside .zip files
INCLUDE += jdk.compiler  # .java profiles (`planetiler MyProfile.java`)
INCLUDE += jdk.charsets  # non-standard charsets (GBK, Big5, Shift_JIS, ...) named by a shapefile's .cpg file
INCLUDE += jdk.jfr       # flight recordings (--jvm-args=-XX:StartFlightRecording) for profiling a run

HOST   := $(shell go env GOOS)_$(shell go env GOARCH)
SHA256 := $(shell command -v sha256sum >/dev/null && echo sha256sum || echo shasum -a 256)

# JDK downloads, cached under $(CACHE): each platform's jmods, plus a host JDK of the same release to run jlink (jlink
# only links its own release's jmods).
dl        = https://api.adoptium.net/v3/binary/version/$(subst +,%2B,$2)/$1
HOST_BIN := $(if $(filter darwin_%,$(HOST)),Contents/Home/)bin
jlink     = $(CACHE)/host/$(call tag,$1)/$(HOST_BIN)/jlink
jmods     = $(CACHE)/jmods/$1/$(call tag,$1)
JDEPS    := $(CACHE)/host/$(JDK_TAG)/$(HOST_BIN)/jdeps

LAUNCHER := $(filter-out payload_%,$(wildcard *.go)) go.mod
GOBUILD   = CGO_ENABLED=0 GOOS=$(firstword $(subst _, ,$*)) GOARCH=$(lastword $(subst _, ,$*)) \
            go build -trimpath -ldflags="-s -w" -o $@ .
ARCHIVES := $(foreach p,$(PLATFORMS),dist/planetiler_$(VERSION)_$(p).$(if $(filter windows_%,$(p)),zip,tar.gz))

.SILENT:
# Keep the payload_* files between runs instead of deleting them as intermediates.
.SECONDARY:
.DELETE_ON_ERROR:
.PHONY: all host smoke clean distclean

all: $(ARCHIVES) dist/checksums.txt
host: build/$(HOST)/planetiler
smoke: host
	PLANETILER_CACHE_DIR=build/cache build/$(HOST)/planetiler setup

# ---- JDK downloads ----

# $* = <JDK tag>
$(CACHE)/host/%/$(HOST_BIN)/jlink:
	echo "downloading host JDK $*"
	rm -rf $(CACHE)/host/$* && mkdir -p $(CACHE)/host/$*
	curl -fsSL "$(call dl,$(call adoptium,$(HOST)),$*)/jdk/hotspot/normal/eclipse" | tar -xz -C $(CACHE)/host/$* --strip-components=1

# $* = <platform>/<JDK tag>
$(CACHE)/jmods/%/java.base.jmod:
	echo "downloading jmods $*"
	rm -rf $(@D) && mkdir -p $(@D)/.x
	curl -fsSL -o $(@D)/.archive "$(call dl,$(call adoptium,$(*D)),$(*F))/jmods/hotspot/normal/eclipse"
	# Windows jmods come as a zip, the others as a tgz
	if [ "$$(head -c2 $(@D)/.archive)" = PK ]; then unzip -q $(@D)/.archive -d $(@D)/.x; else tar -xzf $(@D)/.archive -C $(@D)/.x; fi
	find $(@D)/.x -name '*.jmod' -exec mv {} $(@D) \;
	rm -rf $(@D)/.x $(@D)/.archive

# ---- payload: jlink runtime + jar + training data -> tgz, plus the Go file that embeds it ----

# Second expansion lets the prerequisites use $* (the platform), so each payload depends on its own JDK's jlink + jmods.
.SECONDEXPANSION:
payload_%.go: $(JAR) $(TRAINING) build/modules.txt $$(call jlink,$$*) $$(call jmods,$$*)/java.base.jmod
	echo "building payload $*"
	rm -rf build/stage/$* && mkdir -p build/stage/$*
	$(call jlink,$*) --module-path $(call jmods,$*) \
		--add-modules $$(paste -sd, build/modules.txt) \
		--strip-debug --no-header-files --no-man-pages --compress=zip-0 \
		--output build/stage/$*/runtime
	cp $(JAR) build/stage/$*/planetiler.jar
	# Strip the bundled native libraries that only other platforms need
	zipinfo -1 build/stage/$*/planetiler.jar | grep -E '\.(so|dylib|dll|jnilib|a)$$' | grep -viE '$(call natives,$*)' \
		| zip -qd build/stage/$*/planetiler.jar -@
	mkdir build/stage/$*/training && cp $(TRAINING) build/stage/$*/training/
	COPYFILE_DISABLE=1 tar -czf payload_$*.tgz -C build/stage/$* planetiler.jar runtime training
	rm -rf build/stage/$*
	# payloadID (first 12 hex digits of the payload's sha256) names the directory the launcher unpacks it to
	id=$$($(SHA256) < payload_$*.tgz | cut -c1-12); \
	printf '// Code generated by Makefile; DO NOT EDIT.\n\npackage main\n\nimport _ "embed"\n\n//go:embed payload_%s.tgz\nvar payload []byte\n\nconst payloadID = "%s"\n' "$*" "$$id" > $@

# The modules the jar references, one per line, minus the "<module>/<package>" lines for internal packages it uses
build/jdeps.txt: $(JAR) $(dir $(JDEPS))jlink
	echo "finding modules used by $(JAR)"
	mkdir -p build
	# Without its module-info.class files, jdeps analyzes the jar as a plain classpath jar
	cp $(JAR) build/deps.jar
	zip -qd build/deps.jar module-info.class 'META-INF/versions/*/module-info.class' || true
	$(JDEPS) --ignore-missing-deps --multi-release 27 --list-deps build/deps.jar | grep -v / > $@
	rm build/deps.jar

build/modules.txt: build/jdeps.txt
	printf '%s\n' $(sort $(filter-out $(EXCLUDE),$(shell cat $<)) $(INCLUDE)) > $@
	echo "modules:" $$(cat $@)

# ---- binaries and release archives ----

build/%/planetiler.exe: payload_%.go $(LAUNCHER)
	echo "building $@"
	$(GOBUILD)

build/%/planetiler: payload_%.go $(LAUNCHER)
	echo "building $@"
	$(GOBUILD)

dist/planetiler_$(VERSION)_%.tar.gz: build/%/planetiler
	echo "building $@"
	mkdir -p dist && tar -czf $@ -C build/$* planetiler

dist/planetiler_$(VERSION)_%.zip: build/%/planetiler.exe
	echo "building $@"
	mkdir -p dist && rm -f $@ && cd build/$* && zip -q $(abspath $@) planetiler.exe

dist/checksums.txt: $(ARCHIVES)
	cd dist && $(SHA256) $(notdir $(ARCHIVES)) > checksums.txt

clean:
	rm -rf build dist payload_*

distclean: clean
	rm -rf $(CACHE)
