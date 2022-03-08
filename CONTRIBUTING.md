# Contributing to Planetiler

Pull requests are welcome! Any pull request should:

- Include at least one unit test to verify the change in behavior
- Include an end-to-end test
  in [PlanetilerTests.java](planetiler-core/src/test/java/com/onthegomap/planetiler/PlanetilerTests.java)
  to verify any major new user-facing features work
- Format the change `./mvnw spotless:apply` and test with `./mvnw verify` before pushing

To set up your local development environment:

- Fork the repo
- Install Java 16 or later. You can download Java manually from [Adoptium](https://adoptium.net/installation.html) or
  use:
  - [Windows installer](https://adoptium.net/installation.html#windows-msi)
  - [macOS installer](https://adoptium.net/installation.html#macos-pkg) (or `brew install --cask temurin`)
  - [Linux installer](https://github.com/adoptium/website-v2/blob/main/src/asciidoc-pages/installation/linux.adoc)
    (or `apt-get install openjdk-17-jdk`)
- Generate OpenMapTiles and Protobuf java code:
  - run `scripts/generate-openmaptiles.sh`
  - run `sudo apt install -y protobuf-compiler`
  - run `scripts/generate-protobuf.sh`
- Build and run the tests ([mvnw](https://github.com/takari/maven-wrapper) automatically downloads maven the first time
  you run it):
  - on max/linux: `./mvnw clean test`
  - on windows: `mvnw.cmd clean test`
  - or if you already have maven installed globally on your machine: `mvn clean test`

GitHub Workflows will run regression tests on any pull request.

## IDE Setup

You can use any text editor as long as you format the code and test before pushing. A good IDE will make things a lot
easier though.

### IntelliJ IDEA (recommended)

- [Install IntelliJ IDEA](https://www.jetbrains.com/help/idea/installation-guide.html)
- Install
  the [Adapter for Eclipse Code Formatter plugin](https://plugins.jetbrains.com/plugin/6546-adapter-for-eclipse-code-formatter)
- In IntelliJ, click `Open`, navigate to the the `pom.xml` file in the local copy of this repo, and `Open`
  then `Open as Project`
  - If IntelliJ asks (and you trust the code) then click `Trust Project`
- Under `Preferences -> Tools -> Actions on Save` (or `File -> Settings -> Tools -> Actions on Save` on Linux)
  select `Reformat code` and `Optimize imports` to automatically format code on save.
- To verify everything works correctly, right click on `planetiler-core/src/test/java` folder and
  click `Run 'All Tests'`

Troubleshooting:

- If any java source files show "Cannot resolve symbol..." errors for Planetiler classes, you might need to
  select: `File -> Invalidate Caches... -> Just Restart`.
- If you see a "Project JDK is not defined" error, then choose `Setup SDK` and point IntelliJ at the Java 16 or later
  installed on your system

### Visual Studio Code

- Install the [Extension Pack for Java](https://marketplace.visualstudio.com/items?itemName=vscjava.vscode-java-pack)
- In VSCode, click `File -> Open` and navigate to Planetiler directory
  - If VSCode asks (and you trust the code) then click `Yes I trust the authors`
- To verify everything works correctly, go to the `Testing` tab and click `Run Tests`

Learn more about using VSCode with Java [here](https://code.visualstudio.com/docs/languages/java).

### Eclipse

- In [Eclipse for Java Developers](https://www.eclipse.org/downloads/packages/), click `File -> Import ...`
  then `Maven -> Existing Maven Projects`, navigate to Planetiler directory, and click `Finish`
- Under `Eclipse -> Preferences...`:
  - Under `Java -> Code Style -> Formatter` and choose `Import...`
    choose [`eclipse-formatter.xml`](eclipse-formatter.xml) from the root of this project. Then choose `Planetiler` as
    the Active profile.
  - Under `Java -> Editor -> Save Actions` check `Perform selected actions on save`, `Format source code`
    , `Format all lines` and `Organize imports`
  - Under `Java -> Code Style -> Organize Imports` change the `number of static imports needed for .*` to 5, then remove
    the groups and add 2 new groups:
    - `New Static...` and `*`
    - `New...` and `*`
- To verify everything works correctly, right click on `planetiler-core/src/test/java` folder and
  click `Run As -> JUnit Test`

TODO: Set up checkstyle
