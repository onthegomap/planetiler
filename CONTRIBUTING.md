# Contributing to Flatmap

Pull requests are welcome! To set up your development environment:

- Fork the repo
- [Install Java 16 or later](https://adoptium.net/installation.html)
- Build and run the tests ([mvnw](https://github.com/takari/maven-wrapper) automatically downloads maven the first time
  you run it):
  - on max/linux: `./mvnw clean test`
  - on windows: `mvnw.cmd clean test`
  - or if you already have maven installed globally on your machine: `mvn clean test`

To edit the code:

- [Install IntelliJ IDE](https://www.jetbrains.com/help/idea/installation-guide.html)
- In IntelliJ, click `Open`, navigate to the the `pom.xml` file in the local copy of this repo, and `Open`
  then `Open as Project`
  - If IntelliJ asks (and you trust the code) then click `Trust Project`
  - If any java source files show "Cannot resolve symbol..." errors for Flatmap classes, you might need to
    select: `File -> Invalidate Caches... -> Just Restart`.
  - If you see a "Project JDK is not defined" error, then choose `Setup SDK` and point IntelliJ at the Java 16 or later
    installed on your system
- To verify everything works correctly, right click on `flatmap-core/src/test/java` folder and click `Run 'All Tests'`

Any pull request should:

- Include at least one unit test to verify the change in behavior
- Include an end-to-end test in [FlatmapTests.java](flatmap-core/src/test/java/com/onthegomap/flatmap/FlatmapTests.java)
  to verify any major new user-facing features work
- Use IntelliJ's auto-formatting for modified files (this should get enabled automatically)
- Be free of IntelliJ warnings for modified files

GitHub Workflows will run regression tests on any pull request.

TODO: Set up checkstyle and an auto-formatter to enforce standards, so you can use any IDE.
