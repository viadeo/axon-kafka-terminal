#axon-kafka-terminal

Implementation of axon-framework terminal using Kafka (a distributed event bus).

[![Build Status](https://travis-ci.org/viadeo/axon-kafka-terminal.svg?branch=master)](https://travis-ci.org/viadeo/axon-kafka-terminal)

## How to deploy Snapshots/Releases to Sonatype OSS Repository

2. Specify values for the required properties in `gradle.properties` file.

1. Ensure to do not propagate credentials on the public repository with this instruction : `git update-index --assume-unchanged gradle.properties` (You can reverse with `--no-assume-unchanged ` option).

3. Upload artifact to Nexus : `gradle uploadArchives -P<release|ci>`.
:warning: Without specified properties then the artifact will be uploaded locally.

### Resources

- [Sonatype OSS Maven Repository Usage Guide](https://docs.sonatype.org/display/Repository/Sonatype+OSS+Maven+Repository+Usage+Guide#SonatypeOSSMavenRepositoryUsageGuide-ReadtheOSSRHGuide)
- [How To Generate PGP Signatures With Maven](https://docs.sonatype.org/display/Repository/How+To+Generate+PGP+Signatures+With+Maven)
- [How to use signing plugin](http://www.gradle.org/docs/current/userguide/signing_plugin.html)
