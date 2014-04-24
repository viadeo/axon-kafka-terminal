[![Build Status](https://travis-ci.org/viadeo/axon-kafka-terminal.svg?branch=master)](https://travis-ci.org/viadeo/axon-kafka-terminal)

#axon-kafka-terminal
====================

Implementation of axon-framework terminal using Kafka (a distributed event bus).

## How to deploy Snapshots/Releases to Sonatype OSS Repository

1. Specify values for the required properties in `gradle.properties` file.

2. Upload artifact to Nexus : `$ gradle uploadArchives -P<release|ci>`.
:warning: Without specified properties then the artifact will be uploaded locally.

### Resources

- [Sonatype OSS Maven Repository Usage Guide](https://docs.sonatype.org/display/Repository/Sonatype+OSS+Maven+Repository+Usage+Guide#SonatypeOSSMavenRepositoryUsageGuide-ReadtheOSSRHGuide)
- [How To Generate PGP Signatures With Maven](https://docs.sonatype.org/display/Repository/How+To+Generate+PGP+Signatures+With+Maven)
- [How to use signing plugin](http://www.gradle.org/docs/current/userguide/signing_plugin.html)
