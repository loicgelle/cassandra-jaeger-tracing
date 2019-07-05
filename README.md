# First of all

`pom.xml` contains two "PLACEHOLDER" to be replaced with the absolute system path where you checked out https://github.com/loicgelle/jaeger-client-java

# Build

```
mvn install
```

to build the jar file `target/cassandra-jaeger-tracing-1.0-SNAPSHOT-jar-with-dependencies.jar`. The jar file can then be put in Cassandra's `lib` subdirectory and used as a custom tracer.
