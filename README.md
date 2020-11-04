# Apache Tika ForkParser example

Example of how to use the Apache Tika ForkParser

The tika-fork-main sub-project has a `dist` task that builds the `tikaBin` directory that you give to the `ForkParser`'s first argument.

The tika-fork-client sub-project shows you you can create a parser using tikaBin directory created above. 

# Running

```
./gradlew :tika-fork-main:dist
./gradlew :tika-fork-client:build :tika-fork-client:copyDependencies
# run it with one of the sample files
java -cp tika-fork-client/build/dist/*:tika-fork-client/build/libs/* org.apache.tika.client.TikaAsyncMain tika-fork-main/build/dist tika-fork-client/test-files/pdf-sample.pdf
```
