# Apache Tika ForkParser example

Example of how to use the Apache Tika ForkParser

# Running

```
./gradlew :tika-fork-main:dist
./gradlew :tika-fork-client:build :tika-fork-client:copyDependencies
# run it with one of the sample files
java -cp tika-fork-client/build/dist/*:tika-fork-client/build/libs/* org.apache.tika.client.TikaForkExample tika-fork-main/build/dist tika-fork-client/test-files/pdf-sample.pdf
```
