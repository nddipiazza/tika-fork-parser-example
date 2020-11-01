package org.apache.tika.client;

import org.apache.tika.fork.ForkParser;
import org.apache.tika.fork.ParserFactoryFactory;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class TikaForkExample {
  public static void main(String[] args) throws Exception {

    String pathToTikaMainDist = args[0];
    File fileToParse = new File(args[1]);

    Map<String, String> parserArgs = new HashMap<>();
    parserArgs.put("zipBombCompressionRatio", String.valueOf(200));
    parserArgs.put("maxDepth", String.valueOf(200));
    parserArgs.put("maxPackageEntryDepth", String.valueOf(20));

    ForkParser forkParser = new ForkParser(Paths.get(pathToTikaMainDist), new ParserFactoryFactory("org.apache.tika.fork.main.CollectingParserFactory", parserArgs));

    forkParser.setMaxFilesProcessedPerServer(1000);
    forkParser.setPoolSize(5);
    forkParser.setJavaCommand(Arrays.asList("java", "-Xmx1g", "-Djava.awt.headless=true"));
    forkParser.setServerWaitTimeoutMillis(60000);
    forkParser.setServerParseTimeoutMillis(60000);
    forkParser.setServerPulseMillis(1000);

    CollectingParser collectingParser = new CollectingParser(forkParser, fileToParse.getName());

    ParseContext parseContext = new ParseContext();
    Metadata metadata = new Metadata();

    try (FileInputStream fis = new FileInputStream(fileToParse)) {
      collectingParser.parse(fis, null, metadata, parseContext);
    }
  }
}
