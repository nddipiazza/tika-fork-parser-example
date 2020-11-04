package org.apache.tika.fork.main;

import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ParserFactory;

import java.util.Map;

@SuppressWarnings("unused")
public class CollectingParserFactory extends ParserFactory {

  Map<String, String> args;

  public CollectingParserFactory(Map<String, String> args) {
    super(args);
    this.args = args;
  }

  @Override
  public Parser build() {
    return new ConfigurableAutoDetectParser(new DefaultDetector(), Long.parseLong(args.get("zipBombCompressionRatio")), Long.parseLong(args.get("maxDepth")), Long.parseLong(args.get("maxPackageEntryDepth")));
  }
}