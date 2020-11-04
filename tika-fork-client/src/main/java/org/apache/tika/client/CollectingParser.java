package org.apache.tika.client;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

public class CollectingParser implements Parser {
  private static final Logger LOG = LoggerFactory.getLogger(CollectingParser.class);

  private final Parser wrappedParser;
  private final ParseContext plainContext = new ParseContext();

  public CollectingParser(Parser wrappedParser) {
    this.wrappedParser = wrappedParser;
    this.plainContext.set(Parser.class, wrappedParser);
  }

  @Override
  public Set<MediaType> getSupportedTypes(ParseContext context) {
    return wrappedParser.getSupportedTypes(context);
  }

  @Override
  public void parse(InputStream stream, ContentHandler contentHandler,
                    Metadata metadata, ParseContext context) throws IOException, TikaException, SAXException {
    wrappedParser.parse(stream, contentHandler, metadata, context);
  }

}
