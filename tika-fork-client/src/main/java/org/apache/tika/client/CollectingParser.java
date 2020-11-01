package org.apache.tika.client;

import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
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
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * This is a modified version of RecursiveParserWrapper.
 */
public class CollectingParser implements Parser {
  private static final Logger LOG = LoggerFactory.getLogger(CollectingParser.class);

  private static final int maxStackDepth = 20;

  private final Parser wrappedParser;
  private final ParseContext plainContext = new ParseContext();
  private final String id;

  //used in naming embedded resources that don't have a name.
  private int currentDepth = 0;

  public CollectingParser(Parser wrappedParser, String id) {
    this.wrappedParser = wrappedParser;
    this.id = id;
    this.plainContext.set(Parser.class, wrappedParser);
  }

  @Override
  public Set<MediaType> getSupportedTypes(ParseContext context) {
    return wrappedParser.getSupportedTypes(context);
  }

  @Override
  public void parse(InputStream stream, ContentHandler ignore,
                    Metadata metadata, ParseContext context) throws IOException, SAXException, TikaException {
    TikaInputStream tikaInputStream;
    InputStream streamToClose = null;
    try {
      if (stream == null) {
        tikaInputStream = TikaInputStream.get(new byte[0]);
        streamToClose = tikaInputStream;
      } else if (stream instanceof TikaInputStream) {
        tikaInputStream = (TikaInputStream) stream;
      } else {
        tikaInputStream = TikaInputStream.get(stream);
        streamToClose = tikaInputStream;
      }
      // is main
      parseInternal(id, tikaInputStream, metadata, context, wrappedParser, 0, true);
    } finally {
      if (streamToClose != null) {
        // Consume the remainder of the stream if needed. This prevents Piped input streams from
        // throwing "pipe closed" exceptions when close is called.
        IOUtils.drainInputStream(streamToClose);
        try {
          streamToClose.close();
        } catch (IOException e) {
          LOG.debug("Could not drain remaining bytes from input stream because stream is already closed.", e);
        }
      }
    }
  }

  private void parseInternal(String location, InputStream stream, Metadata metadata, ParseContext context, Parser parser,
                             int lastDoc, boolean isMain) throws IOException, TikaException {
    currentDepth++;
    if (currentDepth > maxStackDepth) {
      LOG.warn("Avoiding parsing loop in " + location + ", nesting level: " + currentDepth);
      return;
    }
    try {
      parseInternalUnchecked(location, stream, metadata, context, parser, lastDoc, isMain);
    } finally {
      currentDepth--;
    }
  }


  private void parseInternalUnchecked(String location, InputStream stream, Metadata metadata, ParseContext context, Parser parser,
                                      int lastDoc, boolean isMain) throws IOException, TikaException {
    String contentType = metadata.get(Metadata.CONTENT_TYPE);
    if (contentType == null) {
      contentType = "application/octet-stream";
      metadata.set(Metadata.CONTENT_TYPE, contentType);
    }
    List<InputStream> streamsToClose = new ArrayList<>(3);
    TikaInputStream tikaInputStream;
    try {
      if (stream instanceof TikaInputStream) {
        tikaInputStream = (TikaInputStream) stream;
      } else {
        tikaInputStream = TikaInputStream.get(stream);
        streamsToClose.add(tikaInputStream);
      }
      TikaBodyContentHandler tikaBodyContentHandler = new TikaBodyContentHandler(new PrintWriter(System.out));
      parser.parse(tikaInputStream, tikaBodyContentHandler, metadata, context);

    } catch (Exception e) {
      throw new TikaException("Could not parse", e);
    } finally {
      for (InputStream is : streamsToClose) {
        try {
          is.close();
        } catch (Exception e) {
          LOG.warn("Error closing intermediate stream, ignoring: " + is, e);
        }
      }
    }
  }
}
