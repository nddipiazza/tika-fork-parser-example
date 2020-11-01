package org.apache.tika.client;

import org.apache.commons.lang3.StringUtils;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
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

  public static long drainInputStream(InputStream is) {
    long numRead = 0;
    try {
      while (is.read() >= 0) {
        // emptying the stream to prevent a client-side "pipe closed" error
        ++numRead;
      }
    } catch (IOException e) {
      if (!StringUtils.containsIgnoreCase(e.getMessage(), "stream closed") &&
          !StringUtils.containsIgnoreCase(e.getMessage(), "pipe closed")) {
        LOG.error("Could not drain remaining bytes.", e);
      } else {
        LOG.debug("Could not drain remaining bytes from input stream because stream is already closed.", e);
      }
    }
    return numRead;
  }

  @Override
  public void parse(InputStream stream, ContentHandler contentHandler,
                    Metadata metadata, ParseContext context) throws IOException, TikaException {
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
      parseInternal(tikaInputStream, metadata, context, wrappedParser, contentHandler);
    } finally {
      if (streamToClose != null) {
        // Consume the remainder of the stream if needed. This prevents Piped input streams from
        // throwing "pipe closed" exceptions when close is called.
        drainInputStream(streamToClose);
        try {
          streamToClose.close();
        } catch (IOException e) {
          LOG.debug("Could not drain remaining bytes from input stream because stream is already closed.", e);
        }
      }
    }
  }


  private void parseInternal(InputStream stream, Metadata metadata, ParseContext context, Parser parser, ContentHandler contentHandler) throws IOException, TikaException {
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
      parser.parse(tikaInputStream, contentHandler, metadata, context);

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
