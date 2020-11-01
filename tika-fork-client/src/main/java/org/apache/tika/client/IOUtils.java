package org.apache.tika.client;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class IOUtils {
  private static final Logger LOG = LoggerFactory.getLogger(IOUtils.class);

  /**
   * <p>
   * Not closeable version of {@link ZipInputStream}
   * </p>
   * <p>
   * {@link ZipInputStream} provides interface where you reuse the same {@link InputStream}
   * over and over for reading different files. The problem though that many
   * libraries that consume the whole stream (e.g. jackson, our jersey client) close it right away.
   * But closing {@link ZipInputStream} makes it impossible to read next zip entries.
   * </p>
   * <p>
   * This implementation prevents closing of {@link ZipInputStream} till explicitly enabled with {@link #enableClose()}
   * </p>
   */
  public static class NotClosableZipInputStream extends ZipInputStream {
    private final InputStream originalStream;
    private boolean closeEnabled = false;

    public NotClosableZipInputStream(InputStream in) {
      super(in);
      this.originalStream = in;
    }

    public void close() throws IOException {
      if (closeEnabled) {
        super.close();
      }
    }

    public void enableClose() {
      this.closeEnabled = true;
    }

    public InputStream getOriginalStream() {
      return originalStream;
    }
  }

  /**
   * <p>
   * Not closeable version of {@link ZipOutputStream}
   * </p>
   * <p>
   * {@link ZipOutputStream} provides interface where you reuse the same {@link OutputStream}
   * over and over for writing different files. The problem though that many
   * libraries that writes the whole stream (e.g. jackson) close it right away.
   * But closing {@link ZipOutputStream} makes it impossible to write next zip entries.
   * </p>
   * <p>
   * This implementation prevents closing of {@link ZipOutputStream} till explicitly enabled with {@link #enableClose()}
   * </p>
   */
  public static class NotClosableZipOutputStream extends ZipOutputStream {
    private boolean closeEnabled = false;

    public NotClosableZipOutputStream(OutputStream out) {
      super(out);
    }

    public void close() throws IOException {
      if (closeEnabled) {
        super.close();
      }
    }

    public void enableClose() {
      this.closeEnabled = true;
    }
  }

  /**
   * Call read() on the input stream until it returns -1 (until the stream is completely consumed).
   *
   * @param is The input stream to consume remainder.
   * @return Number of bytes drained.
   */
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

  /**
   * Call read() on the reader until it returns -1 (until the reader is completely consumed).
   *
   * @param reader The reader to consume remainder.
   * @return Number of characters drained.
   */
  public static long drainReader(Reader reader) {
    long numRead = 0;
    try {
      while (reader.read() >= 0) {
        // emptying the stream to prevent a client-side "pipe closed" error
        ++numRead;
      }
    } catch (IOException e) {
      if (!StringUtils.containsIgnoreCase(e.getMessage(), "stream closed") &&
          !StringUtils.containsIgnoreCase(e.getMessage(), "pipe closed")) {
        LOG.error("Could not drain remaining chars.", e);
      } else {
        LOG.debug("Could not drain remaining chars because reader is already closed.", e);
      }
    }
    return numRead;
  }
}
