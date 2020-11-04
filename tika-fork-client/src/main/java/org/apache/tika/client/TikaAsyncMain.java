package org.apache.tika.client;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.UrlEscapers;
import com.optimaize.langdetect.LanguageDetector;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.NTCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.conn.ssl.X509HostnameVerifier;
import org.apache.http.impl.auth.BasicSchemeFactory;
import org.apache.http.impl.auth.DigestSchemeFactory;
import org.apache.http.impl.auth.KerberosSchemeFactory;
import org.apache.http.impl.auth.NTLMSchemeFactory;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CursorMarkParams;
import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.exception.TikaException;
import org.apache.tika.fork.ForkParser;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.BodyContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TikaAsyncMain {
  private static final Logger logger = LoggerFactory.getLogger(TikaAsyncMain.class);

  private static final long DELAY_BETWEEN_JOBS_MILLIS = Long.parseLong(StringUtils.defaultIfBlank(System.getenv("DELAY_BETWEEN_JOBS_MILLIS"), "100000"));
  private static final int THREAD_COUNT = Integer.parseInt(StringUtils.defaultIfBlank(System.getenv("THREAD_COUNT"), "5"));
  private static final String SOLR_COLLECTION = System.getenv("SOLR_COLLECTION");
  private static final String SOLR_ZK_HOSTS = System.getenv("SOLR_ZK_HOSTS");
  private static final String SOLR_ZK_CHROOT = System.getenv("SOLR_ZK_CHROOT");
  private static final int SOLR_COMMIT_AFTER = Integer.parseInt(StringUtils.defaultIfBlank(System.getenv("SOLR_COMMIT_AFTER"), "100000"));
  private static final int TOTAL_NODES = Integer.parseInt(StringUtils.defaultIfBlank(System.getenv("TOTAL_NODES"), "1"));
  private static final String NODE_INDEX = System.getenv("NODE_INDEX");
  private static final String TIKA_MEMORY = StringUtils.defaultIfBlank(System.getenv("TIKA_MEMORY"), "256m");
  private static final int TIKA_POOL_SIZE = Integer.parseInt(StringUtils.defaultIfBlank(System.getenv("TIKA_POOL_SIZE"), "3"));
  private static final int SERVER_WAIT_TIMEOUT_MILLIS = Integer.parseInt(StringUtils.defaultIfBlank(System.getenv("SERVER_WAIT_TIMEOUT_MILLIS"), "30000"));
  private static final int SERVER_PARSE_TIMEOUT_MILLIS = Integer.parseInt(StringUtils.defaultIfBlank(System.getenv("SERVER_PARSE_TIMEOUT_MILLIS"), "60000"));
  private static final int SERVER_PULSE_MILLIS = Integer.parseInt(StringUtils.defaultIfBlank(System.getenv("SERVER_PULSE_MILLIS"), "2000"));
  private static final int MAX_FILES_PROCESSED_PER_SERVER = Integer.parseInt(StringUtils.defaultIfBlank(System.getenv("MAX_FILES_PROCESSED_PER_SERVER"), "1000"));
  private static final int ZIP_BOMB_COMPRESSION_RATIO = Integer.parseInt(StringUtils.defaultIfBlank(System.getenv("ZIP_BOMB_COMPRESSION_RATIO"), "200"));
  private static final int MAX_DEPTH = Integer.parseInt(StringUtils.defaultIfBlank(System.getenv("MAX_DEPTH"), "200"));
  private static final int MAX_PACKAGE_ENTRY_DEPTH = Integer.parseInt(StringUtils.defaultIfBlank(System.getenv("MAX_PACKAGE_ENTRY_DEPTH"), "20"));
  private static final int HTTPCLIENT_REQUEST_TIMEOUT = Integer.parseInt(StringUtils.defaultIfBlank(System.getenv("HTTPCLIENT_REQUEST_TIMEOUT"), "300000"));
  private static final int HTTPCLIENT_CONNECT_TIMEOUT = Integer.parseInt(StringUtils.defaultIfBlank(System.getenv("HTTPCLIENT_CONNECT_TIMEOUT"), "5000"));
  private static final int HTTPCLIENT_SOCKET_TIMEOUT = Integer.parseInt(StringUtils.defaultIfBlank(System.getenv("HTTPCLIENT_SOCKET_TIMEOUT"), "30000"));
  private static final int SOLR_INSERT_BATCH = Integer.parseInt(StringUtils.defaultIfBlank(System.getenv("SOLR_INSERT_BATCH"), "10"));
  private static final int MAX_PARSE_FAILURES = Integer.parseInt(StringUtils.defaultIfBlank(System.getenv("MAX_PARSE_FAILURES"), "2"));
  private final static String KEY = "ahey74832jfha8hijjhtnuag3ga7462hafha";
  private static final String KEYUN = System.getenv("KEYUN");
  private static final String KEYPWD = System.getenv("KEYPWD");
  private static final String KEYDOM = System.getenv("KEYDOM");
  public static final String LW_ASYNC_PARSING_FAIL_COUNT_I = "_lw_async_parsing_fail_count_i";
  public static final String LW_ASYNC_PARSING_DOWNLOAD_URL_S = "_lw_async_parsing_download_url_s";
  public static final String LW_ASYNC_PARSING_ID_I = "_lw_async_parsing_id_i";
  public static final String LW_PARSE_ERROR_CLASS_S = "_lw_parse_error_class_s";
  public static final String LW_PARSE_ERROR_MESSAGE_S = "_lw_parse_error_message_s";

  private CloseableHttpClient httpClient;

  private static final Pattern SPECIAL_CHAR_PATTERN = Pattern.compile("%([0-9abcdefABCDEF]{2})");

  public static String encodePath(String url) {
    try {
      //Un-encode any existing %20-type things
      Matcher m = SPECIAL_CHAR_PATTERN.matcher(url);
      while (m.find()) {
        char c = (char) Integer.parseInt(m.group(1).toUpperCase(), 16);
        url = url.replaceAll("%" + m.group(1), "" + c);
        m = SPECIAL_CHAR_PATTERN.matcher(url);
      }
      return UrlEscapers.urlFragmentEscaper().escape(url);
    } catch (Exception e) {
      return url;
    }
  }


  private void initHttpClient() {
    Registry<AuthSchemeProvider> authSchemeRegistry = null;
    CredentialsProvider credentialsProvider = null;
    credentialsProvider = new BasicCredentialsProvider();
    authSchemeRegistry = RegistryBuilder
        .<AuthSchemeProvider>create()
        .register("ntlm", new NTLMSchemeFactory())
        .register(AuthSchemes.BASIC, new BasicSchemeFactory())
        .register(AuthSchemes.DIGEST, new DigestSchemeFactory())
        .register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory())
        .register(AuthSchemes.KERBEROS, new KerberosSchemeFactory())
        .build();

    NTCredentials ntCredentials = new NTCredentials(
        AES.decrypt(KEYUN, KEY),
        AES.decrypt(KEYPWD, KEY),
        null,
        AES.decrypt(KEYDOM, KEY));
    credentialsProvider.setCredentials(AuthScope.ANY, ntCredentials);

    SSLConnectionSocketFactory sslsf;
    try {
      SSLContextBuilder sslContextBuilder = SSLContexts.custom();
      sslContextBuilder.loadTrustMaterial(null, new TrustStrategy() {
        @Override
        public boolean isTrusted(X509Certificate[] chain, String authType)
            throws CertificateException {
          return true;
        }
      });
      SSLContext sslContext = sslContextBuilder.build();
      sslsf = new SSLConnectionSocketFactory(
          sslContext, new X509HostnameVerifier() {
        @Override
        public void verify(String host, SSLSocket ssl)
            throws IOException {
        }

        @Override
        public void verify(String host, X509Certificate cert)
            throws SSLException {
        }

        @Override
        public void verify(String host, String[] cns,
                           String[] subjectAlts) throws SSLException {
        }

        @Override
        public boolean verify(String s, SSLSession sslSession) {
          return true;
        }
      });
    } catch (Exception e) {
      throw new RuntimeException("Could not ignore SSL verification", e);
    }

    PoolingHttpClientConnectionManager manager = new PoolingHttpClientConnectionManager(
        RegistryBuilder.<ConnectionSocketFactory>create()
            .register("http",
                PlainConnectionSocketFactory.getSocketFactory())
            .register("https", sslsf).build());
    manager.setDefaultMaxPerRoute(1000);
    manager.setMaxTotal(5 * 1000);

    BasicCookieStore basicCookieStore = new BasicCookieStore();
    HttpClientBuilder builder = HttpClients.custom()
        .setConnectionManager(manager)
        .setDefaultCookieStore(basicCookieStore)
//        .setRedirectStrategy(new CustomRedirectStrategy(sharepointPropertiesWrapper.startLinks))
        .setDefaultRequestConfig(RequestConfig.custom()
            .setConnectionRequestTimeout(HTTPCLIENT_REQUEST_TIMEOUT)
            .setConnectTimeout(HTTPCLIENT_CONNECT_TIMEOUT)
            .setSocketTimeout(HTTPCLIENT_SOCKET_TIMEOUT)
            .build()
        );
    if (authSchemeRegistry != null) {
      builder.setDefaultAuthSchemeRegistry(authSchemeRegistry)
          .setDefaultCredentialsProvider(credentialsProvider);
    }
    this.httpClient = builder.build();
  }


  public static void main(String[] args) throws Exception {
    TikaAsyncMain main = new TikaAsyncMain();
    main.initHttpClient();
    while (true) {
      main.runParseJob();
      logger.info("Sleeping for {} ms", DELAY_BETWEEN_JOBS_MILLIS);
      Thread.sleep(DELAY_BETWEEN_JOBS_MILLIS);
    }
  }

  private void runParseJob() throws IOException, InterruptedException {
    String hostName = InetAddress.getLocalHost().getHostName();

    File filesToParseCsv = fetchFilesToParse(hostName);

    ExecutorService parseService = Executors.newFixedThreadPool(THREAD_COUNT);

    runThreads(filesToParseCsv, parseService);

    Stopwatch sw = Stopwatch.createStarted();
    while (true) {
      try {
        if (parseService.awaitTermination(5000L, TimeUnit.MILLISECONDS)) {
          logger.info("Parse job completed in {} ms", sw.elapsed(TimeUnit.MILLISECONDS));
          break;
        }
        logger.info("Waiting for parse job to complete");
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void runThreads(File filesToParseCsv, ExecutorService parseService) throws IOException, InterruptedException {
    BlockingQueue<String> fetchQueue = new ArrayBlockingQueue<>(5000);

    AtomicInteger numRunningThreads = new AtomicInteger(0);
    for (int i = 0; i < THREAD_COUNT; ++i) {
      parseService.submit(() -> {
        List<LanguageProfile> languageProfiles;
        try {
          languageProfiles = new LanguageProfileReader().readAllBuiltIn();
        } catch (IOException e) {
          throw new RuntimeException("Could not build language profile", e);
        }
        LanguageDetector languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard())
            .withProfiles(languageProfiles)
            .build();
        try (CloudSolrClient solrClient = new CloudSolrClient.Builder()
            .withZkHost(SOLR_ZK_HOSTS)
            .withZkChroot(SOLR_ZK_CHROOT)
            .withConnectionTimeout(10000)
            .withSocketTimeout(60000)
            .build()) {
          ConfigurableAutoDetectParser configurableAutoDetectParser = new ConfigurableAutoDetectParser(new ConfigurableAutoDetectParser(new DefaultDetector(), ZIP_BOMB_COMPRESSION_RATIO, MAX_DEPTH, MAX_PACKAGE_ENTRY_DEPTH));
          try (ForkParser forkParser = new ForkParser(getClass().getClassLoader(), configurableAutoDetectParser)) {
            forkParser.setMaxFilesProcessedPerServer(MAX_FILES_PROCESSED_PER_SERVER);
            forkParser.setPoolSize(TIKA_POOL_SIZE);
            forkParser.setJavaCommand(Arrays.asList("java", "-Xmx" + TIKA_MEMORY, "-Djava.awt.headless=true"));
            forkParser.setServerWaitTimeoutMillis(SERVER_WAIT_TIMEOUT_MILLIS);
            forkParser.setServerParseTimeoutMillis(SERVER_PARSE_TIMEOUT_MILLIS);
            forkParser.setServerPulseMillis(SERVER_PULSE_MILLIS);

            CollectingParser collectingParser = new CollectingParser(forkParser);

            List<SolrInputDocument> insertBatch = Lists.newArrayList();
            String nextLine;
            while (true) {
              int numProcessed = 0;
              try {
                nextLine = fetchQueue.poll(1000, TimeUnit.MILLISECONDS);
                if (nextLine == null && numRunningThreads.get() == 0 && fetchQueue.isEmpty()) {
                  break;
                }

                ++numProcessed;

                String[] spl = nextLine.split("\t");
                String id = spl[0];
                String url = spl[1];
                int failCount = Integer.parseInt(spl[2]);

                logger.info("Fetching next url: {}", url);

                try {
                  numRunningThreads.incrementAndGet();

                  ParseContext parseContext = new ParseContext();
                  Metadata metadata = new Metadata();
                  ByteArrayOutputStream baos = new ByteArrayOutputStream();
                  BodyContentHandler contentHandler = new BodyContentHandler(baos);

                  CloseableHttpResponse response = null;
                  try {
                    response = httpClient.execute(new HttpGet(encodePath(url)));
                    if (response.getStatusLine().getStatusCode() != 200) {
                      throw new IOException("Could not fetch url=" + url + ", HttpStatus=" + response.getStatusLine());
                    }
                    File copiedToFile = File.createTempFile("copyfile", "." + FilenameUtils.getExtension(url));
                    try {
                      FileUtils.copyInputStreamToFile(response.getEntity().getContent(), copiedToFile);
                      try (FileInputStream fis = new FileInputStream(copiedToFile)) {
                        collectingParser.parse(fis, contentHandler, metadata, parseContext);

                        SolrInputDocument updateDoc = new SolrInputDocument();
                        updateDoc.setField("id", id);
                        String body = baos.toString();
                        String lang = languageDetector.detect(body).isPresent() ? languageDetector.detect(body).get().getLanguage() : "en";
                        updateDoc.setField("body_txt_" + lang, ImmutableMap.of("set", body));
                        updateDoc.setField(LW_ASYNC_PARSING_ID_I, ImmutableMap.of("removeregex", ".*"));
                        updateDoc.setField(LW_ASYNC_PARSING_DOWNLOAD_URL_S, ImmutableMap.of("removeregex", ".*"));
                        if (failCount > 0) {
                          updateDoc.setField(LW_PARSE_ERROR_CLASS_S, ImmutableMap.of("removeregex", ".*"));
                          updateDoc.setField(LW_PARSE_ERROR_MESSAGE_S, ImmutableMap.of("removeregex", ".*"));
                          updateDoc.setField(LW_ASYNC_PARSING_FAIL_COUNT_I, ImmutableMap.of("removeregex", ".*"));
                        }
                        insertBatch.add(updateDoc);
                      }
                    } finally {
                      FileUtils.deleteQuietly(copiedToFile);
                    }
                  } catch (TikaException e) {
                    logger.error("Tika exception for url={}. Emitting a parse failure doc.", url, e);
                    insertExceptionToBatch(insertBatch, id, e.getClass().getName(), e.getMessage());
                  } catch (Exception e) {
                    if (failCount < MAX_PARSE_FAILURES) {
                      logger.error("Non-tika exception when trying to parse previouslyFailedCount={}, URL={}. Will retry again next time.", url, failCount, e);
                      SolrInputDocument updateDoc = new SolrInputDocument();
                      updateDoc.setField("id", id);
                      updateDoc.setField(LW_ASYNC_PARSING_FAIL_COUNT_I, ImmutableMap.of("set", failCount + 1));
                      putParseErrorFieldsOnSolrDoc(e.getClass().getName(), e.getMessage(), updateDoc);
                      insertBatch.add(updateDoc);
                    } else {
                      logger.error("Non-tika exception when trying to parse previouslyFailedCount={}, URL={}. Exceeded max retry attempts. Emitting error.", url, failCount, e);
                      insertExceptionToBatch(insertBatch, id, e.getClass().getName(), e.getMessage());
                    }
                  } finally {
                    HttpClientUtils.closeQuietly(response);
                  }
                  if (insertBatch.size() >= SOLR_INSERT_BATCH) {
                    insertBatchIntoSolr(solrClient, insertBatch);
                  }
                } finally {
                  logger.info("Fetch thread is completed, processed={}", numProcessed);
                  numRunningThreads.decrementAndGet();
                }
                insertBatchIntoSolr(solrClient, insertBatch);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            }
          }
        } catch (SolrServerException | IOException e) {
          logger.error("Could not insert batch into solr", e);
        }
      });
    }
    try (FileReader fileReader = new FileReader(filesToParseCsv);
         LineIterator lineIterator = new LineIterator(fileReader)) {
      while (lineIterator.hasNext()) {
        fetchQueue.put(lineIterator.nextLine());
      }
    }
    logger.info("All files added to fetch queue. Waiting for them to process...");
    parseService.shutdown();
  }

  private void insertExceptionToBatch(List<SolrInputDocument> insertBatch, String id, String name, String message) {
    SolrInputDocument updateDoc = new SolrInputDocument();
    updateDoc.setField("id", id);
    putParseErrorFieldsOnSolrDoc(name, message, updateDoc);
    updateDoc.setField(LW_ASYNC_PARSING_ID_I, ImmutableMap.of("removeregex", ".*"));
    updateDoc.setField(LW_ASYNC_PARSING_DOWNLOAD_URL_S, ImmutableMap.of("removeregex", ".*"));
    insertBatch.add(updateDoc);
  }

  private void putParseErrorFieldsOnSolrDoc(String name, String message, SolrInputDocument updateDoc) {
    updateDoc.setField(LW_PARSE_ERROR_CLASS_S, ImmutableMap.of("set", name));
    if (StringUtils.isNotBlank(message)) {
      updateDoc.setField(LW_PARSE_ERROR_MESSAGE_S, ImmutableMap.of("set", message));
    }
  }

  private File fetchFilesToParse(String hostName) throws IOException {
    try (CloudSolrClient solrClient = new CloudSolrClient.Builder()
        .withZkHost(SOLR_ZK_HOSTS)
        .withZkChroot(SOLR_ZK_CHROOT)
        .withConnectionTimeout(10000)
        .withSocketTimeout(60000)
        .build()) {
      int nodeIndex = StringUtils.isNotBlank(NODE_INDEX) ? Integer.parseInt(NODE_INDEX) : Integer.parseInt(hostName.substring(hostName.lastIndexOf("-") + 1));

      int rangeBegin = nodeIndex * (Integer.MAX_VALUE / TOTAL_NODES);
      int rangeEnd = (nodeIndex + 1) * (Integer.MAX_VALUE / TOTAL_NODES);

      int fileCount = 0;

      File filesToParseCsv = File.createTempFile("toparse", ".csv");
      try (PrintWriter printWriter = new PrintWriter(filesToParseCsv)) {
        SolrQuery query = new SolrQuery();
        query.set("q", "_lw_async_parsing_id_i:[" + rangeBegin + " TO " + rangeEnd + "]");
        query.set("fl", String.format("id,%s,%s", LW_ASYNC_PARSING_DOWNLOAD_URL_S, LW_ASYNC_PARSING_FAIL_COUNT_I));
        query.setRows(5000);
        query.setSort(SolrQuery.SortClause.asc("id"));
        String cursorMark = CursorMarkParams.CURSOR_MARK_START;
        boolean done = false;
        while (!done) {
          query.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
          QueryResponse qr = solrClient.query(SOLR_COLLECTION, query);
          String nextCursorMark = qr.getNextCursorMark();
          logger.info("Fetching files to parse on {}", fileCount);
          for (SolrDocument sd : qr.getResults()) {
            ++fileCount;
            printWriter.print(sd.getFieldValue("id"));
            printWriter.print("\t");
            printWriter.print(sd.getFieldValue(LW_ASYNC_PARSING_DOWNLOAD_URL_S));
            printWriter.print("\t");
            int failCount = sd.getFieldValue(LW_ASYNC_PARSING_FAIL_COUNT_I) == null ? 0 : (Integer) sd.getFieldValue(LW_ASYNC_PARSING_FAIL_COUNT_I);
            printWriter.println(failCount);
          }
          if (cursorMark.equals(nextCursorMark)) {
            done = true;
          }
          cursorMark = nextCursorMark;
        }
      } catch (SolrServerException | IOException e) {
        logger.error("Error while fetching files to parse", e);
      }
      return filesToParseCsv;
    }
  }

  private void insertBatchIntoSolr(CloudSolrClient solrClient, List<SolrInputDocument> insertBatch) throws SolrServerException, IOException {
    if (insertBatch.isEmpty()) {
      return;
    }
    solrClient.add(SOLR_COLLECTION, insertBatch, SOLR_COMMIT_AFTER);
    insertBatch.clear();
  }
}