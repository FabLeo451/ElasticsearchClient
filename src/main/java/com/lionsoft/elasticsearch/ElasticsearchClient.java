package com.lionsoft.elasticsearch;

import org.apache.http.HttpHost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.ElasticsearchStatusException;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchHit;
import org.apache.lucene.search.TotalHits;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.RequestLine;
import org.apache.http.util.EntityUtils;

//import java.time.Instant;
import java.io.IOException;
import java.io.*;
import java.util.*;
import java.text.SimpleDateFormat;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import java.util.Iterator;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.cert.X509Certificate;
import java.security.SecureRandom;
import java.security.NoSuchAlgorithmException;
import java.security.KeyManagementException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Jdk14Logger;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class ElasticsearchClient {

  RestHighLevelClient client = null;
  int code = 0;
  String message = "OK";
  RestStatus status;
  BulkResponse bulkResponse = null;

  public ElasticsearchClient() {
    this.code = 0;
    this.message = "No message";

    Log log = LogFactory.getLog("org.apache.http");
    Log4JLogger log4jLogger = (org.apache.commons.logging.impl.Log4JLogger)log;
    Logger logger = log4jLogger.getLogger();
    logger.setLevel(Level.INFO);

    log = LogFactory.getLog("org.elasticsearch.client");
    log4jLogger = (org.apache.commons.logging.impl.Log4JLogger)log;
    logger = log4jLogger.getLogger();
    logger.setLevel(Level.INFO);
  }

  public ElasticsearchClient(String protocol, String ip, int port, String user, String password, boolean checkCertificate, String certificate) {
    this();

    boolean hasCredentials = (user != null && password != null) && (user.length() > 0 && password.length() > 0);

    try {
      final SSLContext sslContext = SSLContext.getInstance("SSL");
      
      if (checkCertificate) {
        // TODO
      } else {
        // set up a TrustManager that trusts everything
        sslContext.init(null, new TrustManager[] { new X509TrustManager() {
                    public X509Certificate[] getAcceptedIssuers() {
                            //System.out.println("getAcceptedIssuers =============");
                            return null;
                    }

                    public void checkClientTrusted(X509Certificate[] certs, String authType) {
                            //System.out.println("checkClientTrusted =============");
                    }

                    public void checkServerTrusted(X509Certificate[] certs, String authType) {
                            //System.out.println("checkServerTrusted =============");
                    }
        } }, new SecureRandom());
      }

      //System.out.println("Connecting to "+protocol+"://"+ip+":"+port);

      RestClientBuilder builder = RestClient.builder(new HttpHost(ip, port, protocol));

      if (hasCredentials) {
          //System.out.println("Logging with credentials "+user+"/"+password);
          final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
          credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));

          builder.setHttpClientConfigCallback(new HttpClientConfigCallback() {
              @Override
              public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                  return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                                          .setSSLContext(sslContext)
                                          .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
              }
          });
      }

      client = new RestHighLevelClient(builder);
    }
    catch (NoSuchAlgorithmException e) {
          setResult(4, "NoSuchAlgorithmException: "+e.getMessage());
          return;
    }
    catch (KeyManagementException e) {
          setResult(4, "KeyManagementException: "+e.getMessage());
          return;
    }

  }

  public ElasticsearchClient(String protocol, String ip, int port) {
    this(protocol, ip, port, null, null, false, null);
  }

	public void setResult(int c, String m) {
		code = c;
		message = m;
	}

	public void setRestStatus(RestStatus s) {
		status = s;
	}

	public RestStatus getRestStatus() {
		return(status);
	}

	public String getMessage() {
		return(message);
	}
/*
  void createClient() {
    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("user", "password"));

    RestClientBuilder builder = RestClient.builder(
        new HttpHost("localhost", 9200, "http")).setHttpClientConfigCallback(new HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        });

     client = new RestHighLevelClient(builder);
   }*/

  public void close() {
    try {
      client.close();
    } catch (IOException e) {
      setResult(1, e.getMessage());
    }
  }

  public boolean ping() {
    if (client == null) {
      setResult(3, "Client not initialized");
      return false;
    }

    try {
      return (client.ping(RequestOptions.DEFAULT));
    } catch (IOException e) {
      setResult(2, e.getMessage());
      return false;
    }
  }

  /**
   * Creates or updates a document in the given index
   *
   * @param  index The index
   * @return           Number of deleted documents
   * @see
   */
  public long emptyIndex(String index) {
    long n = 0;

    DeleteByQueryRequest request = new DeleteByQueryRequest(index);
    request.setConflicts("proceed");
    request.setQuery(new MatchAllQueryBuilder());

    try {
      BulkByScrollResponse bulkResponse = client.deleteByQuery(request, RequestOptions.DEFAULT);
      n = bulkResponse.getDeleted();
    } catch (IOException e) {
      setResult(1, e.getMessage());
      return n;
    } catch (ElasticsearchStatusException e) {
      setRestStatus(e.status());
      setResult(1, e.getMessage());
      return n;
    }

    return n;
  }

  /**
   * Creates or updates a document in the given index
   *
   * @param  index The index
   * @param  id        Id of document. Set null for autoassignement
   * @param  mapObject Key/value pairs
   * @return           true if document has been created or updated. false if not
   * @see
   */
  public boolean putDocument(String index, String id, Map<String, Object> mapObject) {
    IndexRequest request = new IndexRequest(index);

    if (id != null && id.length() > 0)
      request.id(id);

    // Timestamp
    SimpleDateFormat sdf;
    sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    String text = sdf.format(new Date());
    mapObject.put("@timestamp", text);

    //System.out.println(mapObject);

    request.source(mapObject/*, XContentType.JSON*/);

    try {
      IndexResponse response = client.index(request, RequestOptions.DEFAULT);
      status = response.status();
    } catch (IOException e) {
      setResult(1, e.getMessage());
      return false;
    } catch (ElasticsearchStatusException e) {
      setRestStatus(e.status());
      setResult(1, e.getMessage());
      return false;
    }

    return true;
  }

  public Map<String, Object> toMap(JSONObject jo) {
    Map<String, Object> map = new HashMap<String, Object>();

    for (Iterator iterator = jo.keySet().iterator(); iterator.hasNext();) {
      String key = (String) iterator.next();
      Object value = jo.get(key);

      if (value instanceof JSONObject) {
        value = toMap((JSONObject) value);
      }

      map.put(key, value);
    }

    return map;
  }

  /**
   * Creates or updates a document in the given index
   *
   * @param  index     The index
   * @param  id        Id of document. Set null for autoassignement
   * @param  jo        JSON object representing the document
   * @return           true if document has been created or updated. false if not
   * @see
   */
  public boolean putDocumentJSON(String index, String id, JSONObject jo) {
    return (putDocument(index, id, toMap(jo)));
  }

  public Map<String, Object> addTimestamp(Map<String, Object> mapObject) {
    SimpleDateFormat sdf;

    sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    String text = sdf.format(new Date());
    mapObject.put("@timestamp", text);

    return (mapObject);
  }

  /**
   * Performs a bulk insert in the given index
   *
   * @param  index     The index
   * @param  ja        JSON array containing the documents
   * @return           true if operation has been performed. false if not
   * @see
   */
  public boolean bulkInsert(String index, JSONArray ja) {
    BulkRequest request = new BulkRequest();
    /*request.add(new IndexRequest("posts").id("1").source(XContentType.JSON,"field", "foo"));
    request.add(new IndexRequest("posts").id("2").source(XContentType.JSON,"field", "bar"));
    */

    JSONObject jo;
    IndexRequest ir;

    for (int i=0; i<ja.size(); i++) {
      jo = (JSONObject) ja.get(i);

      Map<String, Object> map = toMap(jo);
      map = addTimestamp(map);

      ir = new IndexRequest(index);

      if (map.containsKey("_id")) {
        ir.id((String) map.get("_id"));
        map.remove("_id");
      }

      ir.source(map);
      request.add(ir);
    }

    try {
      bulkResponse = client.bulk(request, RequestOptions.DEFAULT);

      status = bulkResponse.status();

      /*
      for (BulkItemResponse bulkItemResponse : bulkResponse) {
          DocWriteResponse itemResponse = bulkItemResponse.getResponse();

          switch (bulkItemResponse.getOpType()) {
          case INDEX:
          case CREATE:
              IndexResponse indexResponse = (IndexResponse) itemResponse;
              break;
          case UPDATE:
              UpdateResponse updateResponse = (UpdateResponse) itemResponse;
              break;
          case DELETE:
              DeleteResponse deleteResponse = (DeleteResponse) itemResponse;
          }
      }

      if (bulkResponse.hasFailures()) {

      }
      */
    } catch (IOException e) {
      setResult(1, e.getMessage());
      return false;
    } catch (ElasticsearchStatusException e) {
      setRestStatus(e.status());
      setResult(1, e.getMessage());
      return false;
    }

    return true;
  }

  public BulkResponse getBulkResponse() {
    return (bulkResponse);
  }

  /**
   * Performs a query of the given type
   *
   * @param  QueryBuilder Query
   * @param  index        The index
   * @param  from         Offset to start from
   * @param  size         The number of search hits to return
   * @return              Result of query
   * @see
   */
  public SearchResponse query(QueryBuilder query, String index, int from, int size) {
    SearchResponse searchResponse = null;

    try {
      SearchRequest searchRequest = null;

      if (index == null || index.isEmpty())
        searchRequest = new SearchRequest();
      else
        searchRequest = new SearchRequest(index);

      SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
      searchSourceBuilder.query(/*QueryBuilders.matchAllQuery()*/query);
      searchSourceBuilder.from(from);
      searchSourceBuilder.size(size);
      searchRequest.source(searchSourceBuilder);

      searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

      setRestStatus(searchResponse.status());
      /* TimeValue took = searchResponse.getTook();
      Boolean terminatedEarly = searchResponse.isTerminatedEarly();
      boolean timedOut = searchResponse.isTimedOut();*/
    } catch (IOException e) {
      setResult(1, e.getMessage());
    } catch (ElasticsearchStatusException e) {
      setRestStatus(e.status());
      setResult(1, e.getMessage());
    }

    return(searchResponse);
  }

  /**
   * Performs a query of the given type
   *
   * @param  QueryBuilder Query
   * @param  index        The index
   * @return              All hits as JSONArray
   * @see
   */
  public JSONArray queryToJSON(QueryBuilder query, String index) {
    int from = 0, size = 10;
    JSONArray ja = null;

    while(true) {
      SearchResponse searchResponse = query(query, index, from, size);

      if (searchResponse == null)
        break;

      RestStatus status = searchResponse.status();
      setRestStatus(status);

      if (status != RestStatus.OK)
        return null;

      if (ja == null)
        ja = new JSONArray();

      SearchHits hits = searchResponse.getHits();

      if (hits.getHits().length == 0)
        break;

      //  TotalHits totalHits = hits.getTotalHits();
      //long numHits = totalHits.value;
      //System.out.println("Hits found: "+numHits);
      //System.out.println("Hits retrieved: "+hits.getHits().length);

      SearchHit[] searchHits = hits.getHits();
      for (SearchHit hit : searchHits) {
          Map<String, Object> sourceAsMap = hit.getSourceAsMap();
          //System.out.println(id+" "+sourceAsMap.get("name"));
          JSONObject jo = new JSONObject(sourceAsMap);
          jo.put("_id", hit.getId());
          ja.add(jo);
      }

      from += size;
    }

    return(ja);
  }
}
