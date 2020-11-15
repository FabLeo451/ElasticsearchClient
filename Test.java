
import com.lionsoft.elasticsearch.ElasticsearchClient;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchHit;
import org.apache.lucene.search.TotalHits;
import java.util.*;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Test {
  static ElasticsearchClient e = new ElasticsearchClient("http", "localhost", 9200);

  public static void main(String[] input) {

    if (e.ping()) {
      System.out.println("Ping OK");
      
      /*long n = e.emptyIndex("idx-test");
      System.out.println("Deleted: "+n);

      Map<String, Object> mapObject = new HashMap<String, Object>();
      mapObject.put("name", "Pink");
      e.putDocument("idx-test", "15", mapObject);

      JSONParser jsonParser = new JSONParser();
      JSONObject jo = null;

      try {
        jo = (JSONObject) jsonParser.parse("{\"name\":\"Floyd\"}");
      } catch (ParseException ex) {
          ex.printStackTrace();
      }

      e.putDocumentJSON("idx-test", null, jo);

      System.out.println("Status: "+e.getRestStatus());
      */

      //testBulk();
      testQuery();
    }
    else
      System.out.println("No server found.");

    e.close();
  }

  public static void testQuery() {
    //JSONArray ja = e.queryToJSON(QueryBuilders.matchAllQuery(), "idx-test");
    JSONArray ja = e.queryToJSON(QueryBuilders.termQuery("_id", "100"), "idx-test");
    //JSONArray ja = e.queryToJSON(QueryBuilders.matchQuery("name", "Floyd"), "idx-test");

    if (ja != null)
      System.out.println(ja);
    else
      System.out.println("ERROR: "+e.getMessage());
  }

  public static void testBulk() {
    JSONArray ja;
    JSONParser jsonParser = new JSONParser();

    try {
      ja = (JSONArray) jsonParser.parse("[{\"name\":\"Pink\"}, {\"name\":\"Floyd\"}, {\"_id\":\"100\", \"name\":\"Fabio Leone\"}]");

      if (e.bulkInsert("idx-test", ja)) {
        System.out.println("Status: "+e.getRestStatus());

        BulkResponse bulkResponse = e.getBulkResponse();
        System.out.println("Has failures: "+bulkResponse.hasFailures());

        int nIns = 0, nUpd = 0, nErr = 0;

        for (BulkItemResponse bulkItemResponse : bulkResponse) {
            DocWriteResponse itemResponse = bulkItemResponse.getResponse();

            switch (bulkItemResponse.getOpType()) {
              case INDEX:
              case CREATE:
                  //System.out.println("CREATE");
                  IndexResponse indexResponse = (IndexResponse) itemResponse;

                  if (indexResponse.status() == RestStatus.CREATED)
                    nIns ++;
                  else if (indexResponse.status() == RestStatus.OK)
                    nUpd ++;
                  else
                    nErr ++;
                  break;
              /*
              case UPDATE:
                  System.out.println("UPDATE");
                  UpdateResponse updateResponse = (UpdateResponse) itemResponse;

                  if (updateResponse.status() == RestStatus.OK)
                    nUpd ++;
                  else
                    nErr ++;
                  break;

              case DELETE:
                  DeleteResponse deleteResponse = (DeleteResponse) itemResponse;
              }*/
              default:
                break;
          }
        }

        System.out.println("Created: "+nIns);
        System.out.println("Updated: "+nUpd);
        System.out.println("Errors: "+nErr);
      }
      else
        System.out.println("ERROR");
    } catch (ParseException ex) {
        ex.printStackTrace();
    }
  }
}
