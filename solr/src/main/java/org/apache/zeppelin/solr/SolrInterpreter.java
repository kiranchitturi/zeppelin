package org.apache.zeppelin.solr;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.zeppelin.annotation.ZeppelinApi;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * Interpreter for Apache Solr Search engine
 */
public class SolrInterpreter extends Interpreter {

  private static Logger logger = LoggerFactory.getLogger(SolrInterpreter.class);
  public static final String ZK_HOST = "solr.zkhost";

  private String zkHost;
  private CloudSolrClient solrClient;
  public SolrInterpreter(Properties property) {
    super(property);
  }
  private String collection;

  @ZeppelinApi
  public void open() {
    zkHost = getProperty(ZK_HOST);
    logger.info("Connecting to Zookeeper host {}", zkHost);
    solrClient = SolrSupport.getCachedCloudClient(zkHost);
  }

  @ZeppelinApi
  public void close() {}

  @ZeppelinApi
  public InterpreterResult interpret(String st, InterpreterContext context) {
    logger.info("Running command '" + st + "'");

    if (st.isEmpty() || st.trim().isEmpty()) {
      return new InterpreterResult(InterpreterResult.Code.SUCCESS);
    }
    String[] args = st.split(" ");

    if ("list".equals(args[0])) {
      return new InterpreterResult(InterpreterResult.Code.SUCCESS, InterpreterResult.Type.TEXT, SolrQuerySupport.getCollectionsListAsString(zkHost));
    }

    if ("use".equals(args[0])) {
      if (args.length == 2) {
        collection = args[1];
        InterpreterResult result = SolrQuerySupport.transformLukeResponseToZeppelinAction(SolrQuerySupport.getFieldsFromLuke(zkHost, collection));
        if (result.code().equals(InterpreterResult.Code.SUCCESS)) {
          result.add(InterpreterResult.Type.TEXT,  "Setting collection " + collection + " as default");
        }
        return result;
      } else {
        String msg = "Specify the collection to use for this dashboard. Example: use {collection_name}";
        return new InterpreterResult(InterpreterResult.Code.INCOMPLETE, InterpreterResult.Type.TEXT, msg);
      }
    }

    if ("search".equals(args[0])) {
      if (args.length == 2) {
          try {
            QueryResponse response = doQuery(args[1]);
            if (response.getStatus() != 0) {
              return new InterpreterResult(InterpreterResult.Code.ERROR, InterpreterResult.Type.TEXT, response.getResponse().toString());
            }
          } catch (Exception e) {
            return new InterpreterResult(InterpreterResult.Code.INCOMPLETE, InterpreterResult.Type.TEXT, e.getMessage());
          }
      } else {
        String msg = "Specify the query params to search with. Example: search q=Fellas&fq=genre:action";
        return new InterpreterResult(InterpreterResult.Code.INCOMPLETE, InterpreterResult.Type.TEXT, msg);
      }
    }

    return null;
  }

  private QueryResponse doQuery(String queryParamString) throws IOException, SolrServerException {
    SolrQuery solrQuery = SolrQuerySupport.toQuery(queryParamString);
    QueryRequest request = new QueryRequest(solrQuery, SolrRequest.METHOD.POST);
    QueryResponse response = request.process(solrClient, collection);
    return response;
  }

  @Override
  public void cancel(InterpreterContext context) {

  }

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }
}
