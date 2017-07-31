package org.apache.zeppelin.solr

import java.net.URLDecoder

import org.apache.solr.client.solrj.SolrRequest.METHOD
import org.apache.solr.client.solrj._
import org.apache.solr.client.solrj.impl.StreamingBinaryResponseParser
import org.apache.solr.client.solrj.request.{LukeRequest, QueryRequest}
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.common.params.SolrParams
import org.apache.solr.common.util.NamedList
import org.apache.zeppelin.interpreter.InterpreterResult
import org.apache.zeppelin.solr.JsonUtil._
import org.apache.zeppelin.solr.util.QueryConstants
import org.json4s.JsonAST.{JArray, JString, JValue}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.mutable.ListBuffer

case class SolrField(name: String, fieldType: String, docs: Int)
case class SolrLukeResponse(numDocs: Integer, solrFields: List[SolrField])
case class SolrFieldMeta(
                          fieldType: String,
                          dynamicBase: Option[String],
                          isRequired: Option[Boolean],
                          isMultiValued: Option[Boolean],
                          isDocValues: Option[Boolean],
                          isStored: Option[Boolean],
                          fieldTypeClass: Option[String])

object SolrQuerySupport {
  val logger = LoggerFactory.getLogger(SolrQuerySupport.getClass)

  def getCollectionsList(zkHost: String): List[String] = {
    val baseUrl = SolrSupport.getSolrBaseUrl(zkHost)
    val collectionsListUrl = baseUrl + "admin/collections?action=LIST&wt=json"

    val jsonOut: JValue = SolrJsonSupport.getJson(collectionsListUrl)

    if (jsonOut.has("collections")) {
      jsonOut \ "collections" match {
        case list: JArray => {
          val arrayList: ListBuffer[String] = new ListBuffer[String]
          list.arr.foreach {
            case s: JString => arrayList.+=(s.s)
          }
          return arrayList.toList
        }
        case _ => return List.empty
      }
    }
    List.empty
  }

  def getCollectionsListAsString(zkHost: String): String = {
    getCollectionsList(zkHost).mkString("\n")
  }

  def toQuery(queryString: String): SolrQuery = {

    var solrQuery: SolrQuery = new SolrQuery
    if (queryString == null || queryString.isEmpty) {
      solrQuery = solrQuery.setQuery("*:*")
    } else {
      // Check to see if the query contains additional parameters. E.g., q=*:*&fl=id&sort=id asc
      if (!queryString.contains("q=")) {
        // q= is required if passing list of name/value pairs, so if not there, whole string is the query
        solrQuery.setQuery(queryString)
      } else {
        val paramsNL = new NamedList[Object]()
        val params = queryString.split("&")
        for (param <- params) {
          // only care about the first equals as value may also contain equals
          val eqAt = param.indexOf('=')
          if (eqAt != -1) {
            val key = param.substring(0, eqAt)
            val value = URLDecoder.decode(param.substring(eqAt + 1), "UTF-8")
            if (key == "sort") {
              if (!value.contains(" ")) {
                solrQuery.addSort(SolrQuery.SortClause.asc(value))
              } else {
                val split = value.split(" ")
                solrQuery.addSort(SolrQuery.SortClause.create(split(0), split(1)))
              }
            } else {
              paramsNL.add(key, value)
            }
          }
        }
        if (paramsNL.size() > 0) {
          solrQuery.add(SolrParams.toSolrParams(paramsNL))
        }
      }
    }
    val rows = solrQuery.getRows
    if (rows == null)
      solrQuery.setRows(10)

    logger.info(s"Constructed SolrQuery: $solrQuery from user-supplied query param: $queryString")
    solrQuery
  }

  def querySolr(
                 solrClient: SolrClient,
                 solrQuery: SolrQuery,
                 startIndex: Int,
                 cursorMark: String): Option[QueryResponse] =
    querySolr(solrClient, solrQuery, startIndex, cursorMark, null)

  // Use this method instead of [[SolrClient.queryAndStreamResponse]] to use POST method for queries
  def queryAndStreamResponsePost(params: SolrParams, callback: StreamingResponseCallback, cloudClient: SolrClient): QueryResponse = {
    val parser: ResponseParser = new StreamingBinaryResponseParser(callback)
    val req: QueryRequest = new QueryRequest(params, METHOD.POST)
    req.setStreamingResponseCallback(callback)
    req.setResponseParser(parser)
    req.process(cloudClient)
  }

  /*
    Query solr and retry on Socket or network exceptions
   */
  def querySolr(
                 solrClient: SolrClient,
                 solrQuery: SolrQuery,
                 startIndex: Int,
                 cursorMark: String,
                 callback: StreamingResponseCallback): Option[QueryResponse] = {
    var resp: Option[QueryResponse] = None

    try {
      if (cursorMark != null) {
        solrQuery.setStart(0)
        solrQuery.set("cursorMark", cursorMark)
        if (solrQuery.get("sort") == null || solrQuery.get("sort").isEmpty) {
          addDefaultSort(solrQuery, QueryConstants.DEFAULT_REQUIRED_FIELD)
        }
      } else {
        solrQuery.setStart(startIndex)
      }

      if (solrQuery.getRows == null)
        solrQuery.setRows(QueryConstants.DEFAULT_PAGE_SIZE)

      if (callback != null) {
        resp = Some(queryAndStreamResponsePost(solrQuery, callback, solrClient))
      } else {
        resp = Some(solrClient.query(solrQuery, METHOD.POST))
      }
    } catch {
      case e: Exception =>
        logger.error("Query [" + solrQuery + "] failed due to: " + e)

        //re-try once in the event of a communications error with the server
        if (SolrSupport.shouldRetry(e)) {
          try {
            Thread.sleep(2000L)
          } catch {
            case ie: InterruptedException => Thread.interrupted()
          }

          try {
            if (callback != null) {
              resp = Some(queryAndStreamResponsePost(solrQuery, callback, solrClient))
            } else {
              resp = Some(solrClient.query(solrQuery, METHOD.POST))
            }
          } catch {
            case execOnRetry: SolrServerException =>
              logger.error("Query on retry [" + solrQuery + "] failed due to: " + execOnRetry)
              throw execOnRetry
            case execOnRetry1: Exception =>
              logger.error("Query on retry [" + solrQuery + "] failed due to: " + execOnRetry1)
              throw new SolrServerException(execOnRetry1)
          }
        } else {
          e match {
            case e1: SolrServerException => throw e1
            case e2: Exception => throw new SolrServerException(e2)
          }
        }
    }
    resp
  }

  def addDefaultSort(solrQuery: SolrQuery, uniqueKey: String): Unit = {
    if (solrQuery.getSortField == null || solrQuery.getSortField.isEmpty) {
      solrQuery.addSort(SolrQuery.SortClause.asc(uniqueKey))
      logger.info(s"Added default sort clause on uniqueKey field $uniqueKey to query $solrQuery")
    }
  }

  def getNumDocsFromSolr(collection: String, zkHost: String, query: Option[SolrQuery]): Long = {
    val solrQuery = if (query.isDefined) query.get else new SolrQuery().setQuery("*:*")
    val cloneQuery = solrQuery.getCopy
    cloneQuery.set("distrib", "true")
    cloneQuery.setRows(0)
    val cloudClient = SolrSupport.getCachedCloudClient(zkHost)
    val response = cloudClient.query(collection, cloneQuery)
    response.getResults.getNumFound
  }

  def getFieldsFromLuke(zkHost: String, collection: String): SolrLukeResponse = {
    val solrFields: ListBuffer[SolrField] = ListBuffer.empty
    val cloudClient = SolrSupport.getCachedCloudClient(zkHost)
    val lukeRequest = new LukeRequest()
    lukeRequest.setNumTerms(0)
    val lukeResponse = lukeRequest.process(cloudClient, collection)
    if (lukeResponse.getStatus != 0) {
      throw new RuntimeException(
        "Solr request returned with status code '" + lukeResponse.getStatus + "'. Response: '" + lukeResponse.getResponse.toString)
    }
    mapAsScalaMap(lukeResponse.getFieldInfo).foreach(f => {
      solrFields.+=(SolrField(f._1, f._2.getType, f._2.getDocs))
    })
    SolrLukeResponse(lukeResponse.getNumDocs, solrFields.toList)
  }

  def transformLukeResponseToZeppelinAction(lukeResponse: SolrLukeResponse): InterpreterResult = {
    val solrFields = lukeResponse.solrFields
    if (solrFields.isEmpty) {
      return new InterpreterResult(InterpreterResult.Code.ERROR, InterpreterResult.Type.TEXT, "Empty luke response. Make sure you have documents in the collection")
    }
    val interpreterResult = new InterpreterResult(InterpreterResult.Code.SUCCESS)
    val stringBuilder = new StringBuilder
    stringBuilder.++=("Name\tType\tNumberOfDocs\n")
    for (solrField <- solrFields) {
      stringBuilder.++=(s"${solrField.name}\t${solrField.fieldType}\t${solrField.docs}\t")
      stringBuilder.++=("\n")
    }
    interpreterResult.add(InterpreterResult.Type.TABLE, stringBuilder.toString())
    interpreterResult.add(InterpreterResult.Type.HTML, s"<font color=blue>Number of docs in collection: ${lukeResponse.numDocs}.</font>")
    interpreterResult
  }
}
