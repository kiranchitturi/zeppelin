package org.apache.zeppelin.solr

import org.apache.zeppelin.solr.JsonUtil._
import org.json4s.JsonAST.{JArray, JString, JValue}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

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
}
