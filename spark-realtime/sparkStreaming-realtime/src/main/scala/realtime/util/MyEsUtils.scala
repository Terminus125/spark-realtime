package realtime.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder

import java.util
import scala.collection.mutable.ListBuffer

/**
 * ES 工具类，用于对 ES 进行读写操作
 *
 * @author Akaza
 */

object MyEsUtils {


  // 客户端对象
  val esClinet: RestHighLevelClient = build()

  // 创建客户端对象
  def build(): RestHighLevelClient = {
    val host: String = MyPropsUtils(MyConfig.ES_HOST)
    val port: String = MyPropsUtils(MyConfig.ES_PORT)
    val restClientBuilder: RestClientBuilder = RestClient.builder(new HttpHost(host, port.toInt))
    val client: RestHighLevelClient = new RestHighLevelClient(restClientBuilder)
    client
  }

  // 关闭 ES 对象
  def close(): Unit = {
    if (esClinet != null) esClinet.close()
  }

  // 幂等批量写入 ES
  def bulkSave(indexName: String, docs: List[(String, AnyRef)]): Unit = {
    val bulkRequest: BulkRequest = new BulkRequest(indexName)
    for ((docId, docObj) <- docs) {
      val indexRequest: IndexRequest = new IndexRequest()
      val dataJson: String = JSON.toJSONString(docObj, new SerializeConfig(true))
      indexRequest.source(dataJson, XContentType.JSON)
      indexRequest.id(docId)
      bulkRequest.add(indexRequest)
    }
    esClinet.bulk(bulkRequest, RequestOptions.DEFAULT)
  }

  // 查询指定字段
  def searchField(indexName: String, fieldName: String): List[String] = {
    // 第一条数据写入 ES 前程序挂掉，索引未创建
    // 判断索引是否存在
    val getIndexRequest: GetIndexRequest = new GetIndexRequest(indexName)
    val isExists: Boolean =
      esClinet.indices().exists(getIndexRequest, RequestOptions.DEFAULT)
    if (!isExists) {
      return null
    }

    // 写入 ES 中途程序挂掉，索引已创建的情况
    val mids: ListBuffer[String] = ListBuffer[String]()
    val searchRequest: SearchRequest = new SearchRequest(indexName)
    val searchSourceBuilder: SearchSourceBuilder = new SearchSourceBuilder()
    searchSourceBuilder.fetchSource(fieldName, null).size(100000)
    searchRequest.source(searchSourceBuilder)
    val searchResponse: SearchResponse =
      esClinet.search(searchRequest, RequestOptions.DEFAULT)
    val hits: Array[SearchHit] = searchResponse.getHits.getHits
    for (hit <- hits) {
      val sourceMap: util.Map[String, AnyRef] = hit.getSourceAsMap
      val mid: String = sourceMap.get(fieldName).toString
      mids.append(mid)
    }
    mids.toList
  }
}
