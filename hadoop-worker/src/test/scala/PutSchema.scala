import java.net.InetSocketAddress

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.thrift.ThriftClientFramedCodec
import com.twitter.util.Await
import parquet.schema.json.{JsonSchema, JsonType}
import schemamanager.domain.{TSchema, TSchemaData}
import schemamanager.service.TSchemaManager.FinagledClient

/**
  * Created by tiennt4 on 04/01/2017.
  */
object PutSchema {
  def main(args: Array[String]): Unit = {
    val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

    val schemaString =
      """{
        |  "$os": "string",
        |  "ssid": "string",
        |  "uuid": "string",
        |  "$browser": "string",
        |  "$browser_version": "string",
        |  "$screen_height": "int",
        |  "$screen_width": "int",
        |  "$current_url": "string",
        |  "$referral": "string",
        |  "$title": "string",
        |  "$initial_referrer": "string",
        |  "$initial_referring_domain": "string",
        |  "type": "string",
        |  "subtype": "string",
        |  "event": "string",
        |  "object_id": "string",
        |  "extra": "string"
        |}""".stripMargin


    val schema = JsonSchema("people_analytic", 1, schemaString)


    val map = objectMapper.readValue(schemaString, classOf[Map[String, Any]]).map(e => e._2 match {
      case map: Map[Any, Any] => e._1 -> objectMapper.writeValueAsString(map)
      case seq: Seq[Any] => e._1 -> objectMapper.writeValueAsString(seq)
      case any => e._1 -> any.toString
    })
    val client = new FinagledClient(
      ClientBuilder()
        .hosts(Seq(new InetSocketAddress("172.16.100.1", 10118)))
        .codec(ThriftClientFramedCodec())
        .hostConnectionLimit(1)
        .retries(5)
        .build())
    val result = Await.result(client.addSchema(TSchema("admin", 1, TSchemaData(map))))
    println("Result: " + result)

    val getSchema = Await.result(client.getSchema("admin", 1)).data.get
    val parsedSchema = JsonType.parseSchema(getSchema.name, getSchema.version, getSchema.schema.nameToType.toMap)
    println(parsedSchema == schema)

    client.service.close()
  }
}
