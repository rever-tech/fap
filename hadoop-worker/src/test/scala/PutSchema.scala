import java.net.InetSocketAddress

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.thrift.ThriftClientFramedCodec
import com.twitter.util.Await
import schemamanager.domain.{TFieldSchema, TSchema, TSchemaData}
import schemamanager.service.TSchemaManager.FinagledClient

/**
  * Created by tiennt4 on 04/01/2017.
  */
object PutSchema {
  def main(args: Array[String]): Unit = {
    val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

    val schemaString =
      """[{ "name": "$os", "type": "string" },
        |{ "name": "$browser", "type": "string" },
        |{ "name": "$browser_version", "type": "string" },
        |{ "name": "$screen_height", "type": "int" },
        |{ "name": "$screen_width", "type": "int" },
        |{ "name": "$current_url", "type": "string" },
        |{ "name": "$referral", "type": "string" },
        |{ "name": "$title", "type": "string" },
        |{ "name": "$initial_referrer", "type": "string" },
        |{ "name": "$initial_referring_domain", "type": "string" },
        |{ "name": "ssid", "type": "string" },
        |{ "name": "uuid", "type": "string" },
        |{ "name": "$el_id", "type": "string" },
        |{ "name": "$el_name", "type": "string" },
        |{ "name": "$el_text", "type": "string" },
        |{ "name": "$event_type", "type": "string" }]
      """.stripMargin

    val schemaName = "auto_track"
    val schemaVersion = 1
    //    val schema = JsonSchema("admin", 1, schemaString)


    val map = objectMapper.readValue(schemaString, classOf[Seq[Map[String, Any]]]).map(e => e("type") match {
      case map: Map[Any, Any] => (e("name").asInstanceOf[String], objectMapper.writeValueAsString(map))
      case seq: Seq[Any] => (e("name").asInstanceOf[String], objectMapper.writeValueAsString(seq))
      case any => (e("name").asInstanceOf[String], any.toString)
    })
    val client = new FinagledClient(
      ClientBuilder()
        .hosts(Seq(new InetSocketAddress("fap.orever.vn", 10118)))
        .codec(ThriftClientFramedCodec())
        .hostConnectionLimit(1)
        .retries(5)
        .build())
    val result = Await.result(client.addSchema(TSchema(schemaName, schemaVersion, TSchemaData(map.map(f => TFieldSchema(f._1, f._2))))))
    println("Result: " + result)


    val getSchema = Await.result(client.getSchema(schemaName, schemaVersion)).data.get
    println(getSchema)
    //    val parsedSchema = JsonType.parseSchema(getSchema.name, getSchema.version, getSchema.schema.nameToType.toMap)
    //    println(parsedSchema == schema)

    client.service.close()
  }
}
