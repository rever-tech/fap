package schemamanager.util

import java.lang.reflect.{ParameterizedType, Type}

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper, PropertyNamingStrategy}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

/**
 * Created by phuonglam on 10/3/16.
 **/
object JsonUtils {
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES)
  def fromJson[T: Manifest](json: String): T = {
    mapper.readValue[T](json, typeReference[T])
  }
  def readTree(json: String): JsonNode = mapper.readTree(json)
  def toJson[T](t: T): String = {
    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(t)
  }
  private[this] def typeReference[T: Manifest] = new TypeReference[T] {
    override def getType: Type = typeFromManifest(manifest[T])
  }
  private[this] def typeFromManifest(m: Manifest[_]): Type = {
    if (m.typeArguments.isEmpty) {
      m.runtimeClass
    }
    else new ParameterizedType {
      def getRawType = m.runtimeClass
      def getActualTypeArguments = m.typeArguments.map(typeFromManifest).toArray
      def getOwnerType = null
    }
  }
}