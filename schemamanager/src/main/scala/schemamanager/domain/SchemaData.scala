package schemamanager.domain

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

import scala.collection.Map

/**
 * @author sonpn
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class SchemaData(nameToType: Map[String, String])
