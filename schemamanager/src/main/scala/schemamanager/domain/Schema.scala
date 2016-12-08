package schemamanager.domain

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

/**
 * @author sonpn
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class Schema(name: String, version: Int, schema: SchemaData)
