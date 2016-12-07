package schemamanager

import schemamanager.module.SchemaModuleTest

/**
 * @author sonpn
 */
class ServerTest extends Server{
  override val modules = Seq(SchemaModuleTest)

  override def warmup() {
  }

}
