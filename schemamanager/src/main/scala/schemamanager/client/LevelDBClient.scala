package schemamanager.client

import java.io.File

import com.google.inject.Singleton
import org.fusesource.leveldbjni.JniDBFactory
import org.iq80.leveldb.{CompressionType, DB, Options}

/**
 * @author sonpn
 */
@Singleton
case class LevelDBClient(levelDbDir: String) {
  val options = new Options()
    .createIfMissing(true)
    .compressionType(CompressionType.NONE)
    .cacheSize(10 * 1024 * 1024)

  val levelDBFile = new File(levelDbDir)
  levelDBFile.exists() match {
    case false => levelDBFile.mkdirs()
    case _ =>
  }

  val factory = JniDBFactory.factory

  val db: DB = factory.open(levelDBFile, options)

  def close = {
    db.close()
  }

  def destroy: Unit = {
    factory.destroy(levelDBFile, new Options)
  }

  def getDB = db
}
