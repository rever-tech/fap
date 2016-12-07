package schemamanager.client

import java.io.File

import com.twitter.inject.Test
import org.fusesource.leveldbjni.JniDBFactory
import org.iq80.leveldb.{CompressionType, DB, Options, ReadOptions}

/**
 * @author sonpn
 */
class LevelDBTest extends Test {

  "level db jni" should {
    val options = new Options()
      .createIfMissing(true)
      .compressionType(CompressionType.NONE)
      .cacheSize(100 * 1024 * 1024); // 100MB cache

    // Using a memory pool to make native memory allocations more efficient
    "push memory pool" in {
      JniDBFactory.pushMemoryPool(1024 * 512)
    }

    val levelDbDir = new File("./db/leveldb_test_1")
    val db: DB = JniDBFactory.factory.open(levelDbDir, options)
    "put get delete" in {
      val key = "key"
      val value = "value"
      for (i <- 0 to 100) {
        val keyI = s"$key $i"
        val valueI = s"$value $i"
        db.put(bytes(keyI), bytes(valueI))
      }

      for (i <- 0 to 100) {
        val keyI = s"$key $i"
        val valueI = s"$value $i"

        val getValue = new String((db.get(bytes(keyI))))
        assertResult(valueI)(getValue)
      }

      for (i <- 0 to 100) {
        val keyI = s"$key $i"
        db.delete(bytes(keyI))
      }

    }

    "atomic update" in {
      val batch = db.createWriteBatch()
      try {


        db.write(batch)
      } finally {
        batch.close()
      }
    }

    "iterating key/ values" in {
      val iterator = db.iterator
      try {
        iterator.seekToFirst()
        while (iterator.hasNext) {
          val key = new String(iterator.peekNext().getKey)
          val value = new String(iterator.peekNext().getValue)
          println(s"$key - $value")
          iterator.next
        }
      } finally {
        iterator.close()
      }
    }

    "db status" in {
      println(db.getProperty("leveldb.stats"))
    }

    "close db" in {
      db.close()
    }

    "destroy db" in {
      JniDBFactory.factory.destroy(levelDbDir, new Options)
    }

    "pop memory pool" in {
      JniDBFactory.popMemoryPool()
    }
  }

}
