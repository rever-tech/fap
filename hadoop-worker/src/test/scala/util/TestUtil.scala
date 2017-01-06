package util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import worker.hadoop.util.ResourceControl

/**
  * Created by tiennt4 on 18/12/2016.
  */
trait TestUtil {

  private final val conf = new Configuration()

  def assertFileExist(path: Path): Unit = {
    println(path.toString)
    assert(path.getFileSystem(conf).exists(path), s"File ${path.toString} should exist")
  }

  def assertFileNotExist(path: Path): Unit = {
    println(path.toString)
    assert(!path.getFileSystem(conf).exists(path), s"File ${path.toString} should not exist")
  }

  def getMd5HexOfFile(path: Path): String = {
    ResourceControl.using(path.getFileSystem(conf).open(path)) {
      is => org.apache.commons.codec.digest.DigestUtils.md5Hex(is)
    }
  }

  def deleteFile(path: String): Unit = {
    deleteFile(new Path(path))
  }

  def deleteFile(path: Path): Unit = {
    path.getFileSystem(conf).delete(path, true)
  }
}
