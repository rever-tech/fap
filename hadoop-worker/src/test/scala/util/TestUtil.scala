package util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import worker.hadoop.util.ResourceControl

/**
  * Created by tiennt4 on 18/12/2016.
  */
trait TestUtil {
  def assertFileExist(path: Path): Unit = {
    println(path.toString)
    assert(path.getFileSystem(new Configuration()).exists(path))
  }

  def assertFileNotExist(path: Path): Unit = {
    println(path.toString)
    assert(!path.getFileSystem(new Configuration()).exists(path))
  }

  def getMd5HexOfFile(path: Path): String = {
    ResourceControl.using(path.getFileSystem(new Configuration()).open(path)) {
      is => org.apache.commons.codec.digest.DigestUtils.md5Hex(is)
    }
  }
}
