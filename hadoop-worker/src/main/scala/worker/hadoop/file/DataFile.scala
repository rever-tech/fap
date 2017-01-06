package worker.hadoop.file

import worker.hadoop.writer.FileWriter

/**
  * Created by tiennt4 on 13/12/2016.
  *
  * @param name     relative path of file
  * @param fullPath path of file in FS
  */
case class DataFile(name: String, fullPath: String, writer: FileWriter[String]) {
  def write(record: String): Unit = writer.writeObject(record)

  def close(): Unit = writer.close()

  def toJson(): String = s"""{"name": "$name", "fullPath": "$fullPath"}"""
}