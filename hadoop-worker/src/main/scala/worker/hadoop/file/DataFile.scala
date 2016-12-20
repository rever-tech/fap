package worker.hadoop.file

import org.apache.parquet.hadoop.ParquetWriter

/**
  * Created by tiennt4 on 13/12/2016.
  *
  * @param name     relative path of file
  * @param fullPath path of file in FS
  * @todo name and fullPath needed tobe resolve
  */
case class DataFile(name: String, fullPath: String, writer: ParquetWriter[String]) {
  def write(record: String): Unit = writer.write(record)

  def close(): Unit = writer.close()

  def toJson(): String = s"""{"name": "$name", "fullPath": "$fullPath"}"""
}