package parquet.writer


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetWriter}
import parquet.schema.json.JsonSchema

/**
  * Created by tiennt4 on 06/12/2016.
  */

object JsonParquetWriter {
  def builder(file: Path): Builder = new Builder(file)
  def builder(file: String): Builder = builder(new Path(file))
}

class JsonParquetWriter(val file: Path,
                        mode: ParquetFileWriter.Mode,
                        writeSupport: WriteSupport[String],
                        compressionCodecName: CompressionCodecName,
                        blockSize: Int, pageSize: Int,
                        dictionaryPageSize: Int,
                        enableDictionary: Boolean,
                        validating: Boolean,
                        writerVersion: ParquetProperties.WriterVersion,
                        conf: Configuration)
  extends ParquetWriter[String](file: Path,
    mode: ParquetFileWriter.Mode,
    writeSupport: WriteSupport[String],
    compressionCodecName: CompressionCodecName,
    blockSize: Int, pageSize: Int,
    dictionaryPageSize: Int,
    enableDictionary: Boolean,
    validating: Boolean,
    writerVersion: ParquetProperties.WriterVersion,
    conf: Configuration) {

}

class Builder(file: Path) extends ParquetWriter.Builder[String, Builder](file: Path) {

  var schema : JsonSchema = _

  def withSchema(schema : JsonSchema): Builder = {
    this.schema = schema
    this
  }

  override protected def self: Builder = this

  override protected def getWriteSupport(conf: Configuration): WriteSupport[String] =
    new JsonWriteSupport(schema, conf)

}