package worker.hadoop.service

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import com.google.inject.Inject
import com.google.inject.name.Named
import com.twitter.inject.Logging
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.io.{Charsets, IOUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.{Deserializer, IntegerDeserializer, StringDeserializer}
import worker.hadoop.file.{DataSection, FileNamingStrategy}
import worker.hadoop.schema.SchemaInfo
import worker.hadoop.util.ResourceControl

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
  * Created by zkidkid on 12/5/16.
  */
class HadoopStringWorker @Inject()(@Named("worker_config") val workerConfig: String,
                                   @Named("kafka_config") kafkaConfig: String,
                                   schemaService: SchemaManagerService)
  extends AbstractKafkaWorker[Integer, String](kafkaConfig) with Logging {

  final var config: Config = _
  private final val workerName = config.getString("worker_name")
  private final val workDir = config.getString("work_dir")
  private final val writerClass = config.getString("file_writer.class_name")
  private final val writerConfig = if (config.hasPath("file_writer.config")) {
    config.getConfig("file_writer.config").entrySet()
      .map(e => e.getKey -> e.getValue.unwrapped().toString).toMap
  } else null

  override def keyDeserialize(): Deserializer[Integer] = new IntegerDeserializer()

  override def valueDeserialize(): Deserializer[String] = new StringDeserializer()

  override def subscribeList(): Seq[String] = {
    //Not good when parse config here
    config = ConfigFactory.parseString(workerConfig)
    config.getStringList("topics")
  }

  private val dataSectionsLock = new ReentrantLock()
  private val dataSections: mutable.Map[String, DataSection] = mutable.Map.empty

  //Should it moved to outside?
  private val fileForwardWorker: FileForwardWorker = new FileForwardWorker(config.getConfig("file_forwarder_config"))

  private val fileNaming: FileNamingStrategy = {
    val fileNamingConfig = config.getConfig("file_naming_config")
    val className = fileNamingConfig.getString("class")
    val constructor = Class.forName(className).getConstructor(classOf[Config])
    constructor.newInstance(fileNamingConfig.getConfig("config")).asInstanceOf[FileNamingStrategy]
  }

  private val notifier: NotifyService = {
    val notifierConfig = config.getConfig("notifier_config")
    val className = notifierConfig.getString("class")
    val constructor = Class.forName(className).getConstructor(classOf[Config])
    constructor.newInstance(notifierConfig.getConfig("config")).asInstanceOf[NotifyService]
  }

  //region Section Checker background thread
  /**
    * Interval for section checker (second)
    */
  private val checkingInterval: Long = config.getDuration("checking_section_interval").getSeconds * 1000

  /**
    * A background thread that check for finish data section and push it to file forwarder
    */
  lazy val sectionChecker = new Thread() {
    val isRunning: AtomicBoolean = new AtomicBoolean(false)

    override def run(): Unit = {
      var startTimestamp: Long = System.currentTimeMillis()
      var sleepTime: Long = checkingInterval
      while (isRunning.get()) {
        info("[SectionChecker] running section checker")
        startTimestamp = System.currentTimeMillis()
        val finishedSections = dataSections.filter(section => fileNaming.isSectionEnd(section._2))
        try {
          dataSectionsLock.lock()
          finishedSections.foreach(section => {
            val dataSection = section._2
            infoResult("[SectionChecker] schedule forward data section: %s") {
              dataSection.sectionDir.toUri.toString
            }
            fileForwardWorker.schedule(dataSection)(forwardCallback(dataSection))
            dataSections.remove(section._1)
          })
        } finally {
          dataSectionsLock.unlock()
        }

        sleepTime = checkingInterval - (System.currentTimeMillis() - startTimestamp)
        //Do sleep
        infoResult("[SectionChecker] will be sleep %s mills") {
          sleepTime
        }
        if (sleepTime > 0)
          Thread.sleep(sleepTime)
      }
    }

    override def start(): Unit = {
      isRunning.set(true)
      super.start()
    }

    def stopSafe(): Unit = isRunning.set(false)
  }

  def forwardCallback(dataSection: DataSection): Try[Unit] => Unit = {
    case Success(_) =>
      //Commit offset
      debug(s"Forward succeed, finish data section: ${dataSection.getMetaString}")
      try {

        //This method is async, commit offset must be in same thread with polling data.
        queueCommitOffsets(dataSection.topicName, dataSection.offsetInfo.toMap)
        fileForwardWorker.commitSucceed(dataSection.topicName)
        dataSection.clean()
        debug(s"Data section cleaned. ${dataSection.toString}")
      } catch {
        case e: Throwable => error("Commit offset and clean data section failure.", e)
      }

    case Failure(t) =>
      //TODO: Implement notify when failure
      error("Forward data section failure", t)
      notifier.notify("", "", t)
      fileForwardWorker.commitFailure(dataSection.topicName)
  }

  //endregion

  override def beforeStart(): Unit = {
    //Start file forwarder worker
    fileForwardWorker.start()
    //Start section checker
    sectionChecker.start()

    try {
      val workPath = new Path(workDir)
      val fs = workPath.getFileSystem(new Configuration(false))
      if (fs.exists(workPath)) {
        val sections = fs.listStatus(workPath, new PathFilter {
          override def accept(path: Path): Boolean = fs.exists(new Path(path, DataSection.METADATA_FILE_NAME))
        }).map(section => {
          val json = ResourceControl.using(fs.open(new Path(section.getPath, DataSection.METADATA_FILE_NAME))) {
            reader => IOUtils.toString(reader, Charsets.UTF_8);
          }
          DataSection(json)
        }).sortBy(dataSection => dataSection.timestamp)

        info(s"Found old data: \n${sections.map(section => section.getMetaString).mkString("\n")}")

        //TODO: getAssigned
        // schedule forward
      }
    } catch {
      case ex: Throwable => error("Cannot recover old data.", ex)
    }
  }

  override def afterStop(): Unit = {
    sectionChecker.stopSafe()
    //TODO: Implement ensure close and commit offset
    dataSections.foreach(section => {
      val dataSection = section._2
      fileForwardWorker.schedule(dataSection)(forwardCallback(dataSection))
    })
  }

  private def createDataSection(topicName: String, sectionName: String, sectionTimestamp: Long): DataSection = {
    infoResult("[Hadoop Worker] Create DataSection: %s") {
      DataSection(workerName, workDir, topicName, sectionTimestamp,
        System.currentTimeMillis(), fileNamingStrategy = fileNaming,
        writerClass = writerClass, writerConfig = writerConfig)
    }
  }


  /**
    *
    * This function must be sequentially processed
    *
    * @param record data from kafka
    * @todo benchmark
    */
  override def process(record: ConsumerRecord[Integer, String]): Unit = {
    debug(s"Consume record of topic `${record.topic()}`, partition: ${record.partition()}, offset: ${record.offset()}")
    debug(record.value())
    val schema = schemaService.getSchema(SchemaInfo(record.topic(), record.key()))
    val sectionInfo = fileNaming.getSectionInfo(record.topic(), System.currentTimeMillis())

    try {
      dataSectionsLock.lock()
      if (!dataSections.contains(sectionInfo._1)) {
        dataSections.put(sectionInfo._1, createDataSection(record.topic(), sectionInfo._1, sectionInfo._2))
      }
      dataSections(sectionInfo._1).write(schema, record)
    } finally {
      dataSectionsLock.unlock()
    }
    //    if (!dataSections.contains(sectionInfo._1)) {
    //      //Create section
    //      try {
    //        dataSectionsLock.lock()
    //        if (!dataSections.contains(sectionInfo._1)) {
    //          dataSections.put(sectionInfo._1, createDataSection(record.topic(), sectionInfo._1, sectionInfo._2))
    //        }
    //      } finally {
    //        dataSectionsLock.unlock()
    //      }
    //    }
    //    dataSections(sectionInfo._1).write(schema, record)
  }
}
