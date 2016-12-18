package worker.hadoop.service


import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import com.google.inject.Inject
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.{Deserializer, IntegerDeserializer, StringDeserializer}
import worker.hadoop.file.{DataSection, FileNamingStrategy}
import worker.hadoop.schema.SchemaInfo

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.util.{Failure, Success}

/**
  * Created by zkidkid on 12/5/16.
  */
class HadoopWorker @Inject()(workerConfig: String, kafkaConfig: String,
                             schemaService: SchemaManagerService)
  extends AbstractKafkaWorker[Integer, String](kafkaConfig) {

  private val config = ConfigFactory.parseString(workerConfig)
  private final val workerName = config.getString("worker_name")
  private final val workDir = config.getString("work_dir")

  override def keyDeserialize(): Deserializer[Integer] = new IntegerDeserializer()

  override def valueDeserialize(): Deserializer[String] = new StringDeserializer()

  override def subscribeList(): Seq[String] = config.getStringList("topics")

  private val dataSectionsLock = new ReentrantLock()
  private val dataSections: mutable.Map[String, DataSection] = mutable.Map.empty

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
  private val checkingInterval: Long = config.getDuration("checking_section_interval").getSeconds

  /**
    * A background thread that check for finish data section and push it to file forwarder
    */
  lazy val sectionChecker = new Thread() {
    val isRunning: AtomicBoolean = new AtomicBoolean(false)

    override def run(): Unit = {
      var startTimestamp: Long = System.currentTimeMillis()
      var sleepTime: Long = checkingInterval * 1000
      while (isRunning.get()) {
        startTimestamp = System.currentTimeMillis()
        val finishedSections = dataSections.filter(section => fileNaming.isSectionEnd(section._2))
        try {
          dataSectionsLock.lock()
          finishedSections.foreach(section => {
            val dataSection = section._2
            fileForwardWorker.schedule(dataSection)
              //With callback
            {
              case Success(_) =>
                //Commit offset
                commitOffsets(dataSection.topicName, dataSection.offsetInfo.toMap)
                fileForwardWorker.commitSucceed(dataSection.topicName)
                dataSection.clean()

              case Failure(t) =>
                //TODO: Implement notify when failure
                notifier.notify("", "")
                fileForwardWorker.commitFailure(dataSection.topicName)
            }
            dataSections.remove(section._1)
          })
        } finally {
          dataSectionsLock.unlock()
        }

        sleepTime = checkingInterval - (System.currentTimeMillis() - startTimestamp)
        //Do sleep
        Thread.sleep(sleepTime)
      }
    }

    override def start(): Unit = {
      isRunning.set(true)
      super.start()
    }

    def stopSafe(): Unit = isRunning.set(false)
  }
  //endregion

  override def beforeStart(): Unit = {
    //Start file forwarder worker
    fileForwardWorker.start()
    //Start section checker
    sectionChecker.start()

    //TODO: Implement recover local files
  }

  override def afterStop(): Unit = {
    sectionChecker.stopSafe()
    //TODO: Implement ensure close and commit offset
  }

  private def createDataSection(topicName: String, sectionName: String, sectionTimestamp: Long): DataSection =
    DataSection(workerName, workDir, topicName, sectionTimestamp, System.currentTimeMillis(), fileNaming)


  /**
    *
    * This function must be sequentially processed
    *
    * @param record data from kafka
    */
  override def process(record: ConsumerRecord[Integer, String]): Unit = {
    val schema = schemaService.getSchema(SchemaInfo(record.topic(), record.key()))
    val sectionInfo = fileNaming.getSectionInfo(record.topic(), System.currentTimeMillis())
    if (!dataSections.contains(sectionInfo._1)) {
      //Create section
      try {
        dataSectionsLock.lock()
        if (!dataSections.contains(sectionInfo._1)) {
          dataSections.put(sectionInfo._1, createDataSection(record.topic(), sectionInfo._1, sectionInfo._2))
        }
      } finally {
        dataSectionsLock.unlock()
      }
    }
    val section = dataSections(sectionInfo._1)
    section.write(schema, record)
  }
}
