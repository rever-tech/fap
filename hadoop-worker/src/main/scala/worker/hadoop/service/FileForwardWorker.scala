package worker.hadoop.service

import java.util
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import com.twitter.inject.Logging
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import org.apache.hadoop.fs.Path
import worker.hadoop.file.DataSection
import worker.hadoop.util.{FileSystemConfig, ResourceControl}

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
  *
  */
class FileForwardWorker(conf: Config) extends Thread with Logging {

  infoResult("[FileForwardWorker] init with these config: \n%s") {
    conf.root().render(ConfigRenderOptions.concise())
  }

  private final val topicAndFiles: TrieMap[String, SeqQueue[(DataSection, Try[Unit] => Unit)]] = TrieMap.empty

  val srcFSConf: FileSystemConfig = FileSystemConfig(
    if (conf.hasPath("srcFS")) conf.getConfig("srcFS") else ConfigFactory.parseString("{}"))

  val destFSConf: FileSystemConfig = FileSystemConfig(
    if (conf.hasPath("destFS")) conf.getConfig("destFS") else ConfigFactory.parseString("{}"))

  private final val bufferSize = conf.getBytes("fs.buffersize").intValue()

  implicit final val executor: ExecutionContext = ExecutionContext.fromExecutor(
    Executors.newFixedThreadPool(
      if (conf.hasPath("number_worker")) conf.getInt("number_worker") else 3))

  final val isRunning: AtomicBoolean = new AtomicBoolean(false)

  def commitSucceed(name: String): Unit = topicAndFiles.get(name) match {
    case Some(queue) => queue.commitProcessSucceed()
    case None =>
  }

  def commitFailure(name: String): Unit = topicAndFiles.get(name) match {
    case Some(queue) => queue.commitProcessFailure()
    case None =>
  }

  def schedule(t: DataSection)(f: Try[Unit] => Unit): Unit = {
    if (!topicAndFiles.contains(t.topicName)) {
      synchronized {
        if (!topicAndFiles.contains(t.topicName)) {
          val queue = new SeqQueue[(DataSection, Try[Unit] => Unit)]()
          queue.addTail((t, f))
          topicAndFiles.put(t.topicName, queue)
        }
      }
    } else {
      topicAndFiles(t.topicName).addTail((t, f))
    }
  }

  override def run(): Unit = {
    while (isRunning.get()) {
      topicAndFiles.foreach(e => {
        e._2.getHead match {
          case Some(file) => Future {
            forward(srcFSConf, destFSConf, file._1)
          }.onComplete(file._2)
          case None =>
        }
      })
      Thread.sleep(100)
    }
  }

  def stopSafe(): Unit = {
    isRunning.set(false)
    //TODO: Wait all forwarder finished
  }

  override def start(): Unit = {
    isRunning.set(true)
    super.start()
  }

  def forward(srcFSConf: FileSystemConfig, destFSConf: FileSystemConfig, dataSection: DataSection) {
    dataSection.ensureClose()

    dataSection.finishedFiles.foreach(file => {
      val tmpFile = new Path(destFSConf.uri, file.name + ".tmp")
      val inputFile = new Path(file.fullPath)
      val destFs = tmpFile.getFileSystem(destFSConf.conf)
      val srcFs = inputFile.getFileSystem(srcFSConf.conf)
      try {
        ResourceControl.using(srcFs.open(inputFile)) {
          reader => {
            var buffer: Array[Byte] = null
            ResourceControl.using(destFs.create(tmpFile)) {
              writer => {
                buffer = new Array[Byte](bufferSize)
                var readBytes: Int = reader.read(buffer)
                while (readBytes != -1) {
                  writer.write(buffer, 0, readBytes)
                  readBytes = reader.read(buffer)
                }
              }
            }
          }
        }
        destFs.rename(tmpFile, new Path(destFSConf.uri, file.name))
      } finally {
        if (destFs.exists(tmpFile))
          destFs.delete(tmpFile, true)
      }
    })
  }
}

class SeqQueue[E]() {

  private val lock = new ReentrantLock()

  private val isProcessing: AtomicBoolean = new AtomicBoolean(false)
  private val elements: util.ArrayList[E] = new util.ArrayList[E]()

  def size: Int = elements.size()

  def commitProcessSucceed(): Unit = {
    lock.lock()
    try {
      if (elements.isEmpty) {
        //TODO: Warning here
      } else {
        elements.remove(0)
      }
      isProcessing.set(false)
    } finally {
      lock.unlock()
    }
  }

  def commitProcessFailure(): Unit = {
    isProcessing.set(false)
  }

  def addTail(item: E): Boolean = {
    if (item == null) throw new NullPointerException()
    lock.lock()
    try {
      elements.add(item)
    } finally {
      lock.unlock()
    }
  }

  /**
    *
    * @return
    */
  def getHead: Option[E] = {
    lock.lock()
    try {
      if (!isProcessing.get()) {
        if (elements.isEmpty) None else {
          val tmp = Some(elements.get(0))
          isProcessing.set(true)
          tmp
        }
      } else {
        None
      }
    } finally {
      lock.unlock()
    }
  }
}

