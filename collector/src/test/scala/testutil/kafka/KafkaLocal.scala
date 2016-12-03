package testutil.kafka

import java.io.File
import java.util.Properties

import kafka.admin.AdminUtils
import kafka.server.KafkaServerStartable
import kafka.utils.ZkUtils
import org.apache.commons.io
import org.apache.kafka.common.security.JaasUtils
import org.apache.zookeeper.server.quorum.QuorumPeerConfig
import org.apache.zookeeper.server.{ServerConfig, ZooKeeperServerMain}
import org.slf4j.LoggerFactory

class KafkaLocal(kafkaProperties: Properties, zkProperties: Properties) {

  private val logger = LoggerFactory.getLogger(getClass)

  val kafka: KafkaServerStartable = KafkaServerStartable.fromProps(kafkaProperties)

  val zookeeper: ZooKeeperLocal = new ZooKeeperLocal(zkProperties)

  def bootstrapServer: String = s"${kafka.serverConfig.advertisedHostName}:${kafka.serverConfig.advertisedPort}"

  lazy val zkUtils = ZkUtils(zookeeper.getUrl, 30000, 30000, JaasUtils.isZkSecurityEnabled)

  def createTopic(name: String, partitions: Int = 1, replicas: Int = 1) : Unit =
    AdminUtils.createTopic(zkUtils, name, partitions, replicas)

  def start(): Unit = {
    zookeeper.start()
    Thread.sleep(5000)
    kafka.serverConfig.logDirs.foreach(f => {
      io.FileUtils.deleteDirectory(new File(f))
    })
    kafka.startup()
  }

  def stop(): Unit = {
    kafka.shutdown()
    zookeeper.stop()
  }
}

class ZooKeeperLocal(zkProperties: Properties) {
  private val logger = LoggerFactory.getLogger(getClass)

  val quorumConfiguration = new QuorumPeerConfig()
  quorumConfiguration.parseProperties(zkProperties)

  val zooKeeperServer: ZooKeeperServerMain = new ZooKeeperServerMain
  val configuration = new ServerConfig
  configuration.readFrom(quorumConfiguration)

  private val thread = new Thread() {
    override def run(): Unit = {
      //Delete all data when start
      io.FileUtils.deleteDirectory(new File(configuration.getDataDir))
      zooKeeperServer.runFromConfig(configuration)
    }
  }

  def getUrl = s"${configuration.getClientPortAddress.getHostString}:${configuration.getClientPortAddress.getPort}"

  def start(): Unit = thread.start()

  def stop(): Unit = thread.interrupt()
}