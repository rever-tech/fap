package collector.service

import java.util.Properties
import java.util.concurrent.Executors

import cakesolutions.kafka.KafkaConsumer.Conf
import collector.domain.AnalyticRequest
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{IntegerDeserializer, StringDeserializer}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import testutil.kafka.KafkaLocal

import scala.collection.JavaConversions._
import scala.collection.Seq
import scala.concurrent.ExecutionContext

/**
  * Created by tiennt4 on 01/12/2016.
  */
class KafkaConsumerTest extends FunSuite with BeforeAndAfterAll {

  val kafkaConf = new Properties()
  kafkaConf.load(getClass.getResourceAsStream("/kafka_local.properties"))
  val zkConf = new Properties()
  zkConf.load(getClass.getResourceAsStream("/zklocal.properties"))
  val kafka = new KafkaLocal(kafkaConf, zkConf)
  kafka.start()

  lazy val consumer = cakesolutions.kafka.KafkaConsumer(
    Conf(new IntegerDeserializer, new StringDeserializer,
      groupId = "test", bootstrapServers = kafka.bootstrapServer))

  private val pool = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))

  test("KafkaConsumer should send and receive same message in a limit time") {
    val topicName = s"test_${System.currentTimeMillis()}"
    kafka.createTopic(topicName)
    consumer.subscribe(Seq(topicName))
    //First poll
    consumer.poll(100)
    Thread.sleep(1000)
    val timestamp = System.currentTimeMillis()
    pool.execute(new KafkaConsumer(AnalyticRequest(topicName, 1, "", timestamp)))
    consumer.seek(new TopicPartition(topicName, 0), 0)
    val records = consumer.poll(1000)
    assertResult(1)(records.count())
    val record = records.head
    assertResult(record.key())(1)
    assertResult(record.value())("")
    assertResult(record.timestamp())(timestamp)
    assertResult(record.topic())(topicName)
  }
}
