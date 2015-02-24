package example

import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import kafka.server.{KafkaConfig, KafkaServerStartable}
import org.apache.curator.test.{InstanceSpec, TestingServer}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class SparkExampleSpec extends FunSuite with BeforeAndAfterEach {

  val cluster = new ClusterEmbedded()

  override beforeEach() {
    cluster.start()
  }

  override afterEach() {
    cluster.stop()
  }

  test("Should fail") {
    fail("Test is not finished yet")
  }
}

class ClusterEmbedded extends LazyLogging {

  private val zookeeper = new ZooKeeperEmbedded(InstanceSpec.getRandomPort)
  private val kafka = new KafkaEmbedded(new KafkaConfig(new Properties()))

  def start() {
    zookeeper.start()
    kafka.start()
  }

  def stop() {
    kafka.stop()
    zookeeper.stop()
  }
}

class ZooKeeperEmbedded(val port: Int = 2181) extends LazyLogging {

  private val autostart = false
  private val server = new TestingServer(port, autostart)

  def start() {
    logger.info(s"Start embedded ZooKeeper on port $port")
    server.start()
  }

  def stop() {
    logger.debug(s"Stop embedded ZooKeeper on port $port...")
    server.close()
  }
}

class KafkaEmbedded(config: KafkaConfig) extends LazyLogging {

  private val server = new KafkaServerStartable(config)

  def start() {
    logger.debug(s"Start embedded Kafka broker at $config.hostName:$config.port")
    server.startup()
  }

  def stop() {
    logger.debug(s"Stop embedded Kafka broker at $config.hostName:$config.port")
    server.shutdown()
  }
}
