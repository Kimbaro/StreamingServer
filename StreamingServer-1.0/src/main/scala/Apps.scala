
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.event.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, current_date, expr, from_json, log, udf}
import org.apache.spark.sql.streaming.Trigger
import modules.utils.{JDBCStream, UdpStream}
import configs.Manager
import org.apache.log4j.{Logger, PropertyConfigurator}

import java.io.File


class HelloWorld extends Actor with ActorLogging {
  override def receive: Receive = {
    case msg: String => log.debug(s"hi $msg")
  }
}

object Apps {
  var udp_socket: UdpStream = null;
  var jdbc_stream: JDBCStream = null;
  var udp_socket_switch: Boolean = true;
  var config: Manager = null;
  var message: String = "";
  val logger: Logger = Logger.getLogger(getClass.getName)


  def main(args: Array[String]): Unit = {
    Runtime.getRuntime().exec(Array("/bin/bash", "-c", "rm -r /app/chk-point-dir"))
    //빌드 설정
    config = new Manager();
    //println(s"하둡경로 ${config.HADOOP_HOME_DIR}");
    udp_socket = new UdpStream(config.UDP_HOST_PORT);
    jdbc_stream = new JDBCStream(config);

    val toStr = udf((payload: Array[Byte]) => new String(payload))
    val parsing = config.DATAFRAME_KAFKA.withColumn("value", toStr(config.DATAFRAME_KAFKA("value")))

    //UDP 호스트 실행
    udp_socket.startServer();

    //TODO 카프카 구독정보를 실시간으로 받아옴, Trigger.ProcessingTime로 interval 주기 설정
    //    println("KAFKA 리시버 생성");
    val kafka_receiver = parsing.writeStream
      .format("console")
      //.option("path", "src/main/scala")
      .option("checkpointLocation", "/app/chk-point-dir");
    logger.info("Kafka Sink Creation, Waiting for TOPIC publish")
    //    println("Kafka Sink Creation, Waiting for TOPIC issuance");
    kafka_receiver
      .trigger(Trigger.ProcessingTime(config.KAFKA_SUBSCRIBE_INTERVAL_MILLISECOND))
      .foreachBatch(myFunc _)
      .start()
      .awaitTermination();
  }

  //TODO scala 12 부터 foreachBatch 이슈에 대한 대응 함수
  //http://jason-heo.github.io/bigdata/2021/01/23/spark30-foreachbatch.html
  def myFunc(batch: DataFrame, batchId: Long): Unit = {
    try {
      message = batch.first().json; // <- kafka message
      //println(message);
      runningUDP_Pipline(batch, batchId, message);
      runningPSQL_Pipline(batch, batchId, message);
    } catch {
      case e1: NoSuchElementException => {
        logger.info(s"Kafka sync checkpoint does not exist. Create the resource directory. [create dir 'chk-point-dir']");
      }
      case e2: Exception => {
        e2.printStackTrace();
      }
    }
  }

  //subThread 1, UDP 통신 서버
  def runningUDP_Pipline(batch: DataFrame, batchId: Long, message: String): Unit = {

    if (udp_socket_switch) {
      udp_socket_switch = false
      logger.info("UDP host creation failed. Please try again." + udp_socket.getUdpThreadStatus());
      udp_socket.startServer();
    }else{
      //카프카 메시지 수신, 클라이언트에게 전달.
      udp_socket.data = message;
    }
  }

  //subThread 2, Postgresql insert 수행
  def runningPSQL_Pipline(batch: DataFrame, batchId: Long, message: String): Unit = {
    jdbc_stream.insertStream(message);
  }
}