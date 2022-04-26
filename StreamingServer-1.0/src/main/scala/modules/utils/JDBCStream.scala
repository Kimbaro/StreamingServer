package modules.utils

import configs.Manager
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.{Date, Properties, Timer}
import org.json4s.jackson.Serialization

import scala.util.parsing.json._
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import scala.collection.mutable

class JDBCStream(manager: Manager) extends Thread {

  import manager.SESSION_PSQL.sqlContext.implicits._

  implicit val formats = org.json4s.DefaultFormats
  var config: Manager = new Manager();
  var thread: Thread = null;
  var queue: mutable.Queue[String] = new mutable.Queue[String]();
  val logger: Logger = Logger.getLogger(getClass.getName)

  // @TODO 백그라운드 실행함수
  def insertWait(): Unit = {
    val service: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor
    service.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        try {
          val connectionProperties = new Properties()
          connectionProperties.put("user", s"${config.PSQL_USER_ID}")
          connectionProperties.put("password", s"${config.PSQL_USER_PW}")
          connectionProperties.put("driver", "org.postgresql.Driver")

          logger.info(s"xxxxxxxxx insert start    xxxxxxxxx");
          logger.info(s"queue size -> ${queue.size}");
          while (!queue.isEmpty) {
            var json = JSON.parseFull(queue.dequeue())
            //json 데이터 중 key값이 value인 것만
            var value: String = null;
            json match {
              case Some(m: Map[String, Any]) => m("value") match {
                case s: String => value = s;
              }
            }

            json = JSON.parseFull(value)
            //logger.debug("message : " + json)
            //반정형화상태
            var data_semiStructuredData: Option[String] = null;
            var receivedtime_semiStructuredData: Option[String] = null;
            //정형화상태
            var data_structuredData: String = null;
            var receivedtime_structuredData: String = null;

            json match {
              case Some(e) => val res = e.asInstanceOf[Map[String, String]]; //Map으로 형변환
                data_semiStructuredData = res.get("data");
                receivedtime_semiStructuredData = res.get("receivedtime");
            }

            data_structuredData = Serialization.write(data_semiStructuredData);
            receivedtime_structuredData = Serialization.write(receivedtime_semiStructuredData);

            //-64만 들어오는 경우 insert 제외 connectorHub에서 보내고 있음
            if ("{'rssi':-64.0}".length != data_structuredData.length) {
              //psql에 쌓을 실데이터
              json = JSON.parseFull(data_structuredData)
              json match {
                case Some(e) => val res = e.asInstanceOf[Map[String, String]]; //Map으로 형변환
                  val allKeys = res.keySet
                  for (key <- allKeys) {
                    if (!key.equals("ip")) {
                      List((key, Serialization.write(res.get(key)), receivedtime_structuredData)).toDF("mac", "value", "receivedtime")
                        .write.mode(SaveMode.Append)
                        .jdbc(s"jdbc:postgresql://${config.PSQL_HOST_IP}:${config.PSQL_HOST_PORT}/${config.PSQL_DB_NAME}", s"${config.PSQL_TABLE_NAME}", connectionProperties)
                    }
                  }
              }
              //패키지 다시 말아서 반영후 확인해보자
              //val newDataFrame = List((data_structuredData, receivedtime_structuredData)).toDF("value", "receivedtime");
              //newDataFrame.write.mode(SaveMode.Append)
              //  .jdbc(s"jdbc:postgresql://${config.PSQL_HOST_IP}:${config.PSQL_HOST_PORT}/${config.PSQL_DB_NAME}", s"${config.PSQL_TABLE_NAME}", connectionProperties)
              //logger.info("ok-> " + newDataFrame.first())
            }
          }
        } catch {
          case e: Exception => e.printStackTrace();
        } finally {
          logger.info(s"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
          Thread.currentThread().interrupt()
        }
      }
    }, 0, s"${config.PSQL_INTERVAL}".toInt, TimeUnit.MILLISECONDS);
  }

  insertWait();

  def insertStream(message: String): Unit = {
    queue.enqueue(message);
  }
}
