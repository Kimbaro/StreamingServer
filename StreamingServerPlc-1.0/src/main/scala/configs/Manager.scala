package configs

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class Manager {
  /** ----------------------------------- */

  /** --------------logging-------------- */
  Logger.getLogger("org").setLevel(Level.OFF)
  /*  Logger.getLogger("akka").setLevel(Level.OFF)*/
  val logger: Logger = Logger.getLogger(getClass.getName)
  logger.info(s"Spark Dashboard listening to 4040");

  /** ------------------------------------------ */
  /** --------------Basic Setting -------------- */
  /** ------------------------------------------ */
  val conf_app = ConfigFactory.load("app.properties");
  val conf_kafka = ConfigFactory.load("kafka.properties");
  val conf_psql = ConfigFactory.load("postgres.properties");

//  val APP_VERSION: String = conf_app.getString("APP.VERSION")
  val APP_HOST_IP: String = conf_app.getString("APP.HOST.IP")
  val UDP_HOST_PORT: Int = conf_app.getInt("UDP.HOST.PORT");
  val HADOOP_HOME_DIR: String = conf_app.getString("HADOOP.HOME.DIR");
  val PSQL_INTERVAL: String = conf_app.getString("PSQL.INTERVAL");


  val KAFKA_HOST_IP: String = conf_kafka.getString("KAFKA.HOST.IP");
  val KAFKA_HOST_PORT: String = conf_kafka.getString("KAFKA.HOST.PORT");
  val KAFKA_SUBSCRIBE_TOPICS: String = conf_kafka.getString("KAFKA.SUBSCRIBE.TOPICS");
  val KAFKA_SUBSCRIBE_INTERVAL_MILLISECOND: Int = conf_kafka.getInt("KAFKA.SUBSCRIBE.INTERVAL.MILLISECOND");

  val PSQL_HOST_IP: String = conf_psql.getString("PSQL.HOST.IP");
  val PSQL_HOST_PORT: String = conf_psql.getString("PSQL.HOST.PORT");
  val PSQL_DB_NAME: String = conf_psql.getString("PSQL.DB.NAME");
  val PSQL_TABLE_NAME: String = conf_psql.getString("PSQL.TABLE.NAME");
  val PSQL_USER_ID: String = conf_psql.getString("PSQL.USER.ID");
  val PSQL_USER_PW: String = conf_psql.getString("PSQL.USER.PW");


  /** ------------------------------------------ */
  /** --------------Spark Sessions-------------- */
  /** ------------------------------------------ */

  // @TODO 스파크 세션 사양은 콘솔에서 실행하는것으로 한다. (동적변환을위해서)
  val SESSION_KAFKA: SparkSession = SparkSession.builder()
    .appName("SESSION_KAFKA")
    .config("hadoop.home.dir", HADOOP_HOME_DIR)
    .getOrCreate()
  SESSION_KAFKA.sparkContext.setLogLevel("INFO");
  //spark-submit --class Apps --master local[*] --executor-memory 2G --total-executor-cores 1 --driver-memory 2G streaming-1.7-SNAPSHOT-jar-with-dependencies.jar

  val SESSION_PSQL: SparkSession = SparkSession.builder()
    .appName("SESSION_PSQL")
    .master("local[2]")
    .getOrCreate();
  SESSION_PSQL.sparkContext.setLogLevel("INFO")

  /** -------------------------------------------- */
  /** --------------Spark DataFrames-------------- */
  /** -------------------------------------------- */
  val DATAFRAME_KAFKA: DataFrame = SESSION_KAFKA.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", s"${KAFKA_HOST_IP}:${KAFKA_HOST_PORT}")
    .option("subscribe", s"${KAFKA_SUBSCRIBE_TOPICS}")
    .option("stopGracefullyOnShutdown", "true")
    .load()

  val DATAFRAME_PSQL: DataFrame = SESSION_PSQL.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", s"jdbc:postgresql://${PSQL_HOST_IP}:${PSQL_HOST_PORT}/${PSQL_DB_NAME}")
    .option("dbtable", s"${PSQL_TABLE_NAME}")
    .option("user", s"${PSQL_USER_ID}")
    .option("password", s"${PSQL_USER_PW}")
    .load()

  import SESSION_KAFKA.implicits._
  import SESSION_PSQL.implicits._

}
