package modules.utils

import org.apache.log4j.Logger

import java.net.{DatagramPacket, DatagramSocket, InetAddress, InetSocketAddress, Socket}
import org.apache.spark.sql.functions.udf

/**
 * @param port - 소켓 호스트 포트.
 * @var running         - UDP listener 동작 여부, default = true
 * @var buf             - 클라이언트가 보낸 바이트 형식의 데이터 버퍼
 * @var data            - 클라이언트가 보낸 바이트 정보를 문자변환 후 적재될 공간
 * @var kafka_data      - kafka로 부터 받은 payload를 적재함
 * @var thread          - UDP Socket 동작을 위한 sub-thread
 * @var address         - 호스트에 접속한 클라이언트의 IP address
 * @var client_port     - 호스트에 접속한 클라이언트의 Port
 * @var receive_packet  - 클라이언트가 보낸 패킷 덩어리
 */
class UdpStream(port: Int = 8866) {
  var running: Boolean = true;
  private var buf: Array[Byte] = new Array[Byte](3)
  var data: String = null;
  var kafka_data: String = null;
  var thread: Thread = null;
  var address: InetAddress = null;
  var client_port: Int = 0;
  var receive_packet: DatagramPacket = null;
  var socket: DatagramSocket = null;
  val logger: Logger = Logger.getLogger(getClass.getName)

  def startServer(): Unit = {
    thread = new Thread(new Runnable {
      override def run(): Unit = {
        val toStr = udf((payload: Array[Byte]) => new String(payload));
        socket = new DatagramSocket(port);
        logger.info(s"UDP server host created PORT=${port}")
        while (running) {
          receive_packet = new DatagramPacket(buf, buf.length);
          socket.receive(receive_packet);

          //클라이언트 헤더/패킷
          address = receive_packet.getAddress
          client_port = receive_packet.getPort;
          data = new String(receive_packet.getData).trim();
          //보낼 헤더/패킷 작성
          val send_packet: DatagramPacket = new DatagramPacket(kafka_data.getBytes, kafka_data.getBytes.length, address, client_port); //보낼 데이터

          try {
            //reactor pattern ...
            if (data == "END") {
              running = false;
            } else if (data == "REQ") {
              try {
                socket.send(send_packet);
                logger.info(s"send udp packet-> ${kafka_data}")
              } catch {
                case e: Exception => logger.error(s"sending fail");
              }
            }
            /*          logger.info(s"access info : ${address}, ${client_port} sending data : ${kafka_data}")
                        socket.send(send_packet);*/
          } catch {
            case e: Exception => logger.info(s"Failed to send");
          }
        }
        socket.close();
      }
    });
    if (thread.getState == java.lang.Thread.State.NEW) {
      thread.start();
    }
  }

  def getUdpThreadStatus(): Thread.State = {
    return thread.getState;
  }
}