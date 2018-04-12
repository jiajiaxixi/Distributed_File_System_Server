package Server;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.logging.Logger;

import MServer.Message;

public class HeartbeatSender implements Runnable {
	Server Server;
	private final long timeInterval = 5000;
	private static final Logger logger = Logger.getLogger(HeartbeatSender.class.getName());
	public HeartbeatSender(Server Server) {
		this.Server = Server;
	}
	public void run() {
		try {
			while (true) {
				Socket socket = new Socket();
				SocketAddress addr = new InetSocketAddress(Server.metaServerAddress, Server.metaServerPort);
				socket.connect(addr);
				OutputStream outputStream = socket.getOutputStream();
				ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
//				logger.info("Sending 'Heartbeat' message to the metaServer");
				objectOutputStream.writeObject(new Message("heartbeat", Server.fileDict, Server.ip));
				outputStream.flush();
				Thread.sleep(timeInterval);		
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
