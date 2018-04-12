package Server;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

import MServer.Chunk;
import MServer.Message;
import serverRequestHandler.*;

public class Server {
	public ServerSocket server = null;
	Socket socket = null;
	public InputStream inputStream = null;
	public OutputStream outputStream = null;
	static public final int port = 15000;
	static File directory = new File("directory");
	public Map<String, TreeMap<Integer, Chunk>> fileDict;
	public HashSet<String> ChunkName;
	public final String metaServerAddress = "10.176.66.54"; 
	public final int metaServerPort = 16000;
	private static final Logger logger = Logger.getLogger(Server.class.getName());
	public static InetAddress ip;
	public CsvWriter csvWriter;
	public CsvReader csvReader;
	public Server() {
		fileDict = new HashMap<>();
		ChunkName = new HashSet<>();
		try {
			ip = InetAddress.getLocalHost();	
			executeHeartbeatSender();
			if (!new File("fileMap.csv").exists()) {
				csvWriter = new CsvWriter("fileMap.csv", ',', Charset.forName("UTF-8"));
				String[] csvHeaders = {"FileName", "Index", "Host_Server", "size" , "ChunkName"};
				csvWriter.writeRecord(csvHeaders);
				csvWriter.flush();
			} else {
				csvReader = new CsvReader("fileMap.csv", ',', Charset.forName("UTF-8"));
				csvReader.readHeaders();
				while (csvReader.readRecord()) {
					String FileName = csvReader.getValues()[0];
					String Index = csvReader.getValues()[1];
					String Host_Server = csvReader.getValues()[2];
					String size = csvReader.getValues()[3];
					String ChunkName = csvReader.getValues()[4];
					restartFileDict(FileName, Index, Host_Server, size, ChunkName);
				}
				csvWriter = new CsvWriter("fileMap.csv", ',', Charset.forName("UTF-8"));
				for (Map.Entry<String, TreeMap<Integer, Chunk>> en : fileDict.entrySet()) {
					TreeMap<Integer, Chunk> fileMap = en.getValue();
					for (Map.Entry<Integer, Chunk> e : fileMap.entrySet()) {
						String[] csvContent = {en.getKey(), e.getValue().index + "", e.getValue().host_Server.toString(), e.getValue().size + "", e.getValue().ChunkName};
						csvWriter.writeRecord(csvContent);
						csvWriter.flush();
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void startup() throws IOException {
		logger.info("Starting Service at port " + port);
		ServerSocket serverSocket = new ServerSocket(port);
		InputStream inputStream = null;
		OutputStream outputStream = null;

		if (!directory.exists()) {
			directory.mkdirs();
			System.out.println("Created Data Directory");
		}
		while (true) {
			try {
				logger.info("Waiting for request");
				// Block until a client connection 
				Socket clientSocket = serverSocket.accept();
				logger.info("Request received");
				ObjectInputStream objectInputStream = new ObjectInputStream(clientSocket.getInputStream());
				Message message = (Message) objectInputStream.readObject();
				outputStream = clientSocket.getOutputStream();
				ServerRequestHandler handler = dispatchRequest(message);
				logger.info("Processing Request: " + handler);
				handler.setInputStream(inputStream);
				handler.setOutputStream(outputStream);
				handler.setServer(this);
				handler.run();
			} catch (Exception ex) {
				logger.info("Exception while processing request.");
				ex.printStackTrace();
			}
		}
	}

	public void restartFileDict(String FileName, String Index, String Host_Server, String size, String ChunkName) throws NumberFormatException, UnknownHostException {
		if (!this.fileDict.containsKey(FileName)) {
			this.fileDict.put(FileName, new TreeMap<Integer, Chunk>());
		}
		TreeMap<Integer, Chunk> treeMap = fileDict.get(FileName);
		if (treeMap.containsKey(Integer.parseInt(Index))) {
			if (treeMap.get(Integer.parseInt(Index)).size < Integer.parseInt(size)) {
				Chunk chunk = new Chunk(Integer.parseInt(Index), FileName, InetAddress.getByName(Host_Server.substring(1)), Integer.parseInt(size));
				chunk.ChunkName = ChunkName;
				this.ChunkName.add(ChunkName);
				treeMap.put(Integer.parseInt(Index), chunk);
			}
		} else {
			Chunk chunk = new Chunk(Integer.parseInt(Index), FileName, InetAddress.getByName(Host_Server.substring(1)), Integer.parseInt(size));
			chunk.ChunkName = ChunkName;
			this.ChunkName.add(ChunkName);
			treeMap.put(Integer.parseInt(Index), chunk);
		}

	}
	public void executeHeartbeatSender() {
		Thread listener = new Thread(new HeartbeatSender(this));
		listener.start();
	}

	private ServerRequestHandler dispatchRequest(Message message) throws Exception {
		// Get the request and assign corresponding handler
		if ("create".equalsIgnoreCase(message.getMessage())) {
			ServerRequestHandler handler = new CreateRequestHandler(message);
			return handler;
		} else if ("read".equalsIgnoreCase(message.getMessage())) {
			ServerRequestHandler handler = new ReadRequestHandler(message);
			return handler;
		} else if ("append".equalsIgnoreCase(message.getMessage())) {
			ServerRequestHandler handler = new AppendRequestHandler(message);
			return handler;
		} else {
			throw new Exception("Unknown Request: " + message.getMessage());
		}
	}

	public static void main(String args[]) {
		// Start the server
		Server server = new Server();
		try {
			server.startup();
		}
		catch (IOException ex) {
			logger.info("Unable to start server. " + ex.getMessage());
			ex.printStackTrace();
		}
	}
}
