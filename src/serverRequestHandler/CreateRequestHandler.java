package serverRequestHandler;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Random;
import java.util.TreeMap;
import java.util.logging.Logger;

import MServer.Chunk;
import MServer.Message;


public class CreateRequestHandler extends ServerRequestHandler{
	public CreateRequestHandler(Message message) {
		super(message);
	}
	private static Logger logger = Logger.getLogger(CreateRequestHandler.class.getName());
	public void run() throws IOException{	
		// Retrieve the information of chunk from the message
		Chunk toCreateChunk = message.getChunk();
		String fileName = toCreateChunk.name;
		String ChunkName = null;
		while (true) {
			ChunkName = String.valueOf((char)('A'+ new Random().nextInt(26))) + String.valueOf((char)('A' + new Random().nextInt(26)));
			if (!Server.ChunkName.contains(ChunkName)) {
				toCreateChunk.ChunkName = ChunkName;
				break;
			}
		}
		if (Server.fileDict.containsKey(toCreateChunk.name)) {
			Server.fileDict.get(toCreateChunk.name).put(toCreateChunk.index, toCreateChunk);
		} else {
			TreeMap<Integer, Chunk> newTreeMap = new TreeMap<>();
			newTreeMap.put(toCreateChunk.index, toCreateChunk);
			Server.fileDict.put(toCreateChunk.name, newTreeMap);
		}
		logger.info("File's name is " + fileName);
		logger.info("Chunk's name is " + ChunkName);
		File file = new File("directory/"  + ChunkName);
		Boolean create = file.createNewFile();
		logger.info("Create the file successfully? " + create);
		//write csv file
		String[] csvContent = {fileName, toCreateChunk.index + "", toCreateChunk.host_Server.toString(), toCreateChunk.size + "", toCreateChunk.ChunkName};
		Server.csvWriter.writeRecord(csvContent);
		Server.csvWriter.flush();
		ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
		logger.info("Sending 'OK' message to the meta server");
		objectOutputStream.writeObject(new Message("OK"));
		outputStream.flush();
		logger.info("Finished creating chunk");			
	}
}
