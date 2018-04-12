package serverRequestHandler;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.util.Random;
import java.util.logging.Logger;
import MServer.Chunk;
import MServer.Message;


public class ReadRequestHandler extends ServerRequestHandler{
	public ReadRequestHandler(Message message) {
		super(message);
	}
	private static Logger logger = Logger.getLogger(ReadRequestHandler.class.getName());
	public void run() throws IOException {
		// Read the message
		String fileName = message.getFileName();
		int indexOfFile = message.getIndexOfFile();
		Chunk toReadChunk = Server.fileDict.get(fileName).get(indexOfFile);
		File file = new File("directory/"  + toReadChunk.ChunkName);
		if (file.length() != 0) {
			int offset = new Random().nextInt((int)file.length());
			logger.info("file's length is " + file.length());
			logger.info("File's name is " + fileName + ". indexOfFile to read is " + indexOfFile + ". offset to read is " + offset);
			RandomAccessFile raf = new RandomAccessFile(file, "r");
			raf.seek(offset); // Goes to the offset position.
	        byte[] b = new byte[(int) (file.length() - offset)];
	        raf.readFully(b);
	        String content = new String(b);
			// Write response to the outputStream
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
			logger.info("Sending back information to the client");
			objectOutputStream.writeObject(new Message("OK"));
			outputStream.flush();
			objectOutputStream.writeObject(new Message(content));
			outputStream.flush();
			logger.info("Finished reading chunk");	
		} else {
			// Write response to the outputStream
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
			logger.info("Sending back information to the client");
			objectOutputStream.writeObject(new Message("There is no content in the file you want to read!"));
			outputStream.flush();
			logger.info("Finished reading chunk");	
		}

	}
}
