package serverRequestHandler;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.util.logging.Logger;
import MServer.Chunk;
import MServer.Message;

public class AppendRequestHandler extends ServerRequestHandler{
	public AppendRequestHandler(Message message) {
		super(message);
	}

	private static Logger logger = Logger.getLogger(AppendRequestHandler.class.getName());

	public void run() throws IOException { 
		// Read the message
		String fileName = message.getFileName();
		String toAppendContent = message.getToAppendContent();
		logger.info("File's name is " + fileName + ". Content to append is " + toAppendContent);
		Chunk toAppendChunk = Server.fileDict.get(fileName).lastEntry().getValue();
		toAppendChunk.size += toAppendContent.getBytes().length;
		File file = new File("directory/"  + toAppendChunk.ChunkName);
		RandomAccessFile raf = new RandomAccessFile(file, "rw");
		raf.seek(file.length());
		raf.write(toAppendContent.getBytes());
		// Write modification to csv
		String[] csvContent = {fileName, toAppendChunk.index + "", toAppendChunk.host_Server.toString(), toAppendChunk.size + "", toAppendChunk.ChunkName};
		Server.csvWriter.writeRecord(csvContent);
		Server.csvWriter.flush();
		// Write response to the outputStream
		ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
		logger.info("Sending OK to the client");
		objectOutputStream.writeObject(new Message("OK"));
		outputStream.flush();
		logger.info("Finished appending chunk");	
		
		
	}
}
