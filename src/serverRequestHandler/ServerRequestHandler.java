package serverRequestHandler;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import Server.*;
import MServer.*;

public abstract class ServerRequestHandler {
		protected InputStream inputStream;
		protected OutputStream outputStream;
		protected Message message;
		protected Server Server;
		
		abstract public void run() throws IOException;
		public ServerRequestHandler(Message message) {
			this.setMessage(message);
		}
		public void setInputStream(InputStream inputStream) {
			this.inputStream = inputStream;
		}

		public void setOutputStream(OutputStream outputStream) {
			this.outputStream = outputStream;
		}

		public Message getMessage() {
			return message;
		}
		public void setMessage(Message message) {
			this.message = message;
		}
		public void setServer(Server Server) {
			this.Server = Server;
		}
		
 
}
