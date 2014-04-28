package master;
import java.util.ArrayList;
import java.util.List;

import org.zeromq.ZMQ;

import com.google.protobuf.InvalidProtocolBufferException;

import common.WireMessages.BootstrapMessage;
import common.WireMessages.ResponseMessage;

public class BootstrapServer implements Runnable{
	private ZMQ.Socket responder;
	private boolean running;
	private Integer workerCounter;
	
	List<String> workerIDs = new ArrayList<String>();

	public BootstrapServer(ZMQ.Context context){
		this.responder = context.socket(ZMQ.REP);
		this.running = true;
	}

	public void run(){
			responder.bind("tcp://*:5555");
			workerCounter = 1;
			while(running){

				byte[] encodedReponse = null;
				ResponseMessage.Builder response = ResponseMessage.newBuilder();
				
				//receive profobuf encoded message
				byte[] message = responder.recv(0);
				
				try {
					BootstrapMessage parsedMessage = BootstrapMessage.parseFrom(message);
					String worker = parsedMessage.getAddress();
					System.out.println("Bootstrapped " + worker);
					
					
					
					workerIDs.add(worker);
					
					//we're using http style response codes, 200 OK
					response.setStatus(200);
					//build reply to byte array to send
					encodedReponse = response.build().toByteArray();
				} catch (InvalidProtocolBufferException e) {
					System.err.println("Unable to parse bootstrap message");
					//we're using http style response codes, 400 failure
					response.setStatus(400);
					//build reply to byte array to send
					encodedReponse = response.build().toByteArray();
					e.printStackTrace();
				} finally{
					responder.send(encodedReponse);

				}	
			}

	}

	public void stop(){
		this.running = false;
		this.responder.close();
	}

	public static void main(String[] args){
 		ZMQ.Context context = ZMQ.context(1);
		Thread t = new Thread(new BootstrapServer(context));
		t.start();
	}

}