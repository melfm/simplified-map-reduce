package worker;
import org.zeromq.ZMQ;
import com.google.protobuf.InvalidProtocolBufferException;
import common.WireMessages.BootstrapMessage;
import common.WireMessages.ResponseMessage;

 

public class BootstrapClient{
	private ZMQ.Context context;
	private String master;
	private String address;

	public BootstrapClient(ZMQ.Context context, String master, String address){
		this.context = context;
		this.master = master;
		this.address = address;

	}
	/**
	 * this method connects to the master and informs it that it is available
	 * for work
	 * @return boolean with the result of the bootstrap
	 */
	public boolean bootstrap(){
 		ZMQ.Socket requester = context.socket(ZMQ.REQ);
 		
 		//create builder to produce a protobuf message
 		BootstrapMessage.Builder message = BootstrapMessage.newBuilder();
 		//set required fields
 		message.setAddress(address);
 		//build message to byte array to send 
 		byte[] request = message.build().toByteArray();
 		
		requester.connect("tcp://"+master+":5555");
		requester.send(request);
		//set timeout to wait for a reply
		requester.setReceiveTimeOut(2000);

		//receive reply from server
		byte[] reply = requester.recv();
		ResponseMessage parsedReply;
		try {
			parsedReply = ResponseMessage.parseFrom(reply);
			
			if(parsedReply.getStatus() == 200){
				return true;
			}
		} catch (Exception e) {
			System.err.println("Unable to bootstrap");
		}

		
		return false;

	}

	public static void main(String[] args){
 		ZMQ.Context context = ZMQ.context(1);
 		BootstrapClient foo = new BootstrapClient(context, "127.0.0.1", "127.0.0.1");
		foo.bootstrap();
	}

}