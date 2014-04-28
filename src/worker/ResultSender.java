package worker;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;

import common.WireMessages.ResultMessage;

public class ResultSender {
	private Context context;
	private String master;
	
	public ResultSender(Context context, String master){
		this.context = context;
		this.master = master;
	}
	
	public void sendToSink(int id,int result){
		ZMQ.Socket sender = context.socket(ZMQ.PUSH);
		sender.connect("tcp://"+master+":5558");
		
		ResultMessage.Builder message = ResultMessage.newBuilder();
		message.setResult(result);
		message.setId(id);
		sender.send(message.build().toByteArray());
		sender.close();
	}
}
