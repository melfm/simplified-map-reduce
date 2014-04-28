package worker;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;

import com.google.protobuf.InvalidProtocolBufferException;

import common.WireMessages.ComputationMessage;

public class WorkReceiver implements Runnable{

	private Context context;
	private String master;
	
	public WorkReceiver(Context context, String master){
		this.context = context;
		this.master = master;
	}
	
	public void run() {
		// Socket to receive messages on
		ZMQ.Socket receiver = context.socket(ZMQ.PULL);
		receiver.connect("tcp://"+master+":5557");
		//TODO: parse computation message
		try {
			while(!Thread.interrupted()){
			//we receive the message but it has no properties to decode because
			//we haven't implemented ComputationMessage yet.
			ComputationMessage work = ComputationMessage.parseFrom(receiver.recv());
			System.out.println(work);
			int firstVal = work.getValue1();
			int secondVal = work.getValue2();
			int id = work.getId();
			//once you have decoded the message you probably want to call
			//a method from a another class to perform the computation
			int operation = work.getOperation();
			System.out.println("Operation id " + operation);
			int result = 0;
			switch(operation) {
				case 1 : result = peformProduct(firstVal, secondVal,result); break;
				case 2 : result = performSubtraction(firstVal, secondVal,result); break;
				case 3 : result = performSum(firstVal, secondVal,result); break;
				default : break;
			}
			//result = peformProduct(firstVal, secondVal,result);
			System.out.println("Result " + result);
			ResultSender sender = new ResultSender(context, master);
			sender.sendToSink(id,result);
			}
		} catch (InvalidProtocolBufferException e) {
			System.err.println("Unable to decode work message");
			e.printStackTrace();
		}
	}
	
	public int peformProduct(int val1, int val2, int result) {
		result = 1;
		return result = val1 * val2;
	}
	
	public int performSubtraction(int val1, int val2,int result) {
		return result = (val2 - val1);
	}
	
	public int performSum(int val1, int val2, int result) {
		return result = (val1+ val2);
	}

}
