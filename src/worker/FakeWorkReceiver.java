package worker;

import java.util.Random;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;

import com.google.protobuf.InvalidProtocolBufferException;

import common.WireMessages.ComputationMessage;

public class FakeWorkReceiver implements Runnable{

	private Context context;
	private String master;
	
	public FakeWorkReceiver(Context context, String master){
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
			int id = work.getId();
			//once you have decoded the message you probably want to call
			//a method from a another class to perform the computation
			int result = peformProduct();
			System.out.println("Computation result " + result);
			ResultSender sender = new ResultSender(context, master);
			sender.sendToSink(id,result);
			}
		} catch (InvalidProtocolBufferException e) {
			System.err.println("Unable to decode work message");
			e.printStackTrace();
		}
	}
	
	public int peformProduct() {
		Random rand = new Random();
		int max = 100;
		int min = 1;
		int randomNum = rand.nextInt((max - min) + 1) + min;
		return randomNum;
	}
}
