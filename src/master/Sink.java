package master;

import java.util.Collections;
import java.util.HashMap;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;

import com.google.protobuf.InvalidProtocolBufferException;

import common.WireMessages.ComputationMessage;
import common.WireMessages.ResultMessage;

public class Sink implements Runnable{
	private Context context;
	//private ArrayList<Integer> results;
	private int finalProduct = 1;
	private HashMap<Integer, Computation> workParts;
	
	public Sink(Context context, HashMap<Integer, Computation> workParts){
		this.workParts = workParts;
		this.context = context;
	}
	
	public void run(){
		ZMQ.Socket receiver = context.socket(ZMQ.PULL);
		receiver.bind("tcp://*:5558");
		try {
			while (!Thread.interrupted()) {
				// Read a message from the queue
				ResultMessage message = ResultMessage.parseFrom(receiver.recv());
				// Extract message fields
				int result = message.getResult();
				int jobID = message.getId();
				// Lookup job in job list
				// Synchronize access to job list prior to accessing it
				synchronized (Master.lock) {
					if (workParts.containsKey(jobID)) {
						// Get job
						Computation comp = workParts.get(jobID);
						System.out.println(comp.getFirstVal() + " " + comp.getSecondVal());					// ----
						// Add result to result list of the job
						comp.getResultList().add(result);
						// Check if all the results have been received
						if (comp.getResultList().size() == comp.getNumOfSubmissions()) {
							// Check if all computations have the same result
							if (Collections.frequency(comp.getResultList(), result) !=  comp.getNumOfSubmissions()){
								// Results do not tally
								System.out.println("Inconsistent");
								// So erase result list and resubmit
								comp.getResultList().clear();
								comp.setSubmitted(false);
							} else {
								// Job done remove from the list
								finalProduct *= result;
								System.out.println("Final Res " + finalProduct);
								workParts.remove(jobID);
							}
						}
					}
				}
			}
		} catch (InvalidProtocolBufferException e) {
			System.err.println("Unable to decode result message");
			e.printStackTrace();
		}
		
	}
	
	public void resubmitJob(ZMQ.Socket receiver,Context context, Computation job, int jobID){
		// Don't wait for workers since resubmitting,
		// assume there are some workers out there
		ComputationMessage.Builder message = ComputationMessage.newBuilder();
		message.setValue1(job.getFirstVal());
		message.setValue2(job.getSecondVal());
		message.setId(jobID);
		//currently this message type has no properties
		//so we just build it and send an empty message
		receiver.send(message.build().toByteArray());
		
	}
	

}
