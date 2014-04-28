package master;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;

import com.google.protobuf.InvalidProtocolBufferException;

import common.WireMessages.ResultMessage;

public class Sink implements Runnable{
	private Context context;
	private ArrayList<Integer> results = new ArrayList<Integer>();
	
	private int finalResult;
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
								// Job done,store result and remove from the list
								//finalResult = calculateFinalResult(result, comp.getOperation(),finalResult);
								System.out.println("Result " + result);
								results.add(result);
								workParts.remove(jobID);
								if(workParts.isEmpty()){
									finalResult = calculateFinalResult(results, finalResult,comp.getOperation());
									System.out.println("Give final result here");
									System.out.println(finalResult);
									
								}
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
	
	public int calculateFinalResult(ArrayList<Integer> results, int result, int operation){
		switch(operation) {
			case 1 : result = peformProduct(results, result); break;
			case 2 : result = performSubtraction(results, result); break;
			case 3 : result = performSum(results, result); break;
			default : break;
		}
		return result;
		
	}
	
	public int peformProduct(ArrayList<Integer> resultList, int result) {
		// Initialize to zero since performing product
		result = 1;
		for(int i=0; i <resultList.size(); i++){
			result *= resultList.get(i);
		}
		return result;
	}
	
	public int performSubtraction(ArrayList<Integer> resultList, int result) {
		result = 0;
		for(int i=0; i <resultList.size(); i++){
			result -= resultList.get(i);
		}
		return result;
	}
	
	public int performSum(ArrayList<Integer> resultList, int result) {
		result = 0;
		for(int i=0; i <resultList.size(); i++){
			result += resultList.get(i);
		}
		return result;
	}
	

}
