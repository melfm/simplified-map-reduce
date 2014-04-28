package master;

import java.io.IOException;
import java.util.HashMap;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;

import common.WireMessages.ComputationMessage;

public class Ventilator implements Runnable{
	private Context context;
	private HashMap<Integer, Computation> workParts;

	public Ventilator(ZMQ.Context context, HashMap<Integer, Computation> workParts){
		this.context = context;
		this.workParts = workParts;
	}

	
	/**
	 * This thread distributes work amongst the connected workers
	 * you'll need to implement the Computation class or replace
	 * it with something else and complete the ComputationMessage
	 * and the builder to make this method actually send out work
	 */
	public void run() {
		ZMQ.Socket sender = context.socket(ZMQ.PUSH);
		sender.bind("tcp://*:5557");
		
		//this lets you go to another machine to start a worker
		System.out.println("Press Enter when the workers are ready: ");
		try {
			System.in.read();
		} catch (IOException e) {
			e.printStackTrace();
		}
		// Messaging sending loop
		System.out.println("Sending tasks to workers\n");
		while (!Thread.interrupted()) {
			try {
				// If there are jobs to be sent, send them
				// Synchronize access to workParts structure before accessing it
				synchronized (Master.lock) {
					if (!workParts.isEmpty()) {	
						for (Computation comp : workParts.values()) {
							// Submit only unsubmitted jobs
							if (!comp.isSubmitted()) {
								for (int i=0; i < comp.getNumOfSubmissions(); i++) {
									sendJob(sender, comp);
								}
								// Set submitted flag
								comp.setSubmitted(true);
							}
						}
					}
				}
				// Take a short break before reading the job list again
				Thread.sleep(10);
			} catch (InterruptedException e) {
				break;
			}
		}
	}
	
	public void sendJob(ZMQ.Socket sender, Computation job) {
		ComputationMessage.Builder message = ComputationMessage.newBuilder();
		message.setValue1(job.getFirstVal());
		message.setValue2(job.getSecondVal());
		message.setId(job.getJobID());
		message.setOperation(job.getOperation());
		//currently this message type has no properties
		//so we just build it and send an empty message
		sender.send(message.build().toByteArray());
	}
}

