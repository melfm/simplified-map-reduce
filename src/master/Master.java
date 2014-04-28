package master;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;


public class Master {
	
	public static final Object lock = new Object();
	
	
	public static void main(String[] args) throws NumberFormatException, IOException{
		//the context is an object shared between all zmq sockets
		Context context = ZMQ.context(1);
		//HashMap<Integer, Computation> results  = new HashMap<Integer, Computation>();
		
		//create an array list of values to send to workers
		//from the input file
		//ArrayList<Computation> workParts = new ArrayList<Computation>();
		HashMap<Integer, Computation> workParts = new HashMap<Integer, Computation>();
		
		//you need to populate this arraylist 
		String dataFile = args[0];
		Integer operation =Integer.parseInt(args[1]);
		
		getInputFile(dataFile,workParts,operation);
		// Testing values are in the table correctly
		for(Map.Entry<Integer, Computation> entry : workParts.entrySet()){
			System.out.println(entry.getKey() + " val: " + entry.getValue().getFirstVal() +" "+ entry.getValue().getSecondVal());	
		}
		
		Thread bootstrapServer = new Thread(new BootstrapServer(context));
		bootstrapServer.start();
			
		//this will distribute work from the workParts arraylist between workers
		//currently workParts is empty, fix this
		Thread ventilator = new Thread(new Ventilator(context, workParts));
		ventilator.start();
		
		
		//create the sink to receive results
		Thread sink = new Thread(new Sink(context, workParts));
		sink.start();	
	
	}
	
	
	public static void getInputFile(String dataFile, HashMap<Integer, Computation> workParts, Integer operation) throws IOException{
		// Read the comma-separated set of integers and populate computation list
		BufferedReader br = new BufferedReader(new FileReader(dataFile));
		String line;
		int jobID =1 ;
		while ((line = br.readLine()) != null) {
			String[] fields = line.split("\\,");
			for(int i=0; i < fields.length; i++) {
				// Grab a pair
				int numbFirst = Integer.parseInt(fields[i]);
				i++;
				int numbSecond = Integer.parseInt(fields[i]);
				Computation comp = new Computation(numbFirst, numbSecond, jobID, 2, operation);
				workParts.put(jobID, comp);
				jobID++;
			}
		}
		br.close();	
	}
}
