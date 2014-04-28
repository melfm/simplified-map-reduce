package worker;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;

public class Worker {
	public static void main(String[] args){
		String address = null;
		String master = null;
		//the context is an object shared between all zmq sockets
		Context context = ZMQ.context(1);
		
		try{
			 address = args[0];
			 master = args[1];
		} catch(Exception e){
			System.out.println("Usage: Worker <master's IP> <this machine's IP> ");
			e.printStackTrace();
			System.exit(1);
		}
		
		
		BootstrapClient bootstrap = new BootstrapClient(context, master, address);
		if (!bootstrap.bootstrap()){
			System.out.println("unable to bootstrap with master");
			System.exit(1);
		}
	
		System.out.println("Bootstrapped to " + master);
		
		Thread receiver = new Thread(new WorkReceiver(context, master));
		receiver.start();
		
	}
	
	
}
