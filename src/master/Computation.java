package master;

import java.util.ArrayList;


/*
 * this is a class to store ranges of values to
 * be distributed between workers
 */
public class Computation {
	
	private Integer firstVal;
	private Integer secondVal;
	private Integer jobID;
	private Integer duplicates_submited;
	private ArrayList<Integer> resultList = new ArrayList<Integer>();
	private boolean submitted;

	// Constructor
	public Computation(Integer firstVal, Integer secondVal, Integer jobID,Integer duplicates_submited) {
		this.firstVal = firstVal;
		this.secondVal = secondVal;
		this.jobID = jobID;
		this.duplicates_submited = duplicates_submited;
		this.submitted = false;
	}
	
	public Integer getFirstVal(){
		return this.firstVal;
	}
	
	public Integer getSecondVal() {
		return this.secondVal;
	}
	
	public Integer getJobID() {
		return this.jobID;
	}
	
	public ArrayList<Integer> getResultList(){
		return this.resultList;
	}
	
	public Integer getNumOfSubmissions(){
		return this.duplicates_submited;
	}
	
	public boolean isSubmitted() {
		return this.submitted;
	}
	
	public void setSubmitted(boolean submitted) {
		this.submitted = submitted;
	}
}
