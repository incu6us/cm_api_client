package com.intropro.hadoop.api.cm;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.cloudera.api.v9.RootResourceV9;

public class Trial {
	
	private RootResourceV9 apiRootV9;

	public Trial() {
		super();
	}

	public Trial(RootResourceV9 apiRootV9) {
		super();
		this.apiRootV9 = apiRootV9;
	}

	public RootResourceV9 getApiRootV9() {
		return apiRootV9;
	}

	public void setApiRootV9(RootResourceV9 apiRootV9) {
		this.apiRootV9 = apiRootV9;
	}
	
	/**
	 * Begin trial
	 */
	public void begin(){
		apiRootV9.getClouderaManagerResource().beginTrial();
	}
	
	/**
	 * End trial (back to free license)
	 */
	public void end(){
		apiRootV9.getClouderaManagerResource().endTrial();
	}
}
