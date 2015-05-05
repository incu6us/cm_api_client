package com.intropro.hadoop.api.cm;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.cloudera.api.v6.RootResourceV6;

@Consumes({ MediaType.APPLICATION_JSON })
@Produces({ MediaType.APPLICATION_JSON })
public class Trial {

	private RootResourceV6 apiRootV6;

	public Trial() {
		super();
	}

	public Trial(RootResourceV6 apiRootV6) {
		super();
		this.apiRootV6 = apiRootV6;
	}

	public RootResourceV6 getApiRootV6() {
		return apiRootV6;
	}

	public void setApiRootV6(RootResourceV6 apiRootV6) {
		this.apiRootV6 = apiRootV6;
	}

	/**
	 * Begin trial
	 */
	@POST
	@Consumes
	@Path("/trial/begin")
	public void begin() {
		apiRootV6.getClouderaManagerResource().beginTrial();
	}

	/**
	 * End trial (back to free license)
	 */
	public void end() {
		apiRootV6.getClouderaManagerResource().endTrial();
	}
}
