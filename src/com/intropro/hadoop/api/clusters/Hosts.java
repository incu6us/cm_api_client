package com.intropro.hadoop.api.clusters;

import java.util.Arrays;

import com.cloudera.api.model.ApiHostRef;
import com.cloudera.api.model.ApiHostRefList;
import com.cloudera.api.v9.RootResourceV9;

/**
 * - the hosts associated with the cluster; - the newly added hosts to the
 * cluster. if a host is already a member, it will be excluded from the list; -
 * all the hosts that were unassociated with the cluster
 *
 */
public class Hosts {

	private RootResourceV9 apiRootV9;

	public Hosts() {
		super();
	}

	public Hosts(RootResourceV9 apiRootV9) {
		super();
		this.apiRootV9 = apiRootV9;
	}

	public RootResourceV9 getApiRootV9() {
		return apiRootV9;
	}

	public void setApiRootV9(RootResourceV9 apiRootV9) {
		this.apiRootV9 = apiRootV9;
	}
	
	public void addHost(String clusterName, String hostname){
		ApiHostRef host = new ApiHostRef(hostname);
		ApiHostRefList hostList = new ApiHostRefList(Arrays.asList(host));
		apiRootV9.getClustersResource().addHosts(clusterName, hostList);
	}
}
