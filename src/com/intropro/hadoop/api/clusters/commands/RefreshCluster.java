package com.intropro.hadoop.api.clusters.commands;

import com.cloudera.api.v9.RootResourceV9;

/**
 * Updates all refreshable configuration files in the cluster. Will not restart
 * any roles. Work with Api V6
 *
 */
public class RefreshCluster {

	private RootResourceV9 apiRootV9;

	public RefreshCluster() {
		super();
	}

	public RefreshCluster(RootResourceV9 apiRootV9) {
		super();
		this.apiRootV9 = apiRootV9;
	}

	public RootResourceV9 getApiRootV9() {
		return apiRootV9;
	}

	public void setApiRootV9(RootResourceV9 apiRootV9) {
		this.apiRootV9 = apiRootV9;
	}

	@SuppressWarnings("finally")
	public String doFresh(String clusterName) {
		String log = "refresh fail";
		try {
			if (apiRootV9.getClustersResource().refresh(clusterName).getSuccess()) {
				log = "refreshed succesfully";
			}
		} catch (NullPointerException e) {
			e.printStackTrace();
			log = "no such cluster with name: " + clusterName;
		} finally {
			return log;
		}
	}

}