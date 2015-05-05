package com.intropro.hadoop.api.clusters.commands;

import com.cloudera.api.model.ApiCommand;
import com.cloudera.api.v6.RootResourceV6;

/**
 * Updates all refreshable configuration files in the cluster. Will not restart
 * any roles. Work with Api V6
 *
 */
public class RefreshCluster {

	private RootResourceV6 apiRootV6;

	public RefreshCluster() {
		super();
	}

	public RefreshCluster(RootResourceV6 apiRootV6) {
		super();
		this.apiRootV6 = apiRootV6;
	}

	public RootResourceV6 getApiRootV6() {
		return apiRootV6;
	}

	public void setApiRootV6(RootResourceV6 apiRootV6) {
		this.apiRootV6 = apiRootV6;
	}

	@SuppressWarnings("finally")
	public String doFresh(String clusterName) {
		String log = "refresh fail";
		try {
			ApiCommand cmd = apiRootV6.getClustersResource().refresh(clusterName);
			if (cmd.getId() instanceof Long) {
				log = "refreshed succesfully";
			}
		} catch (NullPointerException e) {
			log = "no such cluster with name: " + clusterName;
		} finally {
			return log;
		}
	}

}