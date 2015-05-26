package com.intropro.hadoop.api.clusters;

import java.util.Arrays;

import com.cloudera.api.DataView;
import com.cloudera.api.model.ApiHost;
import com.cloudera.api.model.ApiHostInstallArguments;
import com.cloudera.api.model.ApiHostList;
import com.cloudera.api.model.ApiHostNameList;
import com.cloudera.api.model.ApiHostRef;
import com.cloudera.api.model.ApiHostRefList;
import com.cloudera.api.model.ApiTimeSeriesResponseList;
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

	/**
	 * Install new host
	 * 
	 * @param hostname
	 * @param sshUserName
	 * @param sshPort
	 */
	public void install(String hostname, String sshUserName, String sshPort) {
		ApiHostInstallArguments hostInstall = new ApiHostInstallArguments();
		hostInstall.setHostNames(Arrays.asList(hostname));
		hostInstall.setUserName(sshUserName);
		hostInstall.setSshPort(Integer.valueOf(sshPort));
		apiRootV9.getClouderaManagerResource().hostInstallCommand(hostInstall);
	}

	/**
	 * Add installed host to cluster
	 * 
	 * @param clusterName
	 * @param hostname
	 */
	public void addHost(String clusterName, String hostname) {
		ApiHostRef host = new ApiHostRef(hostname);
		ApiHostRefList hostList = new ApiHostRefList(Arrays.asList(host));
		apiRootV9.getClustersResource().addHosts(clusterName, hostList);
	}

	/**
	 * Get hosts info
	 * 
	 * @param clusterName
	 * @return
	 */
	public ApiHostRefList getHosts(String clusterName) {
		ApiHostRefList apiHostList = new ApiHostRefList();
		apiHostList = apiRootV9.getClustersResource().listHosts(clusterName);
		return apiHostList;
	}

	/**
	 * Get hostId by hostname. If hostname exist will return null
	 * 
	 * @param clusterName
	 * @param hostName
	 * @return
	 */
	public String getHostId(String clusterName, String hostName) {
		String hostId = null;
		ApiHostList hostList = apiRootV9.getHostsResource().readHosts(DataView.SUMMARY);
		for (ApiHost host : hostList) {
			if (host.getHostname().equals(hostName)) {
				hostId = host.getHostId();
			}
		}
		return hostId;
	}

	/**
	 * Delete host from cluster by hostname (ex.: "node4.example.com")
	 * 
	 * @param clusterName
	 * @param hostId
	 */
	public String deleteHostFormClusterByName(String clusterName, String hostName) {
		String hostId = getHostId(clusterName, hostName);
		return apiRootV9.getClustersResource().removeHost(clusterName, hostId).getHostId();
	}

	/**
	 * Delete host from cluster by hostID
	 * 
	 * @param clusterName
	 * @param hostId
	 */
	public void deleteHostFormClusterById(String clusterName, String hostId) {
		apiRootV9.getClustersResource().removeHost(clusterName, hostId);
	}

	/**
	 * Delete host from configuration by name
	 * 
	 * @param clusterName
	 * @param hostName
	 */
	public void deleteHostByName(String clusterName, String hostName) {
		String hostId = getHostId(clusterName, hostName);
		apiRootV9.getHostsResource().deleteHost(hostId);
	}

	/**
	 * Delete host from configuration by id
	 * 
	 * @param clusterName
	 * @param hostId
	 */
	public void deleteHostById(String clusterName, String hostId) {
		apiRootV9.getHostsResource().deleteHost(hostId);
	}

	/**
	 * Host decommision
	 * 
	 * @param hostName
	 */
	public void decommision(String hostName) {
		ApiHostNameList hostNameList = new ApiHostNameList(Arrays.asList(hostName));
		apiRootV9.getClouderaManagerResource().hostsDecommissionCommand(hostNameList);
	}
	
	/**
	 * Returns time-series list of metrics in 5 minutes
	 * @see {@link http://www.cloudera.com/content/cloudera/en/documentation/cloudera-manager/v4-latest/Cloudera-Manager-Diagnostics-Guide/cmdg_tsquery.html}
	 * @param query
	 * @return
	 */
	public ApiTimeSeriesResponseList metrics5min(String query){
		return apiRootV9.getTimeSeriesResource().queryTimeSeries(query, null, "now");
	}
}
