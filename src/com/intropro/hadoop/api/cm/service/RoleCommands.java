package com.intropro.hadoop.api.cm.service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.cloudera.api.DataView;
import com.cloudera.api.model.ApiCluster;
import com.cloudera.api.model.ApiHostRef;
import com.cloudera.api.model.ApiRole;
import com.cloudera.api.model.ApiRoleList;
import com.cloudera.api.model.ApiRoleNameList;
import com.cloudera.api.model.ApiRoleState;
import com.cloudera.api.model.ApiService;
import com.cloudera.api.model.ApiServiceRef;
import com.cloudera.api.v1.ClustersResource;
import com.cloudera.api.v1.RolesResource;
import com.cloudera.api.v1.RootResourceV1;
import com.cloudera.api.v1.ServicesResource;
import com.cloudera.api.v9.RootResourceV9;

public class RoleCommands {

	private RootResourceV1 apiRootV1;
	private RootResourceV9 apiRootV9;

	public RoleCommands() {
		super();
	}

	public RoleCommands(RootResourceV1 apiRootV1) {
		super();
		this.apiRootV1 = apiRootV1;
	}

	public RoleCommands(RootResourceV9 apiRootV9) {
		super();
		this.apiRootV9 = apiRootV9;
	}

	public RootResourceV1 getApiRootV1() {
		return apiRootV1;
	}

	public void setApiRootV1(RootResourceV1 apiRootV1) {
		this.apiRootV1 = apiRootV1;
	}

	public RootResourceV9 getApiRootV9() {
		return apiRootV9;
	}

	public void setApiRootV9(RootResourceV9 apiRootV9) {
		this.apiRootV9 = apiRootV9;
	}

	/**
	 * Start all role in all clusters. Will be start only stopped roles
	 * 
	 * @return error logs
	 */
	public Map<String, List<String>> startAllRoles() {
		Map<String, List<String>> log = new HashMap<String, List<String>>();

		ClustersResource clustersResource = apiRootV1.getClustersResource();
		for (ApiCluster cluster : clustersResource.readClusters(DataView.FULL)) {
			ServicesResource servicesResource = clustersResource.getServicesResource(cluster.getName());
			for (ApiService service : servicesResource.readServices(DataView.FULL)) {
				RolesResource rolesResource = servicesResource.getRolesResource(service.getName());
				for (ApiRole role : rolesResource.readRoles()) {
					if (role.getRoleState().equals(ApiRoleState.STOPPED) || role.getRoleState().equals(ApiRoleState.STOPPING)) {

						ApiRoleNameList roleNames = new ApiRoleNameList();
						roleNames.add(role.getName());
						List<String> tmpLog = apiRootV1.getClustersResource().getServicesResource(cluster.getName()).getRoleCommandsResource(service.getName()).startCommand(roleNames).getErrors();
						if (tmpLog.isEmpty()) {
							log.put(role.getName(), Arrays.asList("state was updated succesfully"));
						} else {
							log.put(role.getName(), tmpLog);
						}

					}
				}
			}
		}

		if (log.isEmpty()) {
			log.put("null", Arrays.asList("nothing to do..."));
		}

		return log;
	}

	/**
	 * Stop all role in all clusters. Will be stop only started roles
	 * 
	 * @return error logs
	 */
	public Map<String, List<String>> stopAllRoles() {
		Map<String, List<String>> log = new HashMap<String, List<String>>();

		ClustersResource clustersResource = apiRootV1.getClustersResource();
		for (ApiCluster cluster : clustersResource.readClusters(DataView.FULL)) {
			ServicesResource servicesResource = clustersResource.getServicesResource(cluster.getName());
			for (ApiService service : servicesResource.readServices(DataView.FULL)) {
				RolesResource rolesResource = servicesResource.getRolesResource(service.getName());
				for (ApiRole role : rolesResource.readRoles()) {
					if (role.getRoleState().equals(ApiRoleState.STARTED) || role.getRoleState().equals(ApiRoleState.STARTING)) {

						ApiRoleNameList roleNames = new ApiRoleNameList();
						roleNames.add(role.getName());
						List<String> tmpLog = apiRootV1.getClustersResource().getServicesResource(cluster.getName()).getRoleCommandsResource(service.getName()).stopCommand(roleNames).getErrors();
						if (tmpLog.isEmpty()) {
							log.put(role.getName(), Arrays.asList("state was updated succesfully"));
						} else {
							log.put(role.getName(), tmpLog);
						}

					}
				}
			}
		}

		if (log.isEmpty()) {
			log.put("null", Arrays.asList("nothing to do..."));
		}

		return log;
	}

	/**
	 * Add HDFS role to host
	 * 
	 * @param clusterName
	 * @param hostId
	 *            from Hosts.getHostId
	 * @param roleType
	 */
	public void addHdfsRole(String clusterName, String hostId, HdfsRoleType roleType) {
		String serviceName = "HDFS";

		ApiHostRef hostRef = new ApiHostRef(hostId);

		ApiServiceRef serviceRef = new ApiServiceRef(clusterName, serviceName);

		ApiRole role = new ApiRole();
		role.setHostRef(hostRef);
		role.setServiceRef(serviceRef);
		role.setType(roleType.toString());
		
		ApiRoleList roles = new ApiRoleList(Arrays.asList(role));
		apiRootV9.getClustersResource().getServicesResource(clusterName).getRolesResource(serviceName).createRoles(roles);
	}
	
	public void addMapReduceRole(String clusterName, String hostId, MapReduceRoleType roleType) {
		String serviceName = "MAPRED";

		ApiHostRef hostRef = new ApiHostRef(hostId);

		ApiServiceRef serviceRef = new ApiServiceRef(clusterName, serviceName);

		ApiRole role = new ApiRole();
		role.setHostRef(hostRef);
		role.setServiceRef(serviceRef);
		role.setType(roleType.toString());
		
		ApiRoleList roles = new ApiRoleList(Arrays.asList(role));
		apiRootV9.getClustersResource().getServicesResource(clusterName).getRolesResource(serviceName).createRoles(roles);
	}
}
