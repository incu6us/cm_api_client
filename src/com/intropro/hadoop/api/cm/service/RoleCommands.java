package com.intropro.hadoop.api.cm.service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.cloudera.api.DataView;
import com.cloudera.api.model.ApiCluster;
import com.cloudera.api.model.ApiRole;
import com.cloudera.api.model.ApiRoleNameList;
import com.cloudera.api.model.ApiRoleState;
import com.cloudera.api.model.ApiService;
import com.cloudera.api.v1.ClustersResource;
import com.cloudera.api.v1.RolesResource;
import com.cloudera.api.v1.RootResourceV1;
import com.cloudera.api.v1.ServicesResource;

public class RoleCommands {

	private RootResourceV1 apiRootV1;

	public RoleCommands() {
		super();
	}

	public RoleCommands(RootResourceV1 apiRootV1) {
		super();
		this.apiRootV1 = apiRootV1;
	}

	public RootResourceV1 getApiRootV1() {
		return apiRootV1;
	}

	public void setApiRootV1(RootResourceV1 apiRootV1) {
		this.apiRootV1 = apiRootV1;
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
		return log;
	}

	/**
	 * Start roles in service of cluster. Will be start only stopped roles
	 * 
	 * @param clusterName
	 * @param serviceName
	 * @return error logs
	 */
	public Map<String, List<String>> startRoles(String clusterName, String serviceName) {
		Map<String, List<String>> log = new HashMap<String, List<String>>();
		log.put(clusterName, Arrays.asList("no such cluster"));

		ClustersResource clustersResource = apiRootV1.getClustersResource();
		for (ApiCluster cluster : clustersResource.readClusters(DataView.FULL)) {
			if (cluster.getName().equals(clusterName)) {
				log.clear();
				ServicesResource servicesResource = clustersResource.getServicesResource(cluster.getName());
				log.put(serviceName, Arrays.asList("no such service"));
				for (ApiService service : servicesResource.readServices(DataView.FULL)) {
					if (service.getName().equals(serviceName)) {
						log.clear();
						RolesResource rolesResource = servicesResource.getRolesResource(service.getName());
						log.put(serviceName, Arrays.asList("nothing to start"));
						for (ApiRole role : rolesResource.readRoles()) {
							if (role.getRoleState().equals(ApiRoleState.STOPPED) || role.getRoleState().equals(ApiRoleState.STOPPING)) {

								log.clear();
								ApiRoleNameList roleNames = new ApiRoleNameList();
								roleNames.add(role.getName());
								List<String> tmpLog = apiRootV1.getClustersResource().getServicesResource(cluster.getName()).getRoleCommandsResource(service.getName()).startCommand(roleNames)
										.getErrors();
								if (tmpLog.isEmpty()) {
									log.put(role.getName(), Arrays.asList("state was updated succesfully"));
								} else {
									log.put(role.getName(), tmpLog);
								}

							}
						}
					}
				}
			}
		}
		return log;
	}

	/**
	 * Stop roles in service of cluster. Will be stop only started roles
	 * 
	 * @param clusterName
	 * @param serviceName
	 * @return error logs
	 */
	public Map<String, List<String>> stopRoles(String clusterName, String serviceName) {
		Map<String, List<String>> log = new HashMap<String, List<String>>();
		log.put(clusterName, Arrays.asList("no such cluster"));

		ClustersResource clustersResource = apiRootV1.getClustersResource();
		for (ApiCluster cluster : clustersResource.readClusters(DataView.FULL)) {
			if (cluster.getName().equals(clusterName)) {
				log.clear();
				ServicesResource servicesResource = clustersResource.getServicesResource(cluster.getName());
				log.put(serviceName, Arrays.asList("no such service"));
				for (ApiService service : servicesResource.readServices(DataView.FULL)) {
					if (service.getName().equals(serviceName)) {
						log.clear();
						RolesResource rolesResource = servicesResource.getRolesResource(service.getName());
						log.put(serviceName, Arrays.asList("nothing to stop"));
						for (ApiRole role : rolesResource.readRoles()) {
							if (role.getRoleState().equals(ApiRoleState.STARTED) || role.getRoleState().equals(ApiRoleState.STARTING)) {

								log.clear();
								ApiRoleNameList roleNames = new ApiRoleNameList();
								roleNames.add(role.getName());
								List<String> tmpLog = apiRootV1.getClustersResource().getServicesResource(cluster.getName()).getRoleCommandsResource(service.getName()).stopCommand(roleNames)
										.getErrors();
								if (tmpLog.isEmpty()) {
									log.put(role.getName(), Arrays.asList("state was updated succesfully"));
								} else {
									log.put(role.getName(), tmpLog);
								}

							}
						}
					}
				}
			}
		}
		return log;
	}

}
