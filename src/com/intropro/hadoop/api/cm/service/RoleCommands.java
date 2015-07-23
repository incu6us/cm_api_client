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

	private final String HDFSSERVICENAME = "HDFS";
	private final String MAPREDSERVICENAME = "MAPRED";
	private final String YARNSERVICENAME = "YARN";
	private final String FLUMESERVICENAME = "FLUME";
	private final String OOZIESERVICENAME = "OOZIE";
	private final String HIVESERVICENAME = "HIVE";
	private final String SPARKONYARNSERVICENAME = "SPARK_ON_YARN";
	private final String HBASESERVICENAME = "HBASE";

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
		ApiHostRef hostRef = new ApiHostRef(hostId);

		ApiServiceRef serviceRef = new ApiServiceRef(clusterName, HDFSSERVICENAME);

		ApiRole role = new ApiRole();
		role.setHostRef(hostRef);
		role.setServiceRef(serviceRef);
		role.setType(roleType.toString());

		ApiRoleList roles = new ApiRoleList(Arrays.asList(role));
		System.out.println(apiRootV9.getClustersResource().getServicesResource(clusterName).getRolesResource(HDFSSERVICENAME).createRoles(roles).getRoles());
	}

	/**
	 * Check role state for HDFS role
	 * 
	 * @param clusterName
	 * @param hostId
	 * @param roleType
	 * @return
	 */
	public String checkHdfsRoleState(String clusterName, String hostId, HdfsRoleType roleType) {
		String roleState = null;

		ApiRoleList createdRoles = apiRootV9.getClustersResource().getServicesResource(clusterName).getRolesResource(HDFSSERVICENAME).readRoles();

		for (ApiRole r : createdRoles) {
			if (r.getHostRef().getHostId().equals(hostId) && r.getServiceRef().getServiceName().equals(HDFSSERVICENAME) && r.getType().equals(roleType.name())) {
				roleState = apiRootV9.getClustersResource().getServicesResource(clusterName).getRolesResource(HDFSSERVICENAME).readRole(r.getName()).getRoleState().name();
			}
		}
		return roleState;
	}

	/**
	 * Delete role from node
	 * 
	 * @param clusterName
	 * @param hostId
	 * @param serviceName
	 */
	public void deleteRole(String clusterName, String hostId, String serviceName) {
		ApiRoleList roles = apiRootV9.getClustersResource().getServicesResource(clusterName).getRolesResource(serviceName).readRoles();

		for (ApiRole r : roles) {
			if (r.getHostRef().getHostId().equals(hostId) && r.getServiceRef().getServiceName().equals(serviceName)) {
				apiRootV9.getClustersResource().getServicesResource(clusterName).getRolesResource(serviceName).deleteRole(r.getName());
			}
		}
	}

	/**
	 * Add MapReduce role to host
	 * 
	 * @param clusterName
	 * @param hostId
	 * @param roleType
	 */
	public void addMapReduceRole(String clusterName, String hostId, MapReduceRoleType roleType) {
		ApiHostRef hostRef = new ApiHostRef(hostId);

		ApiServiceRef serviceRef = new ApiServiceRef(clusterName, MAPREDSERVICENAME);

		ApiRole role = new ApiRole();
		role.setHostRef(hostRef);
		role.setServiceRef(serviceRef);
		role.setType(roleType.toString());

		ApiRoleList roles = new ApiRoleList(Arrays.asList(role));
		apiRootV9.getClustersResource().getServicesResource(clusterName).getRolesResource(MAPREDSERVICENAME).createRoles(roles);
	}
	
	/**
	 * Add Yarn role to host
	 * 
	 * @param clusterName
	 * @param hostId
	 * @param roleType
	 */
	public void addYarnRole(String clusterName, String hostId, YarnRoleType roleType) {
		ApiHostRef hostRef = new ApiHostRef(hostId);

		ApiServiceRef serviceRef = new ApiServiceRef(clusterName, YARNSERVICENAME);

		ApiRole role = new ApiRole();
		role.setHostRef(hostRef);
		role.setServiceRef(serviceRef);
		role.setType(roleType.toString());

		ApiRoleList roles = new ApiRoleList(Arrays.asList(role));
		apiRootV9.getClustersResource().getServicesResource(clusterName).getRolesResource(YARNSERVICENAME).createRoles(roles);
	}
	
	/**
	 * Add Flume role to the host
	 * 
	 * @param clusterName
	 * @param hostId
	 * @param roleType
	 */
	public void addFlumeRole(String clusterName, String hostId, FlumeRoleType roleType) {
		ApiHostRef hostRef = new ApiHostRef(hostId);

		ApiServiceRef serviceRef = new ApiServiceRef(clusterName, FLUMESERVICENAME);

		ApiRole role = new ApiRole();
		role.setHostRef(hostRef);
		role.setServiceRef(serviceRef);
		role.setType(roleType.toString());

		ApiRoleList roles = new ApiRoleList(Arrays.asList(role));
		apiRootV9.getClustersResource().getServicesResource(clusterName).getRolesResource(FLUMESERVICENAME).createRoles(roles);
	}
	
	/**
	 * Add Oozie role to the host
	 * 
	 * @param clusterName
	 * @param hostId
	 * @param roleType
	 */
	public void addOozieRole(String clusterName, String hostId, OozieRoleType roleType) {
		ApiHostRef hostRef = new ApiHostRef(hostId);

		ApiServiceRef serviceRef = new ApiServiceRef(clusterName, OOZIESERVICENAME);

		ApiRole role = new ApiRole();
		role.setHostRef(hostRef);
		role.setServiceRef(serviceRef);
		role.setType(roleType.toString());

		ApiRoleList roles = new ApiRoleList(Arrays.asList(role));
		apiRootV9.getClustersResource().getServicesResource(clusterName).getRolesResource(OOZIESERVICENAME).createRoles(roles);
	}
	
	/**
	 * Add Hive role to the host
	 * 
	 * @param clusterName
	 * @param hostId
	 * @param roleType
	 */
	public void addHiveRole(String clusterName, String hostId, HiveRoleType roleType) {
		ApiHostRef hostRef = new ApiHostRef(hostId);

		ApiServiceRef serviceRef = new ApiServiceRef(clusterName, HIVESERVICENAME);

		ApiRole role = new ApiRole();
		role.setHostRef(hostRef);
		role.setServiceRef(serviceRef);
		role.setType(roleType.toString());

		ApiRoleList roles = new ApiRoleList(Arrays.asList(role));
		apiRootV9.getClustersResource().getServicesResource(clusterName).getRolesResource(HIVESERVICENAME).createRoles(roles);
	}
	
	/**
	 * Add Spark on YARN role to the host
	 * @param clusterName
	 * @param hostId
	 * @param roleType
	 */
	public void addSparkOnYarnRole(String clusterName, String hostId, SparkOnYarnRoleType roleType) {
		ApiHostRef hostRef = new ApiHostRef(hostId);

		ApiServiceRef serviceRef = new ApiServiceRef(clusterName, SPARKONYARNSERVICENAME);

		ApiRole role = new ApiRole();
		role.setHostRef(hostRef);
		role.setServiceRef(serviceRef);
		role.setType(roleType.toString());

		ApiRoleList roles = new ApiRoleList(Arrays.asList(role));
		apiRootV9.getClustersResource().getServicesResource(clusterName).getRolesResource(SPARKONYARNSERVICENAME).createRoles(roles);
	}
	
	/**
	 * Add Hbase role to the host
	 * 
	 * @param clusterName
	 * @param hostId
	 * @param roleType
	 */
	public void addHbaseOnYarnRole(String clusterName, String hostId, HbaseRoleType roleType) {
		ApiHostRef hostRef = new ApiHostRef(hostId);

		ApiServiceRef serviceRef = new ApiServiceRef(clusterName, HBASESERVICENAME);

		ApiRole role = new ApiRole();
		role.setHostRef(hostRef);
		role.setServiceRef(serviceRef);
		role.setType(roleType.toString());

		ApiRoleList roles = new ApiRoleList(Arrays.asList(role));
		apiRootV9.getClustersResource().getServicesResource(clusterName).getRolesResource(HBASESERVICENAME).createRoles(roles);
	}
}
