package com.intropro.main.example;

import org.apache.log4j.Logger;

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
import com.intropro.hadoop.api.InitApiConnection;

public class Test {

	private static Logger LOG = Logger.getLogger(RoleCommandsExample.class);

	private static String HOST = "c-master";
	private static String USER = "admin";
	private static String PASS = "admin";
	private static String MAPREDUCESERVICE = "mapreduce";

	public static void main(String... args) {

		InitApiConnection connection = new InitApiConnection(HOST, USER, PASS);
		RootResourceV1 apiRootV1 = connection.getApiRootV1();

		/*
		 * List of clusters
		 */
		// ApiClusterList clusters =
		// apiRootV9.getClustersResource().readClusters(DataView.SUMMARY);
		// for (ApiCluster cluster : clusters) {
		// LOG.info(cluster.getName() + " - " + cluster.getVersion());
		// }

		/*
		 * Start first cluster
		 */
		// ApiCommand cmd =
		// apiRootV9.getClustersResource().startCommand(clusters.get(0).getName());
		// while (cmd.isActive()) {
		// Thread.sleep(100);
		// cmd = apiRootV9.getCommandsResource().readCommand(cmd.getId());
		// }
		//
		// LOG.info("Cluster start");
		// LOG.info(cmd.getResultMessage());


		/*
		 * List all services with roles
		 */
		ClustersResource clustersResource = apiRootV1.getClustersResource();
		for (ApiCluster cluster : clustersResource.readClusters(DataView.FULL)) {
			LOG.info("cluster --> "+cluster.getName());
			ServicesResource servicesResource = clustersResource.getServicesResource(cluster.getName());
			for (ApiService service : servicesResource.readServices(DataView.FULL)) {
				LOG.info("service --> "+service.getName());
				RolesResource rolesResource = servicesResource.getRolesResource(service.getName());
				for (ApiRole role : rolesResource.readRoles()) {
					LOG.info("role --> "+role.getName()+" | status: "+role.getRoleState());
				}
			}
		}

	}

}
