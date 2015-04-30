package com.intropro.main.example;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.cloudera.api.DataView;
import com.cloudera.api.model.ApiCluster;
import com.cloudera.api.model.ApiClusterList;
import com.cloudera.api.v1.RootResourceV1;
import com.intropro.hadoop.api.InitApiConnection;
import com.intropro.hadoop.api.cm.service.RoleCommands;

public class RoleCommandsExample {

	private static Logger LOG = Logger.getLogger(RoleCommandsExample.class);

	private static String HOST = "c-master";
	private static String USER = "admin";
	private static String PASS = "admin";

	public static void main(String... args) throws InterruptedException, IOException {
		
		InitApiConnection connection = new InitApiConnection(HOST, USER, PASS);
		RootResourceV1 apiRootV1 = connection.getApiRootV1();
		
		/*
		 * List of clusters
		 */
		ApiClusterList clusters = apiRootV1.getClustersResource().readClusters(DataView.SUMMARY);
		for (ApiCluster cluster : clusters) {
			LOG.info(cluster.getName() + " - " + cluster.getVersion());
		}


		/*
		 * Get list of services and roles
		 */
		RoleCommands role = new RoleCommands(apiRootV1);
//		Map<String, List<String>> log = role.startAllRoles();
		Map<String, List<String>> log = role.startRoles("Cluster 1", "mapreduce");
		LOG.info(log);
	}
}
