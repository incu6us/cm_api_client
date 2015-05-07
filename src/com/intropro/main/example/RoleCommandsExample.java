package com.intropro.main.example;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.cloudera.api.ClouderaManagerClientBuilder;
import com.cloudera.api.DataView;
import com.cloudera.api.model.ApiCluster;
import com.cloudera.api.model.ApiClusterList;
import com.cloudera.api.model.ApiService;
import com.cloudera.api.model.ApiServiceList;
import com.cloudera.api.model.ApiServiceRef;
import com.cloudera.api.model.ApiServiceState;
import com.cloudera.api.v1.RootResourceV1;
import com.cloudera.api.v9.RootResourceV9;
import com.intropro.hadoop.api.InitApiConnection;
import com.intropro.hadoop.api.clusters.Hosts;
import com.intropro.hadoop.api.cm.service.HdfsRoleType;
import com.intropro.hadoop.api.cm.service.MapReduceRoleType;
import com.intropro.hadoop.api.cm.service.RoleCommands;

public class RoleCommandsExample {

	private static Logger LOG = Logger.getLogger(RoleCommandsExample.class);

	private static String HOST = "c-master";
	private static String USER = "admin";
	private static String PASS = "admin";
	private static String NODE = "c-slave-1";
	
	public static void main(String... args) throws InterruptedException, IOException {
		
		InitApiConnection connection = new InitApiConnection(HOST, USER, PASS);
		connection.setApiRootV9(new ClouderaManagerClientBuilder().withHost(HOST).withUsernamePassword(USER, PASS).build().getRootV9());
		RootResourceV1 apiRootV1 = connection.getApiRootV1();
		RootResourceV9 apiRootV9 = connection.getApiRootV9();
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
//		RoleCommands role = new RoleCommands(apiRootV1);
//		Map<String, List<String>> log = role.startAllRoles();
//		LOG.info(log);
		
		/*
		 * Add role
		 */
		Hosts hosts = new Hosts(apiRootV9);
		String hostId = hosts.getHostId("AutoCluster1", NODE);
		RoleCommands role = new RoleCommands(apiRootV9);
		role.addHdfsRole("AutoCluster1", hostId, HdfsRoleType.DATANODE);
		role.addHdfsRole("AutoCluster1", hostId, HdfsRoleType.GATEWAY);
		role.addMapReduceRole("AutoCluster1", hostId, MapReduceRoleType.TASKTRACKER);
		role.addMapReduceRole("AutoCluster1", hostId, MapReduceRoleType.GATEWAY);
	}
}
