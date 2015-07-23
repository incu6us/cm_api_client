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

	private static String HOST = "52.17.52.233";
	private static String USER = "admin";
	private static String PASS = "admin";
	private static String NODE = "clouderamanager.node.vpr01.devops.internal.intropro.com";
	
	public static void main(String... args) throws InterruptedException, IOException {
		
		InitApiConnection connection = new InitApiConnection(HOST, USER, PASS);
//		connection.setApiRootV1(new ClouderaManagerClientBuilder().withHost(HOST).withUsernamePassword(USER, PASS).build().getRootV9());
		connection.setApiRootV9(new ClouderaManagerClientBuilder().withHost(HOST).withUsernamePassword(USER, PASS).build().getRootV9());
//		RootResourceV1 apiRootV1 = connection.getApiRootV1();
		RootResourceV9 apiRootV9 = connection.getApiRootV9();
		/*
		 * List of clusters
		 */
		ApiClusterList clusters = apiRootV9.getClustersResource().readClusters(DataView.SUMMARY);
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
		 * Add role (HDFS)
		 */
		Hosts hosts = new Hosts(apiRootV9);
		String hostId = hosts.getHostId("AutoCluster1", NODE);
		LOG.info(hostId);
		RoleCommands role = new RoleCommands(apiRootV9);
		role.addHdfsRole("AutoCluster1", hostId, HdfsRoleType.GATEWAY);
		role.addHdfsRole("AutoCluster1", hostId, HdfsRoleType.DATANODE);
		
//		LOG.info(role.checkHdfsRoleState("AutoCluster1", hostId, HdfsRoleType.DATANODE));
		
//		role.addMapReduceRole("AutoCluster1", hostId, MapReduceRoleType.GATEWAY);
//		role.addMapReduceRole("AutoCluster1", hostId, MapReduceRoleType.TASKTRACKER);
	}
}
