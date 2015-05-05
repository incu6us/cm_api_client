package com.intropro.main.example;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.cloudera.api.ClouderaManagerClientBuilder;
import com.cloudera.api.DataView;
import com.cloudera.api.model.ApiCluster;
import com.cloudera.api.model.ApiClusterList;
import com.cloudera.api.v1.RootResourceV1;
import com.intropro.hadoop.api.InitApiConnection;
import com.intropro.hadoop.api.clusters.commands.RefreshCluster;

public class RefreshClusterExample {

	private static Logger LOG = Logger.getLogger(RefreshClusterExample.class);

	private static String HOST = "c-master";
	private static String USER = "admin";
	private static String PASS = "admin";

	public static void main(String... args) throws InterruptedException, IOException {

		InitApiConnection connection = new InitApiConnection(HOST, USER, PASS);
		connection.setApiRootV9(new ClouderaManagerClientBuilder().withHost(HOST).withUsernamePassword(USER, PASS).build().getRootV9());
		RootResourceV1 apiRootV1 = connection.getApiRootV1();

		/*
		 * List of clusters
		 */
		ApiClusterList clusters = apiRootV1.getClustersResource().readClusters(DataView.SUMMARY);
		for (ApiCluster cluster : clusters) {
			LOG.info(cluster.getName() + " - " + cluster.getVersion());
		}

		/*
		 * Refresh
		 */
		RefreshCluster refresh = new RefreshCluster(connection.getApiRootV9());
		LOG.info(refresh.doFresh("Cluster 1"));

	}
}
