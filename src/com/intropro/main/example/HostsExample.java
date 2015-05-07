package com.intropro.main.example;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;

import org.apache.log4j.Logger;

import com.cloudera.api.ClouderaManagerClientBuilder;
import com.cloudera.api.v9.RootResourceV9;
import com.intropro.hadoop.api.InitApiConnection;
import com.intropro.hadoop.api.clusters.Hosts;

public class HostsExample {

	private static Logger LOG = Logger.getLogger(HostsExample.class);

	private static String HOST = "52.17.36.200";
	private static String USER = "admin";
	private static String PASS = "admin";
	private static String NODE = "node4.ea.intropro.com";

	public static void main(String... args) {
		InitApiConnection connection = new InitApiConnection();
		connection.setApiRootV9(new ClouderaManagerClientBuilder().withHost(HOST).withUsernamePassword(USER, PASS).build().getRootV9());
		RootResourceV9 apiRootV9 = connection.getApiRootV9();
		Hosts hosts = new Hosts(apiRootV9);

		try {
			hosts.install(NODE, "root", "22");
		} catch (BadRequestException e) {

		}

		try {
			hosts.addHost("AutoCluster1", NODE);
		} catch (NotFoundException e) {
			LOG.error(e);
		}
		LOG.info("finish");
	}
}
