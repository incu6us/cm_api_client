package com.intropro.main.example;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.WebApplicationException;

import org.apache.log4j.Logger;

import com.cloudera.api.ClouderaManagerClientBuilder;
import com.cloudera.api.v6.RootResourceV6;
import com.intropro.hadoop.api.InitApiConnection;
import com.intropro.hadoop.api.cm.Trial;

public class EnableEnterpriseTrialExample {

	private static Logger LOG = Logger.getLogger(EnableEnterpriseTrialExample.class);
	
	private static String HOST = "c-master";
	private static String USER = "admin";
	private static String PASS = "admin";

	public static void main(String... args) {
		InitApiConnection connection = new InitApiConnection();
		connection.setApiRootV6(new ClouderaManagerClientBuilder().withHost(HOST).withUsernamePassword(USER, PASS).build().getRootV6());
		RootResourceV6 apiRootV6 = connection.getApiRootV6();

		Trial trial = new Trial(apiRootV6);
		try {
			trial.begin(); // or end()
			LOG.info("done");
		} catch (BadRequestException e) {
			LOG.error("status: "+e.getResponse().getStatus()+" message: "+ e);
		}
	}
}
