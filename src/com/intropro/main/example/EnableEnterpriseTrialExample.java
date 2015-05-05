package com.intropro.main.example;

import com.cloudera.api.ClouderaManagerClientBuilder;
import com.cloudera.api.v9.RootResourceV9;
import com.intropro.hadoop.api.InitApiConnection;
import com.intropro.hadoop.api.cm.Trial;

public class EnableEnterpriseTrialExample {
	
	private static String HOST = "c-master";
	private static String USER = "admin";
	private static String PASS = "admin";
	
	public static void main(String... args) {
		InitApiConnection connection = new InitApiConnection(HOST, USER, PASS);
		connection.setApiRootV9(new ClouderaManagerClientBuilder().withHost(HOST).withUsernamePassword(USER, PASS).build().getRootV9());
		RootResourceV9 apiRootV9 = connection.getApiRootV9();
		
		Trial trial = new Trial(apiRootV9);
		trial.begin();	// or end()
	}
}
