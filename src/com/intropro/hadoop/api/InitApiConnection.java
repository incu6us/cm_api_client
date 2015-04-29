package com.intropro.hadoop.api;

import com.cloudera.api.ClouderaManagerClientBuilder;
import com.cloudera.api.v1.RootResourceV1;
import com.cloudera.api.v2.RootResourceV2;
import com.cloudera.api.v3.RootResourceV3;
import com.cloudera.api.v4.RootResourceV4;
import com.cloudera.api.v5.RootResourceV5;
import com.cloudera.api.v6.RootResourceV6;
import com.cloudera.api.v7.RootResourceV7;
import com.cloudera.api.v8.RootResourceV8;
import com.cloudera.api.v9.RootResourceV9;

public class InitApiConnection {

	private String host = "c-master";
	private String user = "admin";
	private String password = "admin";
	// private String MAPREDUCESERVICE = "mapreduce";

	private RootResourceV1 apiRootV1;
	private RootResourceV2 apiRootV2;
	private RootResourceV3 apiRootV3;
	private RootResourceV4 apiRootV4;
	private RootResourceV5 apiRootV5;
	private RootResourceV6 apiRootV6;
	private RootResourceV7 apiRootV7;
	private RootResourceV8 apiRootV8;
	private RootResourceV9 apiRootV9;

	public InitApiConnection() {
		super();
		
		this.apiRootV1 = new ClouderaManagerClientBuilder().withHost(this.host).withUsernamePassword(this.user, this.password).build().getRootV1();
	}

	public InitApiConnection(String host, String user, String password) {
		super();
		this.host = host;
		this.user = user;
		this.password = password;
		
		this.apiRootV1 = new ClouderaManagerClientBuilder().withHost(this.host).withUsernamePassword(this.user, this.password).build().getRootV1();
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public RootResourceV1 getApiRootV1() {
		return apiRootV1;
	}

	public void setApiRootV1(RootResourceV1 apiRootV1) {
		this.apiRootV1 = apiRootV1;
	}

	public RootResourceV2 getApiRootV2() {
		return apiRootV2;
	}

	public void setApiRootV2(RootResourceV2 apiRootV2) {
		this.apiRootV2 = apiRootV2;
	}

	public RootResourceV3 getApiRootV3() {
		return apiRootV3;
	}

	public void setApiRootV3(RootResourceV3 apiRootV3) {
		this.apiRootV3 = apiRootV3;
	}

	public RootResourceV4 getApiRootV4() {
		return apiRootV4;
	}

	public void setApiRootV4(RootResourceV4 apiRootV4) {
		this.apiRootV4 = apiRootV4;
	}

	public RootResourceV5 getApiRootV5() {
		return apiRootV5;
	}

	public void setApiRootV5(RootResourceV5 apiRootV5) {
		this.apiRootV5 = apiRootV5;
	}

	public RootResourceV6 getApiRootV6() {
		return apiRootV6;
	}

	public void setApiRootV6(RootResourceV6 apiRootV6) {
		this.apiRootV6 = apiRootV6;
	}

	public RootResourceV7 getApiRootV7() {
		return apiRootV7;
	}

	public void setApiRootV7(RootResourceV7 apiRootV7) {
		this.apiRootV7 = apiRootV7;
	}

	public RootResourceV8 getApiRootV8() {
		return apiRootV8;
	}

	public void setApiRootV8(RootResourceV8 apiRootV8) {
		this.apiRootV8 = apiRootV8;
	}

	public RootResourceV9 getApiRootV9() {
		return apiRootV9;
	}

	public void setApiRootV9(RootResourceV9 apiRootV9) {
		this.apiRootV9 = apiRootV9;
	}

	
}
