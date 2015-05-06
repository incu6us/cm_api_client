package com.intropro.hadoop.api.clusters.commands;

import java.util.Arrays;

import com.cloudera.api.model.ApiHostInstallArguments;
import com.cloudera.api.v9.RootResourceV9;

public class HostInstall {

	private RootResourceV9 apiRootV9;

	public HostInstall() {
		super();
	}

	public HostInstall(RootResourceV9 apiRootV9) {
		super();
		this.apiRootV9 = apiRootV9;
	}

	public RootResourceV9 getApiRootV9() {
		return apiRootV9;
	}

	public void setApiRootV9(RootResourceV9 apiRootV9) {
		this.apiRootV9 = apiRootV9;
	}

	public void install(String hostname, String sshUserName, String sshPort){
		ApiHostInstallArguments hostInstall = new ApiHostInstallArguments();
		hostInstall.setHostNames(Arrays.asList(hostname));
		hostInstall.setUserName(sshUserName);
		hostInstall.setSshPort(Integer.valueOf(sshPort));
		apiRootV9.getClouderaManagerResource().hostInstallCommand(hostInstall);
	}
}
