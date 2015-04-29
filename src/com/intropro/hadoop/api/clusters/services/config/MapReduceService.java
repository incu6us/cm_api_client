package com.intropro.hadoop.api.clusters.services.config;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.cloudera.api.DataView;
import com.cloudera.api.model.ApiCluster;
import com.cloudera.api.model.ApiConfig;
import com.cloudera.api.model.ApiConfigList;
import com.cloudera.api.model.ApiRole;
import com.cloudera.api.model.ApiRoleTypeConfig;
import com.cloudera.api.model.ApiService;
import com.cloudera.api.model.ApiServiceConfig;
import com.cloudera.api.v1.ClustersResource;
import com.cloudera.api.v1.RolesResource;
import com.cloudera.api.v1.RootResourceV1;
import com.cloudera.api.v1.ServicesResource;

public class MapReduceService {

	private RootResourceV1 apiRootV1;
	private String mapReduceServiceName;

	private ApiConfig config;
	private ApiRole role;
	private ApiCluster cluster;
	private ApiService service;
	private static ClassLoader classLoader;

	public MapReduceService() {
		super();
	}

	public MapReduceService(RootResourceV1 apiRootV1, String mapReduceServiceName) {
		super();
		this.apiRootV1 = apiRootV1;
		this.mapReduceServiceName = mapReduceServiceName;
	}

	public RootResourceV1 getApiRootV1() {
		return apiRootV1;
	}

	public void setApiRootV1(RootResourceV1 apiRootV1) {
		this.apiRootV1 = apiRootV1;
	}

	public String getMapReduceServiceName() {
		return mapReduceServiceName;
	}

	public void setMapReduceServiceName(String mapReduceServiceName) {
		this.mapReduceServiceName = mapReduceServiceName;
	}

	/**
	 * change mapred_queue_names_list
	 * 
	 * @param values
	 *            for value field(default queue added automatic if it exist in
	 *            the List)
	 */
	public void changeMapredQueueNamesList(List<String> values) {
		if (checkConfig("mapred_queue_names_list")) {
			// Item
			if (!values.contains("default")) {
				values.add("default");
			}
			saveItem(config.getName(), StringUtils.join(values, ","));
		}
	}

	/**
	 * change mapred_jobtracker_taskScheduler
	 * 
	 * @param scheduler
	 *            class from {@link TaskScheduler}
	 */
	public void changeMapredJobtrackerTaskScheduler(String scheduler) {
		if (checkConfig("mapred_jobtracker_taskScheduler")) {
			// Item
			saveItem(config.getName(), scheduler);
		}
	}

	public void changeMapredCapacitySchedulerConfiguration(List<String> queues) throws IOException {
		if (checkConfig("mapred_capacity_scheduler_configuration")) {
			classLoader = getClass().getClassLoader();
			File xmlQueueTemplate = null;
			String customQueueConfig = "";
			String value = readDefaultQueueFile();

			xmlQueueTemplate = new File(classLoader.getSystemResource("mapRedCustomQueue.xml").getFile());

			for (String queue : queues) {
				BufferedReader br = new BufferedReader(new FileReader(xmlQueueTemplate));
				String line;
				while ((line = br.readLine()) != null) {
					customQueueConfig += line + "\n";
				}
				value += customQueueConfig.replaceAll("\\$\\{QUEUE\\}", queue);
			}
			saveItem(config.getName(), value);
		}
	}

	/**
	 * check current MapReduce service config
	 */
	public Boolean checkConfig(String roleTypeItemName) {
		ClustersResource clustersResource = apiRootV1.getClustersResource();
		for (ApiCluster cluster : clustersResource.readClusters(DataView.FULL)) {
			// LOG.info(cluster.getName());
			this.cluster = cluster;
			ServicesResource servicesResource = clustersResource.getServicesResource(cluster.getName());

			for (ApiService service : servicesResource.readServices(DataView.FULL)) {
				this.service = service;

				if (service.getName().equals(mapReduceServiceName)) {
					RolesResource rolesResource = servicesResource.getRolesResource(service.getName());

					for (ApiRole role : rolesResource.readRoles()) {
						// LOG.info("\t" + role.getName() + " (role) ---> " +
						// role.getType());
						this.role = role;

						if (role.getType().equals("JOBTRACKER")) {
							ApiConfigList configs = new ApiConfigList(rolesResource.readRoleConfig(role.getName(), DataView.FULL).getConfigs());
							// mapred_jobtracker_taskScheduler
							// LOG.info("\nList of config namespaces:");
							for (ApiConfig config : configs) {
								// LOG.info("\t" + config.getName());
								this.config = config;

								if (config.getName().equals(roleTypeItemName)) {
									return true;
								}
							}
						}
					}
				}
			}
		}
		return false;
	}

	private String readDefaultQueueFile() {
		classLoader = getClass().getClassLoader();
		File xmlQueueTemplate = null;
		String customQueueConfig = "";

		xmlQueueTemplate = new File(classLoader.getSystemResource("mapRedDefaultQueue.xml").getFile());

		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(xmlQueueTemplate));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		String line;
		try {
			while ((line = br.readLine()) != null) {
				customQueueConfig += line + "\n";
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return customQueueConfig;
	}

	/**
	 * save item on server
	 * 
	 * @param name
	 *            filed
	 * @param value
	 *            filed
	 */
	private void saveItem(String name, String value) {
		ApiConfig newConfig = new ApiConfig(name, value);

		ApiRoleTypeConfig roleType = new ApiRoleTypeConfig();
		roleType.setRoleType(role.getType());
		roleType.setConfigs(Arrays.asList(newConfig));

		ApiServiceConfig srvConf = new ApiServiceConfig();
		srvConf.setRoleTypeConfigs(Arrays.asList(roleType));

		apiRootV1.getClustersResource().getServicesResource(cluster.getName()).updateServiceConfig(service.getName(), "", srvConf);
	}
}
