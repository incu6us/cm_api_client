package com.intropro.main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.cloudera.api.v1.RootResourceV1;
import com.intropro.hadoop.api.InitApiConnection;
import com.intropro.hadoop.api.clusters.services.config.MapReduceService;
import com.intropro.hadoop.api.clusters.services.config.TaskScheduler;

public class Main {

	private static Logger LOG = Logger.getLogger(Main.class);

	private static String HOST = "c-master";
	private static String USER = "admin";
	private static String PASS = "admin";
	private static String MAPREDUCESERVICE = "mapreduce";

	public static void main(String... args) throws InterruptedException, IOException {

		InitApiConnection connection = new InitApiConnection(HOST, USER, PASS);
		RootResourceV1 apiRootV1 = connection.getApiRootV1();

		/*
		 * List of clusters
		 */
		// ApiClusterList clusters =
		// apiRootV9.getClustersResource().readClusters(DataView.SUMMARY);
		// for (ApiCluster cluster : clusters) {
		// LOG.info(cluster.getName() + " - " + cluster.getVersion());
		// }

		/*
		 * Start first cluster
		 */
		// ApiCommand cmd =
		// apiRootV9.getClustersResource().startCommand(clusters.get(0).getName());
		// while (cmd.isActive()) {
		// Thread.sleep(100);
		// cmd = apiRootV9.getCommandsResource().readCommand(cmd.getId());
		// }
		//
		// LOG.info("Cluster start");
		// LOG.info(cmd.getResultMessage());

		/*
		 * Get list of services and roles
		 */
		// ClustersResourceV9 clustersResource =
		// apiRootV9.getClustersResource();
		// for (ApiCluster cluster :
		// clustersResource.readClusters(DataView.FULL)) {
		// LOG.info(cluster.getName());
		// ServicesResourceV2 servicesResource =
		// clustersResource.getServicesResource(cluster.getName());
		// for (ApiService service :
		// servicesResource.readServices(DataView.FULL)) {
		// LOG.info("\t" + service.getName() + " (service)");
		// RolesResourceV2 rolesResource =
		// servicesResource.getRolesResource(service.getName());
		// for (ApiRole role : rolesResource.readRoles()) {
		// LOG.info("\t\t" + role.getName() + " (role)");
		// }
		// }
		// }

		/*
		 * Set config (mapred_queue_names_list)
		 */
		MapReduceService mapReduceService = new MapReduceService(apiRootV1, MAPREDUCESERVICE);
		List<String> queues = new ArrayList<String>();
		queues.add("test");
		mapReduceService.changeMapredQueueNamesList(queues);

		/*
		 * Set config (mapred_jobtracker_taskScheduler)
		 */
		// MapReduceService mapReduceService = new MapReduceService(apiRootV1,
		// MAPREDUCESERVICE);
		mapReduceService.changeMapredJobtrackerTaskScheduler(TaskScheduler.CapacityTaskScheduler.toString());

		/*
		 * Set config (mapred_capacity_scheduler_configuration)
		 */
		// MapReduceService mapReduceService = new MapReduceService(apiRootV1,
		// MAPREDUCESERVICE);
		// List<String> queues = new ArrayList<String>();
		// queues.add("test");
		mapReduceService.changeMapredCapacitySchedulerConfiguration(queues);
	}
}
