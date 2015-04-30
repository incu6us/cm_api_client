package com.intropro.main.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.cloudera.api.DataView;
import com.cloudera.api.model.ApiCluster;
import com.cloudera.api.model.ApiClusterList;
import com.cloudera.api.v1.RootResourceV1;
import com.intropro.hadoop.api.InitApiConnection;
import com.intropro.hadoop.api.clusters.services.config.MapReduceService;
import com.intropro.hadoop.api.clusters.services.config.TaskScheduler;

public class MapReduceExample {

	private static Logger LOG = Logger.getLogger(RoleCommandsExample.class);

	private static String HOST = "c-master";
	private static String USER = "admin";
	private static String PASS = "admin";
	private static String MAPREDUCESERVICE = "mapreduce";

	public static void main(String[] args) throws IOException {

		InitApiConnection connection = new InitApiConnection(HOST, USER, PASS);
		RootResourceV1 apiRootV1 = connection.getApiRootV1();

		/*
		 * List of clusters
		 */
		ApiClusterList clusters = apiRootV1.getClustersResource().readClusters(DataView.SUMMARY);
		for (ApiCluster cluster : clusters) {
			LOG.info(cluster.getName() + " - " + cluster.getVersion());
		}

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
		 * Set config (mapred_queue_names_list)
		 */
		MapReduceService mapReduceService = new MapReduceService(apiRootV1, MAPREDUCESERVICE);
		List<String> queues = new ArrayList<String>();
		queues.add("test");
		mapReduceService.changeMapredQueueNamesList(queues);

		/*
		 * Set config (mapred_jobtracker_taskScheduler)
		 */
		// MapReduceService mapReduceService = new
		// MapReduceService(apiRootV1,
		// MAPREDUCESERVICE);
		mapReduceService.changeMapredJobtrackerTaskScheduler(TaskScheduler.CapacityTaskScheduler.toString());

		/*
		 * Set config (mapred_capacity_scheduler_configuration)
		 */
		// MapReduceService mapReduceService = new
		// MapReduceService(apiRootV1,
		// MAPREDUCESERVICE);
		// List<String> queues = new ArrayList<String>();
		// queues.add("test");
		mapReduceService.changeMapredCapacitySchedulerConfiguration(queues);
	}

}
