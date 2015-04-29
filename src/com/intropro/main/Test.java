package com.intropro.main;

public class Test {

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

}
