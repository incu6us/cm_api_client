package com.intropro.hadoop.api.clusters.services.config;

public enum TaskScheduler {

	CapacityTaskScheduler {
		public String toString() {
			return "org.apache.hadoop.mapred.CapacityTaskScheduler";
		}
	},
	
	JobQueueTaskScheduler{
		public String toString() {
			return "org.apache.hadoop.mapred.JobQueueTaskScheduler";
		}
	},
	
	FairScheduler{
		public String toString() {
			return "org.apache.hadoop.mapred.FairScheduler";
		}
	}

}
