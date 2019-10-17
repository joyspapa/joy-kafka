package com.joy.kafka.monitor.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.joy.kafka.monitor.rest.service.RequestService;
import com.joy.kafka.monitor.rest.service.ResponseService;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Launcher;

public class KafkaMonitorRest extends AbstractVerticle {
	private static final Logger logger = LoggerFactory.getLogger(KafkaMonitorRest.class);
	
	public static void main(String[] args) throws Exception {
		Launcher.main(new String[] { "run", KafkaMonitorRest.class.getName() });
	}
	
	@Override
	public void start() {
		vertx.deployVerticle(new RequestService());
		vertx.deployVerticle(new ResponseService(), getWorkerDeploymentOptions());
	}
	
	private DeploymentOptions getWorkerDeploymentOptions() {
		DeploymentOptions options = new DeploymentOptions()
				  .setWorker(true)
				  .setInstances(1) // matches the worker pool size below
				  .setWorkerPoolName("kafka-monitor-worker-pool")
				  .setWorkerPoolSize(1);
		
		return options;
	}
}
