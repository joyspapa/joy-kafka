package com.joy.kafka.monitor.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.joy.kafka.monitor.config.ConfigHelper;
import com.joy.kafka.monitor.factory.KafkaAdminClientFactory;
import com.joy.kafka.monitor.factory.KafkaConsumerFactory;
import com.joy.kafka.monitor.rest.service.RequestService;
import com.joy.kafka.monitor.rest.service.ResponseService;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Launcher;

public class KafkaMonitorRest extends AbstractVerticle {
	private static final Logger logger = LoggerFactory.getLogger(KafkaMonitorRest.class);

	public static void main(String[] args) throws Exception {
		Launcher.main(new String[] { "run", KafkaMonitorRest.class.getName() });
	}

	@Override
	public void start(Future<Void> startFuture) {
		// =================================
		// 서비스 전 처리
		preProcess();
		
		vertx.deployVerticle(new RequestService(), asyncResult -> 
		{
			if (asyncResult.succeeded()) {
				logger.info("[start] Startup tasks are now complete, RequestService is now started!");
				startFuture.complete();
			} else {
				startFuture.fail(asyncResult.cause());
			}
		});
		
		vertx.deployVerticle(new ResponseService(), getWorkerDeploymentOptions(), asyncResult -> 
		{
			if (asyncResult.succeeded()) {
				logger.info("[start] Startup tasks are now complete, ResponseService is now started!");
				startFuture.complete();
			} else {
				startFuture.fail(asyncResult.cause());
			}
		});
	}

	private DeploymentOptions getWorkerDeploymentOptions() {
		DeploymentOptions options = new DeploymentOptions().setWorker(true)
		//.setInstances(2) // matches the worker pool size below
		//.setWorkerPoolName("kafka-monitor-worker-pool")
		//.setWorkerPoolSize(2)
		;

		return options;
	}

	private void preProcess() {
		logger.info("[start] pre-processing starting...");

		try {
			logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
			logger.info("[start] Rest port 확인...");
			if (ConfigHelper.getPort() == -1) {
				logger.error("[start] Failed !, invalid port : {} ", ConfigHelper.getPort());
				System.exit(1);
			}
			logger.info("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
			logger.info("[start] pre-processing finished !\n");

		} catch (Throwable th) {
			logger.error("[start] Failed !, ", th);
			th.printStackTrace();
			System.exit(1);
		}
	}
	
	@Override
	public void stop(Future<Void> stopFuture) throws Exception {
		// If you have slow cleanup tasks to perform, you can similarly override the async stop method
		logger.info("[stop] Vertx Http server stopping...");

		//===================================
		// TODO stopping process
		KafkaConsumerFactory.closeKafkaConsumer();
		KafkaAdminClientFactory.closeAdminClient();

		super.stop(stopFuture);

		logger.info("[stop] Vertx Http server is now stopped!");
	}
}
