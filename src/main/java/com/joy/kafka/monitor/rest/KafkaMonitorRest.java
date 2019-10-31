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
import io.vertx.core.Launcher;

public class KafkaMonitorRest extends AbstractVerticle {
	private static final Logger logger = LoggerFactory.getLogger(KafkaMonitorRest.class);

	public static void main(String[] args) throws Exception {
		Launcher.main(new String[] { "run", KafkaMonitorRest.class.getName() });
	}

	@Override
	public void start() {
		// =================================
		// 서비스 전 처리
		preProcess();
		
		vertx.deployVerticle(new RequestService());
		
		vertx.deployVerticle(new ResponseService(), getWorkerDeploymentOptions(), stringAsyncResult ->
        {
            logger.info("ResponseService deployed.");

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    			KafkaConsumerFactory.closeKafkaConsumer();
    			KafkaAdminClientFactory.closeAdminClient();
    		}));
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
		logger.info("[KafkaMonitor-rest::start] pre-processing starting...");

		try {
			logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
			logger.info("[KafkaMonitor-rest::start] Rest port 확인...");
			if (ConfigHelper.getPort() == -1) {
				logger.error("[KafkaMonitor-rest::start] Failed !, invalid port : {} ", ConfigHelper.getPort());
				System.exit(1);
			}
			logger.info("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
			logger.info("[KafkaMonitor-rest::start] pre-processing finished !");

		} catch (Throwable th) {
			logger.error("[KafkaMonitor-rest::start] Failed !, ", th);
			th.printStackTrace();
			System.exit(1);
		}

	}
}
