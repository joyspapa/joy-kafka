package com.joy.kafka.monitor.rest.service;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpHeaders;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.CorsHandler;

public class RequestService extends AbstractVerticle {
	private static final Logger logger = LoggerFactory.getLogger(RequestService.class);
	private final int listenPort = 8081;
	
	@Override
	public void start() {
		Router router = Router.router(vertx);
		//router.route().handler(BodyHandler.create()));
		router.route().handler(CorsHandler.create("*").allowedHeaders(getAllowedHeaders()));
		
		//process(router);
		process(router);
		
		vertx.createHttpServer().requestHandler(router::accept).listen(listenPort);

		logger.info(">>> started with port : " + listenPort);
	}
	
	private void process(Router router) {
		router.get("/monitor/topic").handler(request -> {
			logger.info("Received a http request / !");
			vertx.eventBus().<String> send("service-to-response", (request.getBody() == null)? "":request.getBody().toString(), asyncResult -> {
				// TODO 결과처
				//if (asyncResult.succeeded()) {
					logger.info("\n" + asyncResult.result().body());
					request.response().putHeader(HttpHeaders.CONTENT_TYPE, "text/plain; charset=utf-8")
														.setStatusCode(200)
														.end(asyncResult.result().body());
					
					request.response().close();
				//}
			});
		});
	}
	
	private void process2(Router router) {
		router.get("/monitor/topic").handler(request -> {
			logger.info("Received a http request / !");
			request.response().putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
														.setStatusCode(200).setChunked(true)
														.end("test-response", "UTF-8");
		});
	}
	
	private Set<String> getAllowedHeaders() {
		Set<String> allowedHeaders = new HashSet<>();
	    allowedHeaders.add("Access-Control-Allow-Origin");
	    allowedHeaders.add("origin");
	    allowedHeaders.add("Content-Type");
	    allowedHeaders.add("accept");
	    
	    return allowedHeaders;
	}
}
