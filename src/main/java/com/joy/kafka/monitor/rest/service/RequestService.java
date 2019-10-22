package com.joy.kafka.monitor.rest.service;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.CorsHandler;

public class RequestService extends AbstractVerticle {
	private static final Logger logger = LoggerFactory.getLogger(RequestService.class);
	private final int listenPort = 9081;

	@Override
	public void start() {
		Router router = Router.router(vertx);
		//router.route().handler(BodyHandler.create()));
		router.route().handler(CorsHandler.create("*").allowedHeaders(getAllowedHeaders()));

		process(router);

		vertx.createHttpServer().requestHandler(router::accept).listen(listenPort);
		logger.info(">>> started with port : " + listenPort);
	}

	private void process(Router router) {
		router.get("/kafka/monitor/:clientip/:viewtype/:id").handler(request -> {
			long startTime = System.currentTimeMillis();

			vertx.eventBus().<String> send(Constant.bus_response_type_json,
					new JsonObject().put("viewtype", request.request().getParam("viewtype")).put("id",
							request.request().getParam("id")),
					asyncResult -> {
						HttpServerResponse res = request.response();
						try {
							if (asyncResult.succeeded()) {
								//logger.info("response : \n" + asyncResult.result().body());
								res.putHeader(HttpHeaders.CONTENT_TYPE, "application/json");
								res.setStatusCode(200).end(asyncResult.result().body(), "UTF-8");
							} else {
								JsonObject replyObject = new JsonObject();
								replyObject.put("success", false).put("errormessage", asyncResult.cause());

								res.setStatusCode(501).end(replyObject.toString());
							}

							logger.info("[{}] elapsed time : {} ms", request.request().getParam("clientip"),
									System.currentTimeMillis() - startTime);
						} catch (Throwable th) {
							JsonObject replyObject = new JsonObject();
							replyObject.put("success", false).put("errormessage", th.getMessage());
							res.setStatusCode(501).end(replyObject.toString());
						}
					});

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
