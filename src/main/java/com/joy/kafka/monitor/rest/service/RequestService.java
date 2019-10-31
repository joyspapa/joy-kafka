package com.joy.kafka.monitor.rest.service;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.joy.kafka.monitor.config.AdminCommand;
import com.joy.kafka.monitor.config.ConfigHelper;
import com.joy.kafka.monitor.config.Constant;
import com.joy.kafka.monitor.util.ElapsedTime;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.CorsHandler;

public class RequestService extends AbstractVerticle {
	private static final Logger logger = LoggerFactory.getLogger(RequestService.class);
	
	@Override
	public void start() {
		Router router = Router.router(vertx);
		//router.route().handler(BodyHandler.create()));
		router.route().handler(CorsHandler.create("*").allowedHeaders(getAllowedHeaders()));

		monitorService(router);
		adminService(router);

		vertx.createHttpServer().requestHandler(router::accept).listen(ConfigHelper.getPort());
		logger.info(">>> started with port : " + ConfigHelper.getPort());
	}

	private void monitorService(Router router) {
		router.get("/kafka/monitor/:clientip/:viewtype/:id").handler(request -> {

			// =================================
			// 서비스 전 처리
			ElapsedTime.setUp(String.format("%s::%s", request.request().getParam("clientip"), Constant.bus_kafka_monitor));

			vertx.eventBus().<String> send(Constant.bus_kafka_monitor,
					new JsonObject().put("clientIp", request.request().getParam("clientip"))
							.put("viewType", request.request().getParam("viewtype"))
							.put("id", request.request().getParam("id")),

					asyncResult -> {

						try {
							if (asyncResult.succeeded()) {
								//logger.info("response : \n" + asyncResult.result().body());
								response(request, asyncResult.result().body());
							} else {
								JsonObject replyObject = new JsonObject();
								replyObject.put("success", false).put("errormessage", asyncResult.cause());
								response(request, replyObject.toString());
							}
						} catch (Throwable th) {
							logger.warn("{}::{} error : ",request.request().getParam("clientip"), Constant.bus_kafka_monitor, th);
							
							JsonObject replyObject = new JsonObject();
							replyObject.put("success", false).put("errormessage", th.getMessage());
							response(request, replyObject.toString());
						}
						
						// =================================
						// 서비스 후 처리
						ElapsedTime.tearDown();
					});
		});
	}

	private void adminService(Router router) {
		router.get("/kafka/admin/:clientip/list").handler(request -> {

			// =================================
			// 서비스 전 처리
			ElapsedTime.setUp(String.format("%s::list::%s", request.request().getParam("clientip"), Constant.bus_kafka_admin));

			vertx.eventBus().<String> send(Constant.bus_kafka_admin,
					new JsonObject().put("clientIp", request.request().getParam("clientip"))
							.put("command", AdminCommand.List.getName()),

					asyncResult -> {

						try {
							if (asyncResult.succeeded()) {
								//logger.info("response : \n" + asyncResult.result().body());
								response(request, asyncResult.result().body());
							} else {
								JsonObject replyObject = new JsonObject();
								replyObject.put("success", false).put("errormessage", asyncResult.cause());
								response(request, replyObject.toString());
							}							
							logger.warn("[{}::list::{}] Retreived topic list : ",request.request().getParam("clientip"), Constant.bus_kafka_admin);
						} catch (Throwable th) {
							logger.warn("{}::list::{} error : ",request.request().getParam("clientip"), Constant.bus_kafka_admin, th);
							
							JsonObject replyObject = new JsonObject();
							replyObject.put("success", false).put("errormessage", th.getMessage());
							response(request, replyObject.toString());
						}
						
						// =================================
						// 서비스 후 처리
						ElapsedTime.tearDown();
					});
		});
		
		router.get("/kafka/admin/:clientip/create/:topicName/:numPartitions/:replicaFactor").handler(request -> {

			// =================================
			// 서비스 전 처리
			ElapsedTime.setUp(String.format("%s::create::%s", request.request().getParam("clientip"), Constant.bus_kafka_admin));

			vertx.eventBus().<String> send(Constant.bus_kafka_admin,
					new JsonObject().put("clientIp", request.request().getParam("clientip"))
							.put("command", AdminCommand.Create.getName())
							.put("topicName", request.request().getParam("topicName"))
							.put("numPartitions", request.request().getParam("numPartitions"))
							.put("replicaFactor", request.request().getParam("replicaFactor")),

					asyncResult -> {

						try {
							if (asyncResult.succeeded()) {
								//logger.info("response : \n" + asyncResult.result().body());
								response(request, asyncResult.result().body());
							} else {
								JsonObject replyObject = new JsonObject();
								replyObject.put("success", false).put("errormessage", asyncResult.cause());
								response(request, replyObject.toString());
							}							
							logger.warn("[{}::create::{}] Created topic:{}  : ",request.request().getParam("clientip"), Constant.bus_kafka_admin, request.request().getParam("topicName"));
						} catch (Throwable th) {
							logger.warn("{}::create::{} error : ",request.request().getParam("clientip"), Constant.bus_kafka_admin, th);
							
							JsonObject replyObject = new JsonObject();
							replyObject.put("success", false).put("errormessage", th.getMessage());
							response(request, replyObject.toString());
						}
						
						// =================================
						// 서비스 후 처리
						ElapsedTime.tearDown();
					});
		});
		
		router.get("/kafka/admin/:clientip/delete/:topicName").handler(request -> {

			// =================================
			// 서비스 전 처리
			ElapsedTime.setUp(String.format("%s::delete::%s", request.request().getParam("clientip"), Constant.bus_kafka_admin));

			vertx.eventBus().<String> send(Constant.bus_kafka_admin,
					new JsonObject().put("clientIp", request.request().getParam("clientip"))
							.put("command", AdminCommand.Delete.getName())
							.put("topicName", request.request().getParam("topicName")),

					asyncResult -> {

						try {
							if (asyncResult.succeeded()) {
								//logger.info("response : \n" + asyncResult.result().body());
								response(request, asyncResult.result().body());
							} else {
								JsonObject replyObject = new JsonObject();
								replyObject.put("success", false).put("errormessage", asyncResult.cause());
								response(request, replyObject.toString());
							}
							logger.warn("[{}::delete::{}] Deleted topic:{}  : ",request.request().getParam("clientip"), Constant.bus_kafka_admin, request.request().getParam("topicName"));
						} catch (Throwable th) {
							logger.warn("{}::delete::{} error : ",request.request().getParam("clientip"), Constant.bus_kafka_admin, th);
							
							JsonObject replyObject = new JsonObject();
							replyObject.put("success", false).put("errormessage", th.getMessage());
							response(request, replyObject.toString());
						}
						// =================================
						// 서비스 후 처리
						ElapsedTime.tearDown();
					});			
		});
	}

	private void response(RoutingContext request, String responseData) {
		request.response().putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
		 	   .setStatusCode(200)
		 	   .end(responseData, "UTF-8");
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
