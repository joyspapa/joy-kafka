<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<%@ page import="java.util.*"%>
<%@ page import="org.slf4j.Logger,org.slf4j.LoggerFactory"%>
<%@ page import="java.net.*,java.io.*,com.fasterxml.jackson.databind.*,java.text.*"%>

<%!// 전역 변수 선언
	Logger logger = LoggerFactory.getLogger(getClass());

    String baseUrl = "";
    String synapseURL = "http://192.168.10.171:9081/kafka/monitor/";
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
%>

<%
	//if(!(request.getRemoteAddr().startsWith("0"))) {
	//	logger.warn("OffsetViewer Clinet-Addr : " + request.getLocalAddr());
	//	logger.warn("OffsetViewer RemoteAddr : " + request.getRemoteAddr() + " , RemoteHost : " + request.getRemoteHost());
	//}
    
	StringBuilder sbBaseUrl = new StringBuilder();
	sbBaseUrl.append("http://");
	sbBaseUrl.append(InetAddress.getLocalHost().getHostAddress());
	sbBaseUrl.append(":8080/logplanet-ui-admin/OffsetViewerTest.jsp");
	//sbBaseUrl.append(request.getRemoteAddr());
    
	//baseUrl = "http://" + InetAddress.getLocalHost().getHostAddress() +":8080/logplanet-ui-admin/Offset.jsp?clientip="+request.getRemoteAddr();

	int refreshSec = 2;
	boolean isEarliest = false;
	int kafkaTimeout = 0;
	String includeTopic = "";
	
	String jsonString = "{\"success\": false, \"errormessage\": \"초기 화면\" }";
	String viewTypeParam = request.getParameter("viewtype");
	String idParam = request.getParameter("id");
	
	String refreshParam = request.getParameter("refresh");
	if(refreshParam != null && refreshParam.length() > 0) {
		refreshSec = Integer.parseInt(refreshParam);
	}
	sbBaseUrl.append("?refresh=");
	sbBaseUrl.append(refreshSec);
	baseUrl = sbBaseUrl.toString();
%>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<%
	if(viewTypeParam != null) {
		// Rest 호출
		jsonString = call(request.getRemoteAddr(), viewTypeParam, idParam);
%>
<meta http-equiv="refresh" content="<%=refreshSec%>">
<%
	}
%>
<title>Offset Viewer of KAFKA Topic</title>
<!-- CSS Code: Place this code in the document's head (between the 'head' tags) -->
<style>
table.GeneratedTable {
	width: 50%;
	background-color: #ffffff;
	border-collapse: collapse;
	border-width: 1px;
	border-color: #d2d2d2;
	border-style: solid;
	color: #000000;
	font-size: small;
}

table.GeneratedTable td, table.GeneratedTable th {
	border-width: 1px;
	border-color: #d2d2d2;
	border-style: solid;
	padding: 2px;
}

table.GeneratedTable thead {
	background-color: #ffcc00;
}

table.GeneratedTable2 {
	width: 80%;
	background-color: #ffffff;
	border-collapse: collapse;
	border-width: 1px;
	border-color: #d2d2d2;
	border-style: solid;
	color: #000000;
	font-size: small;
}

table.GeneratedTable2 td, table.GeneratedTable2 th {
	border-width: 1px;
	border-color: #d2d2d2;
	border-style: solid;
	padding: 2px;
}

table.GeneratedTable2 thead {
	background-color: #ffcc00;
}

table.GeneratedTable3 {
	width: 30%;
	background-color: #ffffff;
	border-collapse: collapse;
	border-width: 1px;
	border-color: #d2d2d2;
	border-style: solid;
	color: #000000;
	font-size: small;
}

table.GeneratedTable3 td, table.GeneratedTable3 th {
	border-width: 1px;
	border-color: #d2d2d2;
	border-style: solid;
	padding: 2px;
}

table.GeneratedTable3 thead {
	background-color: #ffcc00;
}
</style>
<!-- Request Condition : refresh=3&#38;ip=192.168.10.82&#38;port=9092&#38;timeout=3000&#38;earliest=false&#38;include=topicA,topicB-->
</head>
<body>
	<h4>
	<a href="<%=baseUrl%>" style="text-decoration:none">Offset Viewer</a>
	</h4>
	
	
	<h5>
	<a href="<%=baseUrl%>&viewtype=topic" style="text-decoration:none">Topic 별 현황</a>
	&nbsp;&nbsp;&nbsp;&nbsp;
	<a href="<%=baseUrl%>&viewtype=consumer" style="text-decoration:none">ConsumerGroup 별 현황</a>
	(<a href="<%=baseUrl%>&viewtype=consumerAll" style="text-decoration:none">ALL</a>)
	&nbsp;&nbsp;&nbsp;&nbsp;
	<a href="<%=baseUrl%>&viewtype=deploy" style="text-decoration:none">Deploy 별 현황</a>
	</h5>
		<%
			JsonNode jsonNode = new ObjectMapper().readTree(jsonString);
			if (jsonNode.get("success").asBoolean()) {

				JsonNode hasReportNode = jsonNode.get("report");
				if(hasReportNode == null) {
					JsonNode resultsArrayNode = jsonNode.path("results");
					int resultsCount = resultsArrayNode.size();

					if (resultsCount > 0) {

						boolean hasGroupID = true;
						String checkDeployName = resultsArrayNode.get(0).get("deployName").asText();
						String checkGroupID = resultsArrayNode.get(0).get("groupID").asText();

						if (checkGroupID == null || checkGroupID.length() == 0) {
						hasGroupID = false;
						}

						//logger.warn("jsonString : " + jsonString);


						if (checkDeployName.length() != 0) {
%>
	<p>
	<table class="GeneratedTable3">
		<tbody>
			<tr>
				<td><%=resultsArrayNode.get(0).get("createDT").asText()%></td>
			</tr>
<%
						String bgcolor = "";
						for (int resultPos = 0; resultPos < resultsCount; resultPos++) {
							bgcolor = (resultPos%2 == 1)?	"bgcolor='#F0F0F0'":"";
							checkDeployName = resultsArrayNode.get(resultPos).get("deployName").asText();
%>
			<tr <%=bgcolor%>>
				<td>
					<a href="<%=baseUrl%>&viewtype=deploy&id=<%=checkDeployName%>" style="text-decoration:none;color:black;">
						<%=checkDeployName%>
					</a>
				</td>
			</tr>
			<%
						} // for end
			%>
		</tbody>
	</table>
<%						
					} else if (!hasGroupID) {
						// Topic offset
		%>
	<p>
	<table class="GeneratedTable">
		<tbody>
			<tr>
				<td colspan="5"><%=resultsArrayNode.get(0).get("createDT").asText()%></td>
			</tr>
			<tr>
				<td align="center">Topic</td>
				<td align="center">Partition</td>
				<td align="center">Offset</td>
				<td align="center">Leader</td>
				<td align="center">Replicas</td>
			</tr>
			<%
				String bgcolor = "";
				JsonNode offsetListNode = null;
				for (int resultPos = 0; resultPos < resultsCount; resultPos++) {
					offsetListNode = resultsArrayNode.get(resultPos).path("offsetList");
					int offsetListNodeCount = offsetListNode.size();
					bgcolor = (resultPos%2 == 1)?	"bgcolor='#F0F0F0'":"";

					String topicName = null;
					for (int offsetListPos = 0; offsetListPos < offsetListNodeCount; offsetListPos++) {
						topicName = resultsArrayNode.get(resultPos).get("topic").asText();
			%>
			<tr <%=bgcolor%>>
				<td>
					<%  if(offsetListPos == 0) {%>
					<a href="<%=baseUrl%>&viewtype=topic&id=<%=topicName%>" style="text-decoration:none;color:black;">
						<%=topicName%>
					</a>
					<%  } %>
				</td>
				<td align="center"><%=offsetListNode.get(offsetListPos).get("partition").asInt()%></td>
				<td align="right"><%=String.format("%,10d", offsetListNode.get(offsetListPos).get("endOffset").asLong())%></td>
				<td align="center"><%=offsetListNode.get(offsetListPos).get("leader").asText()%></td>
				<td align="center"><%=offsetListNode.get(offsetListPos).get("replicas").asText()%></td>
			</tr>
			<%
					} // for end
				} // for end
			%>
		</tbody>
	</table>
	<%
					} else { // if(!hasGroupID)
					// Consumer offset
	%>

	<table class="GeneratedTable2">
		<tbody>
			<tr>
				<td colspan="9"><%=resultsArrayNode.get(0).get("createDT").asText()%></td>
			</tr>
			<tr>
				<td align="center">Topic</td>
				<td align="center">Consumer</td>
				<td align="center">Partition</td>
				<td align="center">Host</td>
				<td align="center">EndOffset</td>
				<td align="center">CommittedOffset</td>
				<td align="center">Lag</td>
				<td align="center">Leader</td>
				<td align="center">Replicas</td>
			</tr>
			<%
				String bgcolor = "";
				JsonNode offsetListNode = null;
				for (int resultPos = 0; resultPos < resultsCount; resultPos++) {
					offsetListNode = resultsArrayNode.get(resultPos).path("offsetList");
					int offsetListNodeCount = offsetListNode.size();
					bgcolor = (resultPos%2 == 1)?	"bgcolor='#F0F0F0'":"";
					
					String groupID = null;
					for (int offsetListPos = 0; offsetListPos < offsetListNodeCount; offsetListPos++) {
						long lag = offsetListNode.get(offsetListPos).get("lag").asLong();
						String lagString = "";
						String tdColor = "";
						if(lag < 0) {
							tdColor = "style='color:#909090'";
							lagString = "STOPPED";
						} else {
							lagString = String.format("%,10d", offsetListNode.get(offsetListPos).get("lag").asLong());
						}
									
						groupID = resultsArrayNode.get(resultPos).get("groupID").asText();
			%>
			<tr <%=bgcolor%>>
				<td <%=tdColor%> ><%=(offsetListPos == 0)? resultsArrayNode.get(resultPos).get("topic").asText():""%></td>
				<td <%=tdColor%> >
					<%  if(tdColor.length() == 0) {%>
					<a href="<%=baseUrl%>&viewtype=report&id=<%=groupID%>" style="text-decoration:none;color:black;">
						<%=groupID%>
					</a>
					<%  } else { %>
						<%=groupID%>
					<%  } %>
				</td>
				<td align="center" <%=tdColor%> ><%=offsetListNode.get(offsetListPos).get("partition").asInt()%></td>
				<td align="center" <%=tdColor%> ><%=offsetListNode.get(offsetListPos).get("host").asText()%></td>
				<td align="right" <%=tdColor%> ><%=String.format("%,10d", offsetListNode.get(offsetListPos).get("endOffset").asLong())%></td>
				<td align="right" <%=tdColor%> ><%=String.format("%,10d", offsetListNode.get(offsetListPos).get("committedOffset").asLong())%></td>
				<td align="right" <%=tdColor%> ><%=lagString%></td>
				<td align="center" <%=tdColor%> ><%=offsetListNode.get(offsetListPos).get("leader").asText()%></td>
				<td align="center" <%=tdColor%> ><%=offsetListNode.get(offsetListPos).get("replicas").asText()%></td>
			</tr>
			<%
					} // for end
				} // for end
			%>
		</tbody>
	</table>

	<%
						} // else if(!hasGroupID)

					} else { // if (resultsCount > 0)
						out.write("<p>" + jsonString);
						//logger.warn("Result is 0 : " + jsonString);
					}
				} else { // if(hasReportNode != null)
%>					
	<table class="GeneratedTable2">
		<tbody>
		    <tr>
				<td colspan="12">Consumer Lag &nbsp;&nbsp;[ <%= df.format(System.currentTimeMillis())%> ]</td>
			</tr>
			<tr>
				<td width="36%" align="center">Consumer</td>
				<td width="4%" align="center">Partition</td>
				<td width="6%" align="center">T</td>
				<td width="6%" align="center">T+<%= refreshSec %></td>
				<td width="6%" align="center">T+<%= refreshSec*2 %></td>
				<td width="6%" align="center">T+<%= refreshSec*3 %></td>
				<td width="6%" align="center">T+<%= refreshSec*4 %></td>
				<td width="6%" align="center">T+<%= refreshSec*5 %></td>
				<td width="6%" align="center">T+<%= refreshSec*6 %></td>
				<td width="6%" align="center">T+<%= refreshSec*7 %></td>
				<td width="6%" align="center">T+<%= refreshSec*8 %></td>
				<td width="6%" align="center">T+<%= refreshSec*9 %></td>
			</tr>					
<% 
					JsonNode resultsArrayNode = jsonNode.path("results");
					int resultsCount = resultsArrayNode.size();

					if (resultsCount > 0) {
						JsonNode offsetListNode = null;
						String bgcolor = "";
						for (int resultPos = 0; resultPos < resultsCount; resultPos++) {
							String tdColor = "";
							bgcolor = (resultPos%2 == 1)?	"bgcolor='#F0F0F0'":"";
						
							
%>

			<tr <%=bgcolor%>>
				<td <%=tdColor%> ><%=resultsArrayNode.get(resultPos).get("groupID").asText()%></td>
				<td align="center" <%=tdColor%> ><%=resultsArrayNode.get(resultPos).get("partition").asInt()%></td>
<%
							offsetListNode = resultsArrayNode.get(resultPos).path("offsets");
							int offsetListNodeCount = offsetListNode.size();
							
							String lag = null;
							for (int offsetListPos = 0; offsetListPos < 10; offsetListPos++) {
								if(offsetListPos < offsetListNodeCount) {
									lag = String.format("%,10d", offsetListNode.get(offsetListPos).get("lag").asLong());
								} else {
									lag = "-";
								}
%>								
				<td align="right" <%=tdColor%> ><%=lag %></td>
<%								
							}
%>
			</tr>	
<%								
						}
					}
%>			
		</tbody>
	</table>					
					
					
<%					
				}
			} else {
				if (viewTypeParam != null) {
					out.write("<p>" + jsonString);
				}
				//logger.warn("Error : " + jsonString);
			} // if (jsonNode.get("success").asBoolean())
	%>
	<P>
</body>
</html>

<%!// 메소드 정의
	public String makeCallUrl(String clientid, String viewType, String id) {
		StringBuilder urlString = new StringBuilder();
		urlString.append(synapseURL);
		urlString.append(clientid);
		urlString.append("/");
		urlString.append(viewType);
		if(id != null) {
			urlString.append("/");
			urlString.append(id);
		}
		logger.warn("makeCallUrl={}", urlString.toString());
		return urlString.toString();
	}
%>

<%!// 메소드 정의
	public String call(String clientid, String viewType, String id) {
		String responseBody = "";
		HttpURLConnection connection = null;
		try {
			
			URL url = new URL(makeCallUrl(clientid, viewType, (id==null)? "none":id));
			connection = (HttpURLConnection) url.openConnection();
			connection.setConnectTimeout(2000);
			connection.setRequestMethod("GET");

			int code = connection.getResponseCode();

			try (BufferedReader input = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
				String line;
				StringBuffer buffer = new StringBuffer();
				while ((line = input.readLine()) != null) {
					buffer.append(line);
				}
				responseBody = buffer.toString();
			}

			if (logger.isInfoEnabled()) {
				logger.debug("[call] jsonString : \n" + responseBody);
			}

			//availableConsumerList.add(consumerHost);

		} catch (Exception ex) {
			logger.warn("[call] HttpURLConnection error : " + ex.toString());
			responseBody = "{\"success\": false, \"errormessage\": \"" + ex.toString() + "\" }";
		} finally {
			if (connection != null) {
				try {
					connection.disconnect();
				} catch (Exception e) {
					logger.warn("[call] HttpURLConnection.close(), error=" + e.toString());
				}
			}
		}
		
		return responseBody;
	}
%>