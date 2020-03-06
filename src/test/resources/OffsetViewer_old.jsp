<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<%@ page import="com.obzen.elp.kafka.viewer.KafkaTopicViewer,com.obzen.elp.kafka.config.KafkaConfig" %>
<%@ page import="com.obzen.elp.kafka.viewer.vo.TopicViewerVO,com.obzen.elp.kafka.viewer.vo.TopicListVO,com.obzen.elp.kafka.viewer.vo.TopicVO"  %>
<%@ page import="org.slf4j.Logger,org.slf4j.LoggerFactory"  %>
<%@ page import="java.util.*,java.text.SimpleDateFormat,java.text.NumberFormat"  %>
<%@ page import="java.util.Comparator,java.util.Collections,java.util.HashMap"  %>

<%! 
	SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	HashMap<String, TopicViewerVO> topicViewerVOPerAddrMap = new HashMap<String, TopicViewerVO>();
  //TopicViewerVO viewerVO = null;
%>

<%
	//if(!(request.getRemoteAddr().startsWith("0"))) {
	//	System.out.println("OffsetViewer Clinet-Addr : " + request.getRemoteAddr());
  	//System.out.println("OffsetViewer RemoteAddr : " + request.getRemoteAddr() + " , RemoteHost : " + request.getRemoteHost());
	//}
	int refreshSec = 5;
 	boolean isEarliest = false;
 	int kafkaTimeout = 0;
 	String includeTopic = "";
%>

<%
	if(request.getParameter("refresh") != null) {
		refreshSec = Integer.parseInt(request.getParameter("refresh"));
	} else {
		refreshSec = 5;
	}
	
	if(request.getParameter("ip") != null) {
		KafkaConfig.brokerIp=request.getParameter("ip");
	}
	
	if(request.getParameter("port") != null) {
		KafkaConfig.brokerPort = Integer.parseInt(request.getParameter("port"));
	}
	
	if(request.getParameter("earliest") != null) {
		isEarliest = true;
	}
	
	if(request.getParameter("timeout") != null) {
		kafkaTimeout = Integer.parseInt(request.getParameter("timeout"));
		KafkaConfig.timeout = kafkaTimeout;
	}
	
	if(request.getParameter("include") != null) {
		includeTopic = request.getParameter("include");
	}
	
 %>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<meta http-equiv="refresh" content="<%= refreshSec %>" >
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
}

table.GeneratedTable td, table.GeneratedTable th {
  border-width: 1px;
  border-color: #d2d2d2;
  border-style: solid;
  padding: 3px;
}

table.GeneratedTable thead {
  background-color: #ffcc00;
}
</style>
<!-- Request Condition : refresh=3&#38;ip=192.168.10.82&#38;port=9092&#38;timeout=3000&#38;earliest=false&#38;include=topicA,topicB-->
</head>
<body>
<h4>Offset Viewer</h4>
<h6>Request Condition : refresh=3&#38;ip=192.168.10.82&#38;port=9092&#38;timeout=3000&#38;earliest=false&#38;include=topicA,topicB :: <%= df.format(System.currentTimeMillis())%> </h6>
<%
	// Condition : refresh=3&#38;brokerip=192.168.10.82&#38;earliest=false&#38;timeout=3000
	TopicViewerVO viewerVO = new TopicViewerVO();
	
	boolean isExist = false;
	for (String addr : topicViewerVOPerAddrMap.keySet()) {

		if (request.getRemoteAddr().equals(addr)) {
			viewerVO = topicViewerVOPerAddrMap.get(addr);
			isExist = true;
			break;
		}
	}
	
	if(!isExist) {
	
		viewerVO = new TopicViewerVO();
		viewerVO.addTopicListVO(KafkaTopicViewer.readAllTopicInfo(isEarliest));
		//viewerVO.addTopicListVO(new KafkaTopicViewer().readAllTopicInfo(isEarliest));
	
	} else if(viewerVO.getTopicListVO().size() == 1) {
		
		viewerVO.addTopicListVO(KafkaTopicViewer.readAllTopicInfo(isEarliest));
			
	} else if(viewerVO.getTopicListVO().size() > 1) { 
		
		//viewerVO.addTopicListVO(1, viewerVO.getTopicListVO().get(0));
		//viewerVO.addTopicListVO(0, KafkaTopicViewer.readAllTopicInfo(isEarliest));
		viewerVO.addTopicListVO(0, viewerVO.getTopicListVO().get(1));
		viewerVO.addTopicListVO(1, KafkaTopicViewer.readAllTopicInfo(isEarliest));
			
	}
	
	topicViewerVOPerAddrMap.put(request.getRemoteAddr(), viewerVO);
	
	HashMap<String, Long> beforeLastOffset = new HashMap<String, Long>();
	int viewerCount = 0; // max 2
	
	
	String time = null;
	//for (TopicListVO topicListVO : new ArrayList<>(viewerVO.getTopicListVO())) {
	for (TopicListVO topicListVO : viewerVO.getTopicListVO()) {
		time = df.format(topicListVO.getViewDate());
		//System.out.println("OffsetViewer Clinet-Addr : " + request.getRemoteAddr() + ", time : " + time);
 %>  
<table class="GeneratedTable">
  <tbody>
		<tr>
      <td colspan="5"><%= time %></td>
    </tr>
  	<tr>
      <td align="center">Topic</td>

      <td align="center">Partition</td>

      <td align="center">Offset</td>

      <td align="center">Leader</td>
      
      <td align="center">Replicas</td>
    </tr>
<%
		// set request parameter includeTopic
		String[] tempIncludeTopic = null;
		if(includeTopic.length() > 0) {
			tempIncludeTopic = includeTopic.split(",");
		}
		
		//================================================
		// sort

		Comparator<TopicVO> comparator = new Comparator<TopicVO>() {
			
			@Override
			public int compare(TopicVO before, TopicVO after) {
				if(before == null || after == null) {
					return 0;
				}
					
				// asc
				return (before.getTopicName()).compareTo(after.getTopicName());
					
				// desc
				//return (after.getTopicName()).compareTo(before.getTopicName());
			}
		};
		
		Collections.sort(topicListVO.getTopicListVO(), comparator);

		//================================================
		
		
		for (TopicVO topicVO : topicListVO.getTopicListVO()) {
			boolean isContinue = true;
			if(tempIncludeTopic != null) {
				for(String temp : tempIncludeTopic) {
					if(topicVO.getTopicName().toUpperCase().contains(temp.toUpperCase())) {
						isContinue = false;
					}
				}
			} else {
				isContinue = false;
			}
			
			if(isContinue) {
				continue;
			}
			
			if(viewerVO.getTopicListVO().size() == 2 && viewerCount == 0) {
				beforeLastOffset.put(topicVO.getTopicName() + ":" + topicVO.getPartitionId() , topicVO.getLastOffset());
			}
 %>  
    <tr>
      <td><%= topicVO.getTopicName() %></td>
      <td align="center"><%= topicVO.getPartitionId() %></td>
<%
			if(viewerVO.getTopicListVO().size() == 2 && viewerCount == 1 && beforeLastOffset.get(topicVO.getTopicName() + ":" + topicVO.getPartitionId()) != null
						&& beforeLastOffset.get(topicVO.getTopicName() + ":" + topicVO.getPartitionId()) < topicVO.getLastOffset()) {
%>	
      <td align="right"><font color="blue"><%= NumberFormat.getNumberInstance().format(topicVO.getLastOffset()) %></font></td>
<%
			} else {
%>		
			<td align="right"><%= NumberFormat.getNumberInstance().format(topicVO.getLastOffset()) %></td>
<%
			}
%>
      <td align="right"><%= topicVO.getLeaderBroker() %></td>
      <td align="right"><%= topicVO.getReplicas() %></td>
    </tr>
<%
		}
%>    
  </tbody>
</table>

<P>
<%
		viewerCount ++;

	} // for (TopicListVO topicListVO : viewerVO.getTopicListVO()) end
%>  

<%
	beforeLastOffset.clear();
 //KafkaTopicViewer.getInstance().print(viewerVO) 
%>
</body>
</html>