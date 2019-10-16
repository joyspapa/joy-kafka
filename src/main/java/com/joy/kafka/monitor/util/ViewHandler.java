package com.joy.kafka.monitor.util;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.joy.kafka.monitor.handler.vo.ConsumerGroupVO;
import com.joy.kafka.monitor.handler.vo.OffsetVO;

public class ViewHandler {
	private static final Logger logger = LoggerFactory.getLogger(ViewHandler.class);

	private static final String CORNER = "+";
	private static final String DELIMITER = "|";
	private static final String LINE = "-";
	private static final String NEWLINE = "\n";

	private static final ViewColumnType[] default_showConsumerOffset = { ViewColumnType.TOPIC, ViewColumnType.GROUP,
			ViewColumnType.PARTITION, ViewColumnType.HOST, ViewColumnType.END_OFFSET, ViewColumnType.COMMITTED_OFFSET,
			ViewColumnType.LAG, ViewColumnType.LEADER, ViewColumnType.REPLICAS };

	private static final ViewColumnType[] default_showTopicOffset = { ViewColumnType.TOPIC, ViewColumnType.PARTITION,
			ViewColumnType.END_OFFSET, ViewColumnType.LEADER, ViewColumnType.REPLICAS };

	public static String showByConsumerOffset(ConsumerGroupVO consumerOffset) {
		StringBuilder show = new StringBuilder();
		show.append(head(default_showConsumerOffset, consumerOffset.getCreateDT()));
		show.append(printBody(default_showConsumerOffset, consumerOffset));

		return show.toString();
	}

	public static String showByConsumerOffsetList(List<ConsumerGroupVO> consumerOffsetList) {
		StringBuilder show = new StringBuilder();
		show.append(head(default_showConsumerOffset, (!consumerOffsetList.isEmpty())? consumerOffsetList.get(0).getCreateDT():null));

		for (ConsumerGroupVO consumerGroup : consumerOffsetList) {
			show.append(printBody(default_showConsumerOffset, consumerGroup));
		}

		return show.toString();
	}

	public static String showByTopicOffset(ConsumerGroupVO topicOffset) {
		StringBuilder show = new StringBuilder();
		show.append(head(default_showTopicOffset, topicOffset.getCreateDT()));
		show.append(printBody(default_showTopicOffset, topicOffset));

		return show.toString();
	}
	
	public static String showByTopicOffsetList(List<ConsumerGroupVO> topicOffsetList) {
		StringBuilder show = new StringBuilder();
		show.append(head(default_showTopicOffset, (!topicOffsetList.isEmpty())? topicOffsetList.get(0).getCreateDT():null));
		
		for (ConsumerGroupVO topicOffsetVO : topicOffsetList) {
			show.append(printBody(default_showTopicOffset, topicOffsetVO));
		}
		
		return show.toString();
	}
	
	public static String showList(ViewColumnType[] showColumn, List<String> list) {
		StringBuilder show = new StringBuilder();

		show.append(head(showColumn));
		show.append(printBody(showColumn, list));

		return show.toString();
	}

	private static String head(ViewColumnType[] showColumn) {
		return head(showColumn, null);
	}

	private static String head(ViewColumnType[] showColumn, String dateTime) {
		StringBuilder head = new StringBuilder().append(printLine(showColumn));
		if(dateTime != null) {
			head.append(printDateTime(showColumn, dateTime)).append(printLine(showColumn));
		}
		head.append(printTitle(showColumn)).append(printLine(showColumn));
		
		return head.toString();
	}
	
	private static String printTitle(ViewColumnType[] showColumn) {
		StringBuilder sb = new StringBuilder();
		sb.append(DELIMITER);
		for (ViewColumnType col : showColumn) {
			sb.append(centerString(col.colLength(), col.colName())).append(DELIMITER);
		}
		sb.append(NEWLINE);

		return sb.toString();
	}

	private static String printDateTime(ViewColumnType[] showColumn, String dateTime) {
		StringBuilder sb = new StringBuilder();
		sb.append(DELIMITER);
		int count = 0;
		for (ViewColumnType col : showColumn) {
			if(count == 0) {
				sb.append(String.format(stringFormat(col.colLength()), dateTime));
			} else {
				sb.append(String.format(stringFormat(col.colLength() + 1), " "));
			}
			count++;
		}
		sb.append(DELIMITER);
		
		sb.append(NEWLINE);

		return sb.toString();
	}
	
	private static String printBody(ViewColumnType[] showColumn, ConsumerGroupVO consumerGroup) {
		StringBuilder sb = new StringBuilder();
		int counsumerCnt = 0;
		for (OffsetVO offset : consumerGroup.getOffsetList()) {
			sb.append(DELIMITER);
			
			for(ViewColumnType col : showColumn) {
				switch(col) {
					case TOPIC : 
						if (counsumerCnt > 0) {
							sb.append(String.format(stringFormat(ViewColumnType.TOPIC.colLength()), "")).append(DELIMITER);
						} else {
							sb.append(String.format(stringFormat(ViewColumnType.TOPIC.colLength()), consumerGroup.getTopic()))
									.append(DELIMITER);
						}
						break;
					case GROUP : 
						sb.append(String.format(stringFormat(ViewColumnType.GROUP.colLength()), consumerGroup.getGroupID()))
						.append(DELIMITER);
						break;
					case PARTITION : 
						sb.append(String.format(numberFormat(ViewColumnType.PARTITION.colLength()), offset.getPartition()))
						.append(DELIMITER);
						break;
					case HOST : 
						sb.append(centerString(ViewColumnType.HOST.colLength(), offset.getHost())).append(DELIMITER);
						break;
					case COMMITTED_OFFSET : 
						sb.append(String.format(numberFormat(ViewColumnType.COMMITTED_OFFSET.colLength()),
								offset.getCommittedOffset())).append(DELIMITER);
						break;
					case END_OFFSET : 
						sb.append(String.format(numberFormat(ViewColumnType.END_OFFSET.colLength()), offset.getEndOffset()))
						.append(DELIMITER);
						break;
					case LAG : 
						sb.append(String.format(numberFormat(ViewColumnType.LAG.colLength()), offset.getLag())).append(DELIMITER);
						break;
					case LEADER : 
						sb.append(centerString(ViewColumnType.LEADER.colLength(), offset.getLeader())).append(DELIMITER);
						break;
					case REPLICAS : 
						sb.append(centerString(ViewColumnType.REPLICAS.colLength(), offset.getReplicas())).append(DELIMITER);
						break;
					default : 
						logger.warn("[printBody] Not Found ColumnType : {}", col);
				}// end switch
			}
			
			sb.append(NEWLINE);
			sb.append(printLine(showColumn));
			
			counsumerCnt++;
		}
		
		
		return sb.toString();
	}
	
	/*
	 * replaced vy printBody(ViewColumnType[] showColumn, ConsumerGroupVO consumerGroup
	 */
	@Deprecated
	private static String printBody(ConsumerGroupVO consumerGroup) {
		StringBuilder sb = new StringBuilder();
		int counsumerCnt = 0;
		for (OffsetVO offset : consumerGroup.getOffsetList()) {
			sb.append(DELIMITER);

			if (counsumerCnt > 0) {
				sb.append(String.format(stringFormat(ViewColumnType.TOPIC.colLength()), "")).append(DELIMITER);
			} else {
				sb.append(String.format(stringFormat(ViewColumnType.TOPIC.colLength()), consumerGroup.getTopic()))
						.append(DELIMITER);
			}

			sb.append(String.format(stringFormat(ViewColumnType.GROUP.colLength()), consumerGroup.getGroupID()))
					.append(DELIMITER);
			sb.append(String.format(numberFormat(ViewColumnType.PARTITION.colLength()), offset.getPartition()))
					.append(DELIMITER);
			sb.append(centerString(ViewColumnType.HOST.colLength(), offset.getHost())).append(DELIMITER);
			sb.append(String.format(numberFormat(ViewColumnType.COMMITTED_OFFSET.colLength()),
					offset.getCommittedOffset())).append(DELIMITER);
			sb.append(String.format(numberFormat(ViewColumnType.END_OFFSET.colLength()), offset.getEndOffset()))
					.append(DELIMITER);
			sb.append(String.format(numberFormat(ViewColumnType.LAG.colLength()), offset.getLag())).append(DELIMITER);
			sb.append(centerString(ViewColumnType.LEADER.colLength(), offset.getLeader())).append(DELIMITER);
			sb.append(centerString(ViewColumnType.REPLICAS.colLength(), offset.getReplicas())).append(DELIMITER);

			sb.append(NEWLINE);
			sb.append(printLine(default_showConsumerOffset));

			counsumerCnt++;
		}

		return sb.toString();
	}

	private static String printBody(ViewColumnType[] showColumn, List<String> list) {
		StringBuilder sb = new StringBuilder();
		for (String str : list) {
			sb.append(DELIMITER);
			sb.append(String.format(stringFormat(showColumn[0].colLength()), str)).append(DELIMITER);

			sb.append(NEWLINE);
			sb.append(printLine(showColumn));

		}
		return sb.toString();
	}

	private static String printLine(ViewColumnType[] showColumn) {
		StringBuilder sb = new StringBuilder();
		sb.append(CORNER);
		for (ViewColumnType col : showColumn) {
			sb.append(makeLine(col.colLength())).append(CORNER);
		}
		sb.append(NEWLINE);

		return sb.toString();
	}

	private static String makeLine(int len) {
		StringBuilder line = new StringBuilder();
		for (int i = 0; i < len; i++) {
			line.append(LINE);
		}
		return line.toString();
	}

	private static String centerString(int length, String str) {
		return String.format("%-" + length + "s",
				String.format("%" + (str.length() + (length - str.length()) / 2) + "s", str));
	}

	private static String stringFormat(int length) {
		return "%-" + length + "s";
		//return stringFormat(length, false);
	}

	private static String stringFormatRight(int length) {
		return "%" + length + "s";
	}

	private static String numberFormat(int length) {
		return "%," + length + "d";
	}
}
