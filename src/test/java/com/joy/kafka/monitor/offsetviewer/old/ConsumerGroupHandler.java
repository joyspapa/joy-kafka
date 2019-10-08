package com.joy.kafka.monitor.offsetviewer.old;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.joy.kafka.monitor.offsetviewer.old.vo.OffsetPerPartitonVO;
import com.joy.kafka.monitor.offsetviewer.old.vo.OffsetPerTopicVO;

/**
 * Created by lixun on 2017/3/23.
 */
public class ConsumerGroupHandler {
	private static final Logger logger = LoggerFactory.getLogger(ConsumerGroupHandler.class);

	private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private List<PartitionInfo> pratitions;
	
	public static long getLogEndOffset(KafkaConsumer<String, String> consumer, String topic, int partition) {
		TopicPartition topicPartition = new TopicPartition(topic, partition);
		List<TopicPartition> partitions = new ArrayList<TopicPartition>();
		partitions.add(topicPartition);
		consumer.assign(partitions);
		consumer.seekToEnd(partitions);
		long logEndOffset = consumer.position(topicPartition);

		return logEndOffset;
	}

	public List<OffsetPerTopicVO> describeGroupNew(String brokers, OffsetPerTopicVO offsetPerTopicVO) {

		List<OffsetPerTopicVO> offsetPerTopicResult = new ArrayList<OffsetPerTopicVO>();

		try (KafkaConsumer<String, String> consumer = ConsumerFactory.getKafkaConsumer(brokers,
				offsetPerTopicVO.getGroupID()/*null*/)) {
			// if groupID is null, offset, lag is -1
			//Map<String, List<PartitionInfo>> topics = consumer.listTopics();

			//Map<String, OffsetPerTopicVO> topicPartitionMap = new HashMap<String, OffsetPerTopicVO>();
			pratitions = consumer.listTopics().get(offsetPerTopicVO.getTopic());
			logger.debug("pratitions 1 : " + pratitions);
			// 토픽이 없을 경우 생성을 시킨다.
			//List<PartitionInfo> pratitions = consumer.partitionsFor(offsetPerTopicVO.getTopic());

			String createdDate = null;
			if (pratitions != null) {
				long logEndOffset = 0l;
				int partitionIdx = 0;

				if (pratitions.size() > 1) {
					sortAscendingPartitionNum(pratitions);
				}
				
				logger.debug("pratitions : " + pratitions);
				for (PartitionInfo partition : pratitions) {

					printPartitionInfo(consumer, partition);

					OffsetPerPartitonVO offsetPerPartitionVO = new OffsetPerPartitonVO();
					offsetPerPartitionVO.setPartition(partition.partition());

					String leader = (partition.leader() == null) ? "none"
							: (partition.leader().host().contains(".") ? partition.leader().host().split("\\.")[3]
									: partition.leader().host());
					offsetPerPartitionVO.setLeader(leader);

					String replica = null;
					for (Node node : partition.replicas()) {
						replica = (node.host() == null) ? "none"
								: (node.host().contains(".") ? node.host().split("\\.")[3] : node.host());
						offsetPerPartitionVO.addReplicas(replica);
					}

					//for (Node node : partition.inSyncReplicas()) {
					//	logger.debug("inSyncReplicas : " + node.host());
					//}

					logEndOffset = getLogEndOffset(consumer, offsetPerTopicVO.getTopic(), partition.partition());
					offsetPerPartitionVO.setLogSize(logEndOffset);

					OffsetAndMetadata committed = consumer
							.committed(new TopicPartition(offsetPerTopicVO.getTopic(), partition.partition()));

					if (partitionIdx == 0) {
						createdDate = dateFormat.format(new Date(System.currentTimeMillis()));
					}

					if (committed != null) {

						offsetPerPartitionVO.setOffset(committed.offset());
						offsetPerPartitionVO
								.setLag((committed.offset() == -1L) ? 0L : logEndOffset - committed.offset());

					} else {

						//offsetPerPartitionVO.setOffset(-1);
						//offsetPerPartitionVO.setLag(-1);
						offsetPerPartitionVO.setOffset(0);
						offsetPerPartitionVO.setLag(0);

					}

					offsetPerTopicVO.addPartitions(offsetPerPartitionVO);

					partitionIdx++;
				}
			} else {
				offsetPerTopicVO.setOffsets(-1);
				offsetPerTopicVO.setLag(-1);
				logger.warn("[describeGroupNew] Topic is NOT Exist. Topic : " + offsetPerTopicVO.getTopic());
			}

			offsetPerTopicVO.setCreated(
					(createdDate == null) ? dateFormat.format(new Date(System.currentTimeMillis())) : createdDate);

			offsetPerTopicResult.add(offsetPerTopicVO);

		} catch (Exception ex) {
			logger.warn("describeGroup error : ", ex);
		}

		return offsetPerTopicResult;
	}

	/*
	 * https://stackoverflow.com/questions/50527319/kafka-consumer-list-api
	 * 
	public void getConsumerGroupList(String brokers) {
		String[] arg = new String[3];
		arg[0] = "--bootstrap-server";
		arg[1] = " localhost:9092";
		arg[2] = "--list";

		String[] resultArray = new String[3];
		ConsumerGroupCommand.ConsumerGroupCommandOptions checkArgs = new ConsumerGroupCommand.ConsumerGroupCommandOptions(
		    arg);
		checkArgs.checkArgs();
		List<String> result = new ConsumerGroupCommand.KafkaConsumerGroupService(checkArgs).listGroups();
		System.out.println(result);
		result.copyToArray(resultArray);
		java.util.List<String> names = java.util.Arrays.asList(resultArray);

		System.out.println("result" + names);
	}
	*/
	
	/*
	 * https://github.com/rusonding/kafka-monitor/blob/master/common/src/main/java/com/kafka/monitor/common/ConsumerGroupCommand.java
	 * 
	public static List<String> getConsumers(String brokers) {
        List<String> list = new ArrayList<String>();
        AdminClient adminClient = KafkaConsumerFactory.createAdminClient(brokers);
        scala.collection.immutable.Map<Node, scala.collection.immutable.List<GroupOverview>> nodeListMap = adminClient.listAllConsumerGroups();
        Iterator<Tuple2<Node, scala.collection.immutable.List<GroupOverview>>> iterator = nodeListMap.iterator();
        while (iterator.hasNext()) {
            Tuple2<Node, scala.collection.immutable.List<GroupOverview>> next = iterator.next();
            scala.collection.immutable.List<GroupOverview> groupOverviewList = next._2();
            List<Object> objects = JavaConversions.bufferAsJavaList(groupOverviewList.toBuffer());
            for (Object obj : objects) {
                list.add(((GroupOverview) obj).groupId());
            }
        }
        return list;
    }
	*/
	private static void printPartitionInfo(KafkaConsumer<String, String> consumer, PartitionInfo partitionInfo) {
		StringBuilder sb = new StringBuilder();
		sb.append(partitionInfo.topic()).append("-").append(partitionInfo.partition());
		sb.append(partitionInfo.topic());

		logger.debug("========================================================");
		logger.debug("topic : " + partitionInfo.topic());
		logger.debug("partition : " + partitionInfo.partition());
		logger.debug("leader : " + partitionInfo.leader().host());
		logger.debug("replicas  [");
		for (Node node : partitionInfo.replicas()) {
			logger.debug("         " + node.host());
		}
		logger.debug("          ]");

		long logEndOffset = getLogEndOffset(consumer, partitionInfo.topic(), partitionInfo.partition());
		logger.debug("logEndOffset : " + logEndOffset);

		OffsetAndMetadata committed = consumer
				.committed(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
		logger.debug("처리건수 : " + committed.offset());
		logger.debug("미처리건수 : " + (logEndOffset - committed.offset()));
		logger.debug("");
		logger.debug("");
	}

	private void sortAscendingPartitionNum(List<PartitionInfo> pratitionList) {
		pratitions = new ArrayList<PartitionInfo>(pratitionList);
		pratitions.sort(Comparator.comparing(PartitionInfo::partition));
	}
	
	@Deprecated
	private void sortAscendingPartitionNum_old(List<PartitionInfo> pratitionList) {
		pratitions = new ArrayList<PartitionInfo>(pratitions);
		Collections.sort(pratitions, new Comparator<PartitionInfo>() {
			@Override
			public int compare(PartitionInfo before, PartitionInfo after) {
				// asc
				int r = ((Comparable<Integer>) before.partition()).compareTo(after.partition());
				logger.debug("r : " + r);
				return r;
			}
		});
	}
}
