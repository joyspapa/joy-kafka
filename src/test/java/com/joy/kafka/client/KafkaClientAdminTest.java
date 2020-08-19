package com.joy.kafka.client;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.joy.kafka.monitor.factory.KafkaAdminClientFactory;

/*
 * ** https://github.com/vert-x3/vertx-kafka-client/blob/master/src/main/java/examples/KafkaAdminClientExamples.java
 * ** https://springboot.cloud/36 (KAFKA 한국 사용자 모임)
 * https://stackoverflow.com/questions/53124255/ensuring-kafka-consumer-registers-to-an-unassigned-partition-where-consumers-are
 */
public class KafkaClientAdminTest {

	private KafkaAdminClient adminClient = null;

	@Before
	public void preProcess() {
		adminClient = KafkaAdminClientFactory
				.getAdminClient("192.168.20.201:9092,192.168.20.202:9092,192.168.20.203:9092");
	}

	@After
	public void afterProcess() {
		adminClient.close();
	}

	@Test
	public void testDescribeConsumerGroups() throws InterruptedException, ExecutionException {
		DescribeConsumerGroupsResult describeConsumerGroupsResult = adminClient.describeConsumerGroups(
				Collections.singletonList("groupID-SSG_TRK_SAVE_CACHE-DE1594368934-ENT4828-CUSTOMLOGIC_IMDG"));

		Iterator<ConsumerGroupDescription> runningConsumerIter = describeConsumerGroupsResult.all().get().values()
				.iterator();
		while (runningConsumerIter.hasNext()) {
			ConsumerGroupDescription consumerGroupDescription = runningConsumerIter.next();

			System.out.println(
					"Group id=" + consumerGroupDescription.groupId() + " state= " + consumerGroupDescription.state()
							+ " coordinator host= " + consumerGroupDescription.coordinator().host());

			for (MemberDescription memberDescription : consumerGroupDescription.members()) {
				System.out.println("client id= " + memberDescription.clientId());
				System.out.println("consumerId= " + memberDescription.consumerId());

				MemberAssignment memberAssignment = memberDescription.assignment();
				Set<TopicPartition> topicPartitions = memberAssignment.topicPartitions();
				System.out.println(" topic partitions= " + topicPartitions);
			}
		}
	}

	@Test
	public void testListConsumerGroups() throws InterruptedException, ExecutionException {
		ListConsumerGroupsResult consumerGroups = adminClient.listConsumerGroups();
		consumerGroups.all().get().forEach(consumerGroup -> System.out.println("groupId= " + consumerGroup.groupId()));
	}

	/*
	 * https://stackoverflow.com/questions/56080896/what-are-internal-topics-used-in-kafka?rq=1
	 */
	@Test
	public void testListTopics() throws InterruptedException, ExecutionException {
	    // listInternal(false) : exclude __consumer_offsets
		ListTopicsResult listTopicsResult = adminClient.listTopics(new ListTopicsOptions().listInternal(false));
		listTopicsResult.names().get().forEach(topic -> System.out.println("Topic= " + topic));
	}

	@Test
	public void testDescribeTopics() throws InterruptedException, ExecutionException {
		String topicName = "SSG-TRK-IN-TOPIC";
		DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singletonList(topicName));
		TopicDescription topicDescription = describeTopicsResult.all().get().get(topicName);

		System.out.println("Topic name=" + topicDescription.name() + " isInternal= " + topicDescription.isInternal()
				+ " partitions= " + topicDescription.partitions().size());

		for (TopicPartitionInfo topicPartitionInfo : topicDescription.partitions()) {
			System.out.println(
					"Partition id= " + topicPartitionInfo.partition() + " leaderId= " + topicPartitionInfo.leader().id()
							+ " replicas= " + topicPartitionInfo.replicas() + " isr= " + topicPartitionInfo.isr());
		}
	}

	@Test
	public void testCreateTopics() throws InterruptedException, ExecutionException {
		String topicName = "TEST-TOPIC";
		CreateTopicsResult createTopicsResult = adminClient
				.createTopics(Collections.singletonList(new NewTopic(topicName, 1, (short) 1)));
		System.out.println("topics created : " + createTopicsResult.all().isDone());

	}

	@Test
	public void testDeleteTopics() throws InterruptedException, ExecutionException {
		String topicName = "TEST-TOPIC";
		DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Collections.singletonList(topicName));
		System.out.println("topics deleted : " + deleteTopicsResult.all().isDone());

	}

	@Test
	public void testDescribeConfigs() throws InterruptedException, ExecutionException {
		String topicName = "TEST-TOPIC";
		DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Collections.singletonList(
				new ConfigResource(org.apache.kafka.common.config.ConfigResource.Type.TOPIC, topicName)));

		Iterator<Config> configIter = describeConfigsResult.all().get().values().iterator();
		while (configIter.hasNext()) {
			Config config = configIter.next();
			System.out.println("config : " + config.toString());
		}
	}

	@Test
	public void testAlterConfigs() {
		String topicName = "TEST-TOPIC";
		ConfigResource resource = new ConfigResource(org.apache.kafka.common.config.ConfigResource.Type.TOPIC,
				topicName);
		// create a entry for updating the retention.ms value on the topic
		ConfigEntry retentionEntry = new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "51000");

		Map<ConfigResource, Collection<AlterConfigOp>> updateConfigs = new HashMap<>();
		AlterConfigOp alterConfigOp = new AlterConfigOp(retentionEntry, OpType.APPEND);
		updateConfigs.put(resource, Collections.singleton(alterConfigOp));

		AlterConfigsResult alterConfigsResult = adminClient.incrementalAlterConfigs(updateConfigs);
		System.out.println("topics alter : " + alterConfigsResult.all().isDone());
	}

	@Test
	public void testCreatePartitions() {
		String topicName = "TEST-TOPIC";
		int numPartitions = 2;

		Map<String, NewPartitions> newPartitionSet = new HashMap<>();
		newPartitionSet.put(topicName, NewPartitions.increaseTo(numPartitions));
		CreatePartitionsResult createPartitionsResult = adminClient.createPartitions(newPartitionSet);
		//System.out.println("topics alter : " + alterConfigsResult.all().isDone());
	}

	@Test
	public void getTopicPartitionInfo() throws ExecutionException, InterruptedException {
		String topicName = "TEST-TOPIC";
		DescribeTopicsResult describeTopicsResult = this.adminClient
				.describeTopics(Collections.singletonList(topicName));
		KafkaFuture<Map<String, TopicDescription>> futureOfDescribeTopicResult = describeTopicsResult.all();
		Map<String, TopicDescription> topicDescriptionMap = futureOfDescribeTopicResult.get();
		TopicDescription topicDescription = topicDescriptionMap.get(topicName);

		for (TopicPartitionInfo topicPartitionInfo : topicDescription.partitions()) {
			System.out.println("partition : " + topicPartitionInfo.partition());
		}
	}

	@Test
	public void getConsumerGroupOffsetInfo()
			throws ExecutionException, InterruptedException {
		String consumerGroupID = "groupID-SSG_TRK_SAVE_CACHE-DE1594368934-ENT4828-CUSTOMLOGIC_IMDG";
		ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult = this.adminClient
				.listConsumerGroupOffsets(consumerGroupID);
		KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> futureOfConsumerGroupOffsetResult = listConsumerGroupOffsetsResult
				.partitionsToOffsetAndMetadata();
		Map<TopicPartition, OffsetAndMetadata> consumerGroupOffsetInfo = futureOfConsumerGroupOffsetResult.get();
		Map<TopicPartition, Long> consumerGroupOffsetInfoMap = consumerGroupOffsetInfo.entrySet().stream()
				.collect(Collectors.toMap(Map.Entry::getKey, x -> x.getValue().offset()));
		
		Iterator<TopicPartition> topicPartitionIter = consumerGroupOffsetInfoMap.keySet().iterator();
		while(topicPartitionIter.hasNext()) {
			TopicPartition topicPartition = topicPartitionIter.next();
			long l = consumerGroupOffsetInfoMap.get(consumerGroupOffsetInfoMap);
			System.out.println("topicPartition.partition : " + topicPartition.partition());
			System.out.println("l : " + l);
		}
	}
	
	private void getAvailablePartitions() {
		List<TopicPartitionInfo> topicPartitionInfoList = null;
		Map<String, Set<TopicPartition>> memberToTopicPartitionMap = null;

		Map<Integer, List<String>> partitionToMemberMap = new HashMap<>();
		topicPartitionInfoList.forEach(x -> partitionToMemberMap.put(x.partition(), new LinkedList<>()));
		for (Map.Entry<String, Set<TopicPartition>> entry : memberToTopicPartitionMap.entrySet()) {
			String memberID = entry.getKey();
			for (TopicPartition topicPartition : entry.getValue()) {
				partitionToMemberMap.get(topicPartition.partition()).add(memberID);
			}
		}

		List<Integer> availablePartitions = partitionToMemberMap.entrySet().stream().filter(x -> x.getValue().isEmpty())
				.map(Map.Entry::getKey).collect(Collectors.toList());

		for (Integer available : availablePartitions) {
			System.out.println("availablePartition : " + available);
		}
	}
}
