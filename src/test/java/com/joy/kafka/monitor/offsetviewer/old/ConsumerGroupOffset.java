package com.joy.kafka.monitor.offsetviewer.old;

import java.util.List;

import org.apache.kafka.clients.admin.AdminClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.joy.kafka.monitor.offsetviewer.old.vo.OffsetPerPartitonVO;
import com.joy.kafka.monitor.offsetviewer.old.vo.OffsetPerTopicVO;

/**
 * https://github.com/rusonding/kafka-monitor
 * https://github.com/recc/AnotherKafkaMonitor
 * 
* Created by lixun on 2017/3/22.
*/
public class ConsumerGroupOffset {

	private static final Logger logger = LoggerFactory.getLogger(ConsumerGroupOffset.class);

	//@PostConstruct
	//public void setUp() {
	//logger.debug("setUp : ");//logger.debug
	//}

	//@PreDestroy
	//public void tearDown() {
	//	KafkaConsumerFactory.closeAllConnection();
	//}

//	private static List<String> getConsumerList(AdminClient adminClient) throws Exception {
//		List<String> consumerList = ConsumerGroupCommand.getConsumers(adminClient);
//
//		return consumerList;
//	}

	private List<OffsetPerTopicVO> getConsumerGroupDescribeNew(String brokers, OffsetPerTopicVO offsetPerTopicVO) throws Exception {
		return new ConsumerGroupHandler().describeGroupNew(brokers, offsetPerTopicVO);
	}

	public List<OffsetPerTopicVO> getConsumerOffsetInfoNew(String brokers, OffsetPerTopicVO offsetPerTopicVO)
			throws Exception {

		List<OffsetPerTopicVO> offsetPerTopicVOList = getConsumerGroupDescribeNew(brokers, offsetPerTopicVO);

		return offsetPerTopicVOList;
	}
	
	public OffsetPerTopicVO getConsumerOffsetInfoNew(String brokers, OffsetPerTopicVO offsetPerTopicVO,
			AdminClient adminClient) throws Exception {

		List<OffsetPerTopicVO> resultOffsetPerTopicVO = getConsumerOffsetInfoNew(brokers, offsetPerTopicVO);

		if (resultOffsetPerTopicVO != null && resultOffsetPerTopicVO.size() > 0) {
			//offsetPerTopicVO.setTopic(resultOffsetPerTopicVO.get(0).getTopic());
			offsetPerTopicVO.setPartitions(resultOffsetPerTopicVO.get(0).getPartitions());
		} else {
			offsetPerTopicVO.addPartitions(new OffsetPerPartitonVO(-1, -1, -1)); // no consumer
		}

		return offsetPerTopicVO;
	}
}
