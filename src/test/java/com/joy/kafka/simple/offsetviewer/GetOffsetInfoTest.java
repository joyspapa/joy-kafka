package com.joy.kafka.simple.offsetviewer;

import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.joy.kafka.simple.offsetviewer.old.ConsumerGroupOffset;
import com.joy.kafka.simple.offsetviewer.old.vo.OffsetPerTopicVO;

public class GetOffsetInfoTest {
	private static final Logger logger = LoggerFactory.getLogger(GetOffsetInfoTest.class);
	
	String brokers = "ecube82:9092,ecube83:9092,ecube84:9092";

	public static void main(String[] args) {
		GetOffsetInfoTest test = new GetOffsetInfoTest();
		test.getConsumerOffsetInfo();
	}
	
	//@Test
	public void getConsumerOffsetInfo() {
		String topicName = "OBZ-TEST-IN-TOPIC";
		String groupID = "groupID-IGNITE_TEST-DE1559267912-ENT8243-SP";
		
		topicName = "IGNITE_TEST-ENT8243-SP-OUT01-TOPIC";
		groupID = "groupID-IGNITE_TEST-DE1559267912-ENT6787-MEMORYGRID";
		
		try {
			OffsetPerTopicVO offsetPerTopicVO = new OffsetPerTopicVO();
			offsetPerTopicVO.setGroupID(groupID);
			offsetPerTopicVO.setTopic(topicName);
			
			ConsumerGroupOffset info = new ConsumerGroupOffset();
			List<OffsetPerTopicVO> resultList = info.getConsumerOffsetInfoNew(brokers, offsetPerTopicVO);

			for(OffsetPerTopicVO re : resultList) {
				logger.debug("topic : " + re.getTopic());
				logger.debug("Lag : " + re.getLag());
				logger.debug("LogSize : " + re.getLogSize());
				logger.debug("Offsets : " + re.getOffsets());
				logger.debug("PartitionSize : " + re.getPartitionSize());
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
