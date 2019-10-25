package com.joy.kafka.util;

import java.util.LinkedList;
import java.util.Queue;

import org.junit.Test;

public class UtilsTest {

	@Test
	public void testQueue() {
		// https://blog.naver.com/pjok1122/221504688603
		Queue<String> queue = new LinkedList<String>();
		for(int i=0;i<3;i++) {
			queue.add("데이터-"+i); //큐에 데이터를 삽입한다.
		}
		//queue = ["데이터-0", "데이터-1", "데이터-2"], front=0, rear=2
		
		System.out.println("before : " + queue);
		System.out.println(queue.peek()); //큐의 front가 가리키는 값을 반환한다.         데이터-0
		//System.out.println(queue.poll()); //큐의 front가 가리키는 값을 반환하고 삭제한다.    데이터-0
		System.out.println("remove : " + queue.remove());
		System.out.println(queue.isEmpty()); // false
		System.out.println("after : " + queue);			//[데이터-1, 데이터-2]
		
		int a = 0;
		int b = 0;
		if(a <= b) {
			
		}
	}
}
