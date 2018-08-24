package com.paxos.main;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.paxos.doer.Acceptor;
import com.paxos.doer.Proposer;

/**
 * 主函数 
 *
 */
public class Main {
	private static final int NUM_OF_PROPOSER = 3; // 提议者数量
	private static final int NUM_OF_ACCEPTOR = 3; // 接受提议者数量
	public static CountDownLatch latch = new CountDownLatch(NUM_OF_PROPOSER); // 延迟N个线程执行配置
	
	public static void main(String[] args) throws Exception{
		// 创建 NUM_OF_ACCEPTOR 个接受者
		List<Acceptor> acceptors = new ArrayList<>();
		for	(int i=0;i<NUM_OF_ACCEPTOR;i++){
			acceptors.add(new Acceptor());
		}
		System.out.println(latch);
		// 用线程池 创建线程
		ExecutorService es = Executors.newCachedThreadPool();
		for	(int i=0;i<NUM_OF_PROPOSER;i++){
			// 开启NUM_OF_PROPOSER个线程,执行提案
			Proposer proposer =  new Proposer(i, i + "#Proposer", NUM_OF_PROPOSER, acceptors);
			//任务提交
			es.submit(proposer);
		}
		es.shutdown();
		System.out.println(latch);
	}
}
