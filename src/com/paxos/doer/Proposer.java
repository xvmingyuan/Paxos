package com.paxos.doer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.paxos.bean.AcceptorStatus;
import com.paxos.bean.CommitResult;
import com.paxos.bean.PrepareResult;
import com.paxos.bean.Proposal;
import com.paxos.main.Main;
import com.paxos.util.PaxosUtil;
import com.paxos.util.PerformanceRecord;

/**
 * 提议者
 *
 */
public class Proposer implements Runnable{
	
	// 序列号
	int myID = 0;
	// 提议者的名字
	private String name;
	//	提议者的提案
	private Proposal proposal;
	
	//	是否已经有提案获得大多数决策者确认
	private boolean voted;
	//	大多数决策者的最小个数
	private int halfCount;
	
	private int proposerCount;
	
	private int numCycle = 0;
	//	决策者们
	private List<Acceptor> acceptors;
	
	public Proposer(int myID, String name, int proposerCount, List<Acceptor> acceptors){
		this.myID = myID;
		this.name = name;
		this.acceptors = acceptors;
		this.proposerCount = proposerCount;
		numCycle = 0;
		voted = false;
		halfCount = acceptors.size() / 2 + 1;
		this.proposal = new Proposal(PaxosUtil.generateId(myID, numCycle, proposerCount), 
										myID+ "#Proposal", myID + "为多数票");
		numCycle++;
	}
	
	//	准备提案过程，获得大多数决策者支持后进入确认提案阶段。
	public synchronized boolean prepare(){
		System.out.println(proposal.getId()+"提案准备阶段开始");
		PrepareResult prepareResult = null;
		
		boolean isContinue = true;
		//	已获得承诺个数
		int promisedCount = 0;
		
		do{
			// 决策者已承诺的提案集合
			List<Proposal> promisedProposals = new ArrayList<Proposal>(); 
			// 决策者已接受(确定)的提案集合
			List<Proposal> acceptedProposals = new ArrayList<Proposal>();
			// 承诺数量
			promisedCount = 0;
			for	(Acceptor acceptor : acceptors){
				//  发送准备提案给决策者
				prepareResult = acceptor.onPrepare(proposal);
				//	随机休眠一段时间，模拟网络延迟。
				PaxosUtil.sleepRandom();
				
				//	模拟网络异常
				if	(null==prepareResult){
					System.out.println(proposal.getId()+"网络不可用");
					continue;
				}
				
				//	获得承诺
				if	(prepareResult.isPromised()){
					System.out.println(proposal.getId()+"获取一个承诺"+prepareResult.isPromised());
					promisedCount ++;
				}else{
					
					//	决策者已经给了更高id题案的承诺
					if	(prepareResult.getAcceptorStatus()==AcceptorStatus.PROMISED){
						System.out.println(proposal.getId()+"的提案承诺失败,已有更高级提案被承诺了");
						promisedProposals.add(prepareResult.getProposal());
					}
					//	决策者已经通过了一个题案
					if	(prepareResult.getAcceptorStatus()==AcceptorStatus.ACCEPTED){
						System.out.println(proposal.getId()+"的提案承诺失败,决策者已经通过了一个题案,无法更改"+prepareResult.getProposal());
						acceptedProposals.add(prepareResult.getProposal());
					}
				}
			}// end of for
			
			//	获得多数决策者的承诺
			//	可以进行第二阶段：题案提交
			if	(promisedCount >= halfCount){
				System.out.println(proposal.getId()+"的'承诺'阶段通过,数量:"+promisedCount+"-----最小提案通过数:"+halfCount);
				break;
			}
			Proposal votedProposal = votedEnd(acceptedProposals);
			//	决策者已经半数通过题案
			if	(votedProposal !=null){
				System.out.println(myID + " : 决策已经投票结束:" + votedProposal);
				return true;
			}
			
			
			Proposal maxIdAcceptedProposal = getMaxIdProposal(acceptedProposals);
			//	在已经被决策者通过题案中选择序列号最大的决策,作为自己的决策。
			if	(maxIdAcceptedProposal != null){
				proposal.setId(PaxosUtil.generateId(myID, numCycle, proposerCount));
				proposal.setValue(maxIdAcceptedProposal.getValue());
				System.out.println(proposal.getId()+"获取更大ID,并接受maxIdAcceptedProposal提案中的value"+maxIdAcceptedProposal);
			}else{
				System.out.println(proposal.getId()+"当前maxIdAcceptedProposal中没有被'确认'的提案");
				proposal.setId(PaxosUtil.generateId(myID, numCycle, proposerCount));
			}
			
			numCycle ++;
		}while(isContinue);
		return false;
	}
	
	//	获得大多数决策者承诺后，开始进行提案确认
	public synchronized boolean commit(){
		System.out.println(proposal.getId()+"提案确认阶段开始");
		boolean isContinue = true;
		
		//	已获得接受该提案的决策者个数
		int acceptedCount = 0;
		do{
			// 决策者已接受(确定)的提案集合
			List<Proposal> acceptedProposals = new ArrayList<Proposal>();
			acceptedCount = 0;
			// 分别给决策者发送提案
			for	(Acceptor acceptor : acceptors){
				// 决策者返回的提案结果
				CommitResult commitResult = acceptor.onCommit(proposal);
				//	模拟网络延迟
				PaxosUtil.sleepRandom();
				
				//	模拟网络异常
				if	(null==commitResult){
					System.out.println(proposal.getId()+"网络不可用");
					continue;
				}
				
				//	题案被决策者接受。
				if	(commitResult.isAccepted()){
					System.out.println(proposal.getId()+"提案被接受一次"+"接受者"+acceptor);
					acceptedCount ++;
				}else{
					System.out.println(proposal.getId()+"提案未被接受"+"不接受者"+acceptor);
					// 将未接受的提案 放入集合中
					acceptedProposals.add(commitResult.getProposal());
				}
			}
			//	题案被半数以上决策者接受，说明题案已经被选出来。
			if	(acceptedCount >= halfCount){
				System.out.println(myID + " : 题案已经投票选出:" + proposal);
				return true;
			}else{
				Proposal maxIdAcceptedProposal = getMaxIdProposal(acceptedProposals);
				//	在已经被决策者通过题案中选择序列号最大的决策,重新生成递增id，改变自己的value为序列号最大的value。
				//	这是一种预测，预测此maxIdAccecptedProposal最有可能被超过半数的决策者接受。
				if	(maxIdAcceptedProposal != null){
					proposal.setId(PaxosUtil.generateId(myID, numCycle, proposerCount));
					proposal.setValue(maxIdAcceptedProposal.getValue());
					System.out.println(proposal.getId()+"获取更大ID,并接受maxIdAcceptedProposal提案中的value"+maxIdAcceptedProposal);
				}else{
					proposal.setId(PaxosUtil.generateId(myID, numCycle, proposerCount));
					System.out.println(proposal.getId()+"提案号加大,做重审准备"+proposal);
				}
				
				numCycle++;
				//	回退到决策准备阶段
				if	(prepare()) {
					System.out.println(proposal+"重新准备提案"+numCycle);
					return true;}
			}
			
		}while(isContinue);
		return true;
	}
	
	//	获得序列号最大的提案
	private Proposal getMaxIdProposal(List<Proposal> acceptedProposals){
		if	(acceptedProposals.size()>0){
			Proposal retProposal = acceptedProposals.get(0);
			for	(Proposal proposal : acceptedProposals){
				System.out.println("寻找最大提案号:"+proposal.getId());
				if	(proposal.getId()>retProposal.getId())
					retProposal = proposal;
					System.out.println(proposal+"接受名单中最大编号的提案"+retProposal);
			}
			
			return retProposal;
		}
		return null;
	}
	
	//	是否已经有某个提案，被大多数决策者接受
	private Proposal votedEnd(List<Proposal> acceptedProposals){
		// List 转 Map
		Map<Proposal, Integer> proposalCount = countAcceptedProposalCount(acceptedProposals);
		for	(Entry<Proposal, Integer> entry : proposalCount.entrySet()){
			if	(entry.getValue()>=halfCount){
				System.out.println(proposal.getId()+"当前多数派已出现"+entry.getValue());
				voted = true;
				return entry.getKey();
			}
		}
		return null;
	}
	
	//	计算决策者回复的每个已经被接受的提案计数
	private Map<Proposal, Integer> countAcceptedProposalCount(
			List<Proposal> acceptedProposals){
		Map<Proposal, Integer> proposalCount = new HashMap<>();
		for	(Proposal proposal : acceptedProposals){
			//	决策者没有回复，或者网络异常
			if	(null==proposal) continue;
			int count = 1;
			if	(proposalCount.containsKey(proposal)){
				count = proposalCount.get(proposal) + 1;
			}
			proposalCount.put(proposal, count);
		}
		System.out.println(proposal.getId()+"计算决策者回复的每个已经被接受的提案计数"+proposalCount);
		return proposalCount;
	}

	@Override
	public void run() {
		// 
		Main.latch.countDown();
		System.out.println("主函数线程数释放一个线程");
		try {
			System.out.println("主函数等待");
			Main.latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("计时器启动,ID:"+myID);
		PerformanceRecord.getInstance().start("Proposer" + myID, myID);
		System.out.println(myID+"的提案去准备");
		prepare();
		System.out.println(myID+"的提案去确认");
		commit();
		
		PerformanceRecord.getInstance().end(myID);
		System.out.println("计时器结束,ID:"+myID);
	}
}
