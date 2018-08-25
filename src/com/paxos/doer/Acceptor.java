package com.paxos.doer;

import com.paxos.bean.AcceptorStatus;
import com.paxos.bean.CommitResult;
import com.paxos.bean.PrepareResult;
import com.paxos.bean.Proposal;
import com.paxos.util.PaxosUtil;

/**
 * 决策者
 *
 */
public class Acceptor {
	private int acceptor_id;
	// 记录已处理提案的状态
	private AcceptorStatus status = AcceptorStatus.NONE;
	// 记录最新承诺的提案
	private Proposal promisedProposal = new Proposal();
	// 记录最新批准的提案
	private Proposal acceptedProposal = new Proposal();

	public Acceptor(int acceptor_id) {
		this.acceptor_id = acceptor_id;
	}

	// 加锁此准备函数，不允许同时访问。模拟单个决策者串行处理一个请求。
	public synchronized PrepareResult onPrepare(Proposal szProposal) {
		PrepareResult prepareResult = new PrepareResult();

		// 模拟网络不正常，发生丢包、超时现象
		if (PaxosUtil.isCrashed()) {
			PaxosUtil.printStr(szProposal+"Acceptor Prepare 网络出现故障"+this);
			return null;
		}

		switch (status) {
		// NONE表示之前没有承诺过任何提议者
		// 此时，接受提案
		case NONE:
			prepareResult.setAcceptorStatus(AcceptorStatus.NONE);
			prepareResult.setPromised(true);
			prepareResult.setProposal(null);
			// 转换自身的状态，已经承诺了提议者，并记录承诺的提案。
			status = AcceptorStatus.PROMISED;
			promisedProposal.copyFromInstance(szProposal);
			System.out.println(szProposal+"当前为NONE，承诺了提议者，并记录承诺的提案");
			return prepareResult;
		// 已经承诺过任意提议者
		case PROMISED:
			// 判断提案的先后顺序，只承诺相对较新的提案
			if (promisedProposal.getId() > szProposal.getId()) {
				prepareResult.setAcceptorStatus(status);
				prepareResult.setPromised(false);
				prepareResult.setProposal(promisedProposal);
				System.out.println(szProposal+"太低的编号,当前为" +promisedProposal.getId()+ ",所以不接受承诺");
				return prepareResult;
			} else {
				promisedProposal.copyFromInstance(szProposal);
				prepareResult.setAcceptorStatus(status);
				prepareResult.setPromised(true);
				prepareResult.setProposal(promisedProposal);
				System.out.println(szProposal + "的提案被承诺了");
				return prepareResult;
			}
			// 已经批准过提案
		case ACCEPTED:
			// 如果是同一个提案，只是序列号增大
			// 批准提案，更新序列号。
			if (promisedProposal.getId() < szProposal.getId()
					&& promisedProposal.getValue().equals(szProposal.getValue())) {
				promisedProposal.setId(szProposal.getId());
				prepareResult.setAcceptorStatus(status);
				prepareResult.setPromised(true);
				prepareResult.setProposal(promisedProposal);
				System.out.println(szProposal+"已经批准过，更新序列号,返回:" + prepareResult);
				return prepareResult;
			} else { // 否则，不予批准
				prepareResult.setAcceptorStatus(status);
				prepareResult.setPromised(false);
				prepareResult.setProposal(acceptedProposal);
				System.out.println(szProposal+"不予批准:返回:" + prepareResult);
				return prepareResult;
			}
		default:
			// return null;
		}

		return null;
	}

	// 加锁此提交函数，不允许同时访问，模拟单个决策者串行决策
	public synchronized CommitResult onCommit(Proposal szProposal) {
		CommitResult commitResult = new CommitResult();
		if (PaxosUtil.isCrashed()) {
			System.out.println(szProposal+"Acceptor Commit阶段 网络出现故障"+this);
			return null;
		}
		switch (status) {
		// 不可能存在此状态
		case NONE:
			return null;
		// 已经承诺过提案
		case PROMISED:
			// 判断commit提案和承诺提案的序列号大小
			// 大于，接受提案。
			System.out.println(szProposal.getId() + "与PROMISED阶段的编号比较" + promisedProposal.getId());
			if (szProposal.getId() >= promisedProposal.getId()) {
				promisedProposal.copyFromInstance(szProposal);
				acceptedProposal.copyFromInstance(szProposal);
				status = AcceptorStatus.ACCEPTED;
				commitResult.setAccepted(true);
				commitResult.setAcceptorStatus(status);
				commitResult.setProposal(promisedProposal);
				System.out.println(szProposal+"号大 接受");
				return commitResult;

			} else { // 小于，回绝提案
				commitResult.setAccepted(false);
				commitResult.setAcceptorStatus(status);
				commitResult.setProposal(promisedProposal);
				System.out.println(szProposal+"号小 拒绝");
				return commitResult;
			}
			// 已接受过提案
		case ACCEPTED:
			// 同一提案，序列号较大，接受
			System.out.println(szProposal.getId() + "与ACCEPTED阶段的编号比较" + promisedProposal.getId());
			if (szProposal.getId() > acceptedProposal.getId()
					&& szProposal.getValue().equals(acceptedProposal.getValue())) {
				acceptedProposal.setId(szProposal.getId());
				commitResult.setAccepted(true);
				commitResult.setAcceptorStatus(status);
				commitResult.setProposal(acceptedProposal);
				System.out.println(szProposal+"号大 接受 状态是ACCEPTED,只更新编号,不更新值 ");
				return commitResult;
			} else { // 否则，回绝提案
				commitResult.setAccepted(false);
				commitResult.setAcceptorStatus(status);
				commitResult.setProposal(acceptedProposal);
				System.out.println(szProposal+"号太小 拒绝");
				return commitResult;
			}
		}

		return null;
	}

	@Override
	public String toString() {
		return "Acceptor [acceptor_id=" + acceptor_id + ", status=" + status + ", promisedProposal=" + promisedProposal
				+ ", acceptedProposal=" + acceptedProposal + "]";
	}

}
