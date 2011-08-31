package com.taobao.oceanbase.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.oceanbase.ClientImpl;
import com.taobao.oceanbase.network.Server;
import com.taobao.oceanbase.network.SessionFactory;
import com.taobao.oceanbase.network.exception.ConnectException;
import com.taobao.oceanbase.network.exception.TimeOutException;
import com.taobao.oceanbase.network.packet.CommandPacket;
import com.taobao.oceanbase.network.packet.OBIRoleResponseInfo;
import com.taobao.oceanbase.util.Const;
import com.taobao.oceanbase.util.Helper;
import com.taobao.oceanbase.vo.ResultCode;
import com.taobao.oceanbase.vo.inner.PacketCode;

public class ObInstanceManager {
	
	class ObInstance {
		public ObInstance(String a,int p) {
			ip = a;
			port = p;
		}
		public String ip;
		public int port;
	}
	
	private static int MASTER = 0;
	private static int SLAVE = 1; 
	private static final Log log = LogFactory.getLog(ObInstanceManager.class);
	private RootServer master_instance_;
	private List<ObInstance> instance_list_ = new ArrayList<ObInstanceManager.ObInstance>();
	private AtomicInteger request = new AtomicInteger(0);
	private ReentrantLock lock = new ReentrantLock();
	private Map<ObInstance,Server> session_list = new HashMap<ObInstance,Server>();
	public SessionFactory factory;
	private int timeout;
	
	public ObInstanceManager(SessionFactory session,int network_timeout,List<String>instance_list) {
		factory = session;
		timeout = network_timeout;
		for(String addr : instance_list)  {
			//System.err.println(addr + Helper.getHost(addr) + Helper.getPort(addr));
			this.instance_list_.add(new ObInstance(Helper.getHost(addr),Helper.getPort(addr)));
		}
	}
	
	public RootServer getInstance() {
		if (master_instance_ != null) {
			return master_instance_;
		}
		
		lock.lock();
		try {
			if (master_instance_ != null) {
				return master_instance_;
			}

			ObInstance ins = getMasterInstance(factory,instance_list_);
			if (ins == null) {
				throw new RuntimeException("cann't get master instance");
			} else {
				master_instance_ = new RootServer(session_list.get(ins));
			}
		} finally {
			lock.unlock();
		}

		return master_instance_;
	}
	
	public RootServer getInstanceForRead() {
		return null;
	}
	
	public void removeCache() {
		master_instance_ = null;
	}
	
	private ObInstance getMasterInstance(SessionFactory session,List<ObInstance> instance_list) {
		ObInstance ret = null;
		for(ObInstance ins : instance_list) {
			try {
				if (isMasterInstance(ins)){
					ret = ins;
					break;
				}
			} catch (RuntimeException e) {
				continue;
			}
		}
		return ret;
	}
	
	private boolean isMasterInstance(ObInstance instance) {
		boolean ret = false;
		if (instance == null){
			ret = false;
		} else {
			Server tmp_server = session_list.get(instance);
			if (tmp_server == null){
				 tmp_server = Helper.getServer(factory, instance.ip,
							instance.port, timeout);
				 session_list.put(instance,tmp_server);
			}
			CommandPacket packet = new CommandPacket(
					PacketCode.OB_GET_OBI_ROLE, getRequest());
			OBIRoleResponseInfo response = null;
			try {
				response = tmp_server.commit(packet);
			} catch (ConnectException e) {
				Helper.sleep(Const.MIN_TIMEOUT);
				throw new TimeOutException("get role info timeout" + e);
			}
			if (!(ResultCode.OB_SUCCESS == ResultCode.getResultCode(response
					.getResultCode())))
				throw new RuntimeException("get role info  error");
			int role = response.getRole();

			if (role == MASTER) {
				log.info(" master instance " + instance.ip + ":" + instance.port);
				ret = true;
			}else {
				log.debug("this instance is not a master instance " + instance.ip + ":" + instance.port);
			}

		}
		return ret;
	}
	
	private int getRequest() {
		int seq = request.incrementAndGet();
		return seq != 0 ? seq : getRequest();
	}

}
