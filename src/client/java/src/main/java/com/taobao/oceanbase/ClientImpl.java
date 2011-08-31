package com.taobao.oceanbase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.oceanbase.network.SessionFactory;
import com.taobao.oceanbase.network.exception.ConnectException;
import com.taobao.oceanbase.network.exception.EagainException;
import com.taobao.oceanbase.network.exception.NoMergeServerException;
import com.taobao.oceanbase.network.exception.TimeOutException;
import com.taobao.oceanbase.network.mina.MinaSessionFactory;
import com.taobao.oceanbase.server.MergeServer;
import com.taobao.oceanbase.server.ObInstanceManager;
import com.taobao.oceanbase.server.RootServer;
import com.taobao.oceanbase.server.UpdateServer;
import com.taobao.oceanbase.util.CheckParameter;
import com.taobao.oceanbase.util.CheckQueryParams;
import com.taobao.oceanbase.util.Const;
import com.taobao.oceanbase.util.Helper;
import com.taobao.oceanbase.vo.GetParam;
import com.taobao.oceanbase.vo.InsertMutator;
import com.taobao.oceanbase.vo.ObMutatorBase;
import com.taobao.oceanbase.vo.QueryInfo;
import com.taobao.oceanbase.vo.Result;
import com.taobao.oceanbase.vo.RowData;
import com.taobao.oceanbase.vo.Rowkey;
import com.taobao.oceanbase.vo.UpdateMutator;
import com.taobao.oceanbase.vo.inner.ObActionFlag;
import com.taobao.oceanbase.vo.inner.ObCell;
import com.taobao.oceanbase.vo.inner.ObGetParam;
import com.taobao.oceanbase.vo.inner.ObMutator;
import com.taobao.oceanbase.vo.inner.ObObj;
import com.taobao.oceanbase.vo.inner.ObRow;
import com.taobao.oceanbase.vo.inner.schema.ObSchemaManagerV2;

public class ClientImpl implements Client {

	private static final Log log = LogFactory.getLog(ClientImpl.class);

	private static long MAX_FAIL_TIMES = 50;
	private int port;
	private String ip;
	public RootServer root;
	private int timeout = Const.MIN_TIMEOUT;
	private List<String> instance_list = null;
	public SessionFactory factory = new MinaSessionFactory(); 
	private ObInstanceManager instanceManager;
	private boolean isReadConsistency = true;
	
	private long fail_times = 0;
	
	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public void setPort(int port) {
		this.port = port;
	}
	
	public void setInstanceList(List<String> instance_list) {
		this.instance_list = instance_list;
	}

	public void init() {
		if (ip != null) {
			root = new RootServer(Helper.getServer(factory, ip, port,timeout));
			log.info("OceanBase client inited, rootserver address: " + ip + ":" + port);	
		} else {
			CheckParameter.checkCollection("instance list is null", instance_list);
			instanceManager = new ObInstanceManager(factory,timeout,instance_list);
			if (instanceManager == null) 
				throw new RuntimeException("cann't get master instance");
			root = instanceManager.getInstance();
		}
	}
	
	public void setReadConsistency(boolean isReadConsistency) {
		this.isReadConsistency = isReadConsistency;
	}
	
	public boolean isReadConsistency() {
		return isReadConsistency;
	}

	public Result<Boolean> update(UpdateMutator mutator) {
		CheckParameter.checkNull("UpdateMutator is null", mutator);
		UpdateServer update = null;
		Result<Boolean> ret = null;
		try {
			update = root.getUpdateServer(factory,timeout);
			ret = update.update(mutator);
		}catch(EagainException e) {
			instanceManager.removeCache();
			return update(mutator);
		}
		return ret;
	}
	
	public Result<Boolean> update(List<ObMutatorBase> mutators){
		return update(mutators,ObActionFlag.OP_USE_OB_SEM);
	}
	
	public Result<Boolean> update(List<ObMutatorBase> mutators,ObActionFlag actionFlag){
		CheckParameter.checkCollection("mutators is null", mutators);
		int cap = 0;
		for (ObMutatorBase im : mutators){
			cap += im.getColumns().size();
			CheckParameter.checkNull("mutator is null", im);
		}
		
		ObMutator mutator = new ObMutator(cap, actionFlag);
		for (ObMutatorBase im : mutators) {
			for (ObMutatorBase.Tuple column : im.getColumns()) {
				ObCell cell = new ObCell(im.getTable(), im.getRowkey(),
						column.name, column.value.getObject(column.add));
				mutator.addCell(cell, im.getActionFlag());
			}
		}
		return apply(mutator);
	}
	
	public Result<Boolean> delete(String table, Rowkey rowkey) {
		CheckParameter.checkTableName(table);
		CheckParameter.checkNull("rowkey is null", rowkey);
		UpdateServer update = root.getUpdateServer(factory,timeout);
		return update.delete(table, rowkey.getRowkey());
	}
	
	public Result<Boolean> delete(Map<String, Set<Rowkey>> keys) {
		return delete(keys, ObActionFlag.OP_USE_OB_SEM);
	}
	
	public Result<Boolean> delete(Map<String, Set<Rowkey>> keys, ObActionFlag actionFlag) {
		CheckParameter.checkMap("rowkey is null", keys);
		
		int count = keys.size();
		ObMutator mutator = new ObMutator(count, actionFlag);
		for (String table : keys.keySet()) {
			for (Rowkey key : keys.get(table)) {
				ObObj op = new ObObj();
				op.setExtend(ObActionFlag.OP_DEL_ROW);
				ObCell cell = new ObCell(table, key.getRowkey(), null, op);
				mutator.addCell(cell, ObActionFlag.OP_DEL_ROW);
			}
		}
	
		return apply(mutator);
	}
	
	public Result<Boolean> insert(InsertMutator mutator) {
		CheckParameter.checkNull("InsertMutator is null", mutator);
		
		ObMutator obMutator = new ObMutator(mutator.getColumns().size(),
				ObActionFlag.OP_USE_OB_SEM);
		for (InsertMutator.Tuple column : mutator.getColumns()) {
			ObCell cell = new ObCell(mutator.getTable(), mutator.getRowkey(),
					column.name, column.value.getObject(false));
			obMutator.addCell(cell, ObActionFlag.OP_INSERT);
		}
		return apply(obMutator);
	}
	
	public Result<Boolean> insert(List<InsertMutator> mutators) {
		return insert(mutators, ObActionFlag.OP_USE_OB_SEM);
	}
	public Result<Boolean> insert(List<InsertMutator> mutators, ObActionFlag actionFlag) {
		CheckParameter.checkCollection("mutators is null",mutators);
		int cap = 0;
		for (InsertMutator im : mutators) {
			CheckParameter.checkNull("mutator is null", im);
			cap += im.getColumns().size();
		}
		
		ObMutator mutator = new ObMutator(cap, actionFlag);
		for (InsertMutator im : mutators) {
			for (InsertMutator.Tuple column : im.getColumns()) {
				ObCell cell = new ObCell(im.getTable(), im.getRowkey(),
						column.name, column.value.getObject(false));
				mutator.addCell(cell, ObActionFlag.OP_INSERT);
			}
		}
		return apply(mutator);
	}

	public Result<RowData> get(String table, Rowkey rowkey, Set<String> columns) {
		CheckParameter.checkTableName(table);
		CheckParameter.checkNull("rowkey is null", rowkey);
		CheckParameter.checkCollection("columns are null", columns);
		CheckParameter.checkNullSetElement("columns have null column", columns);
		String rk = rowkey.getRowkey();
		MergeServer merge = root.getMergeServer(factory,table, rk, timeout);
		
		ObGetParam param = new ObGetParam(columns.size());
		param.setReadConsistency(isReadConsistency);
		for (String column : columns) {
			param.addCell(new ObCell(table, rk, column, null));
		}
		
		try {
			//return merge.get(table, rk, columns);
			Result<List<ObRow>> ret = merge.get(param);
			return new Result<RowData>(ret.getCode(),(ret.getResult() == null || ret.getResult().size() == 0 || ret.getResult().get(0).getColumns().size() <= 0 ) ? 
					null : Helper.getRowData(ret.getResult().get(0)));
		}catch (ConnectException e) {
			log.warn(e);
			root.remove(rk, merge);
			return get(table, rowkey, columns);
		}catch (TimeOutException e) {
			log.warn(e);
			root.remove(rk, merge);
			return get(table, rowkey, columns);
		}catch(EagainException e) {
			log.warn(e);
			instanceManager.removeCache();
			root = instanceManager.getInstance();
			return get(table,rowkey,columns);
		}catch (NoMergeServerException e) {
			log.warn(e);
			if (instanceManager != null) {
				instanceManager.removeCache();
				root = instanceManager.getInstance();
				return get(table,rowkey,columns);
			}else{
				throw new RuntimeException("no mergeserver");
			}
		}
	}
	
	public Result<List<RowData>> get(List<GetParam> getParams) {
		CheckParameter.checkNull("params is null", getParams);
		CheckParameter.checkCollection("invalid argument", getParams);
		int count = 0;
		String table = null;
		String rowkey = null;
		for (GetParam getParam : getParams) {
			CheckParameter.checkTableName(getParam.getTableName());
			CheckParameter.checkNull("rowkey is null", getParam.getRowkey());			
			CheckParameter.checkCollection("columns are null", getParam.getColumns());
			CheckParameter.checkNullSetElement("columns have null column", getParam.getColumns());			
			count += getParam.getColumns().size();
			table = getParam.getTableName();
			rowkey = getParam.getRowkey().getRowkey();
		}
		
		ObGetParam getParam = new ObGetParam(count);
		for (GetParam gp : getParams) {
			for (String column : gp.getColumns()) {
				getParam.addCell(new ObCell(gp.getTableName(), gp.getRowkey().getRowkey(), column, null));
			}
		}
		getParam.setReadConsistency(isReadConsistency);
		MergeServer merge = root.getMergeServer(factory,table, rowkey, timeout);
		try {
			Result<List<ObRow>> tmpResult = merge.get(getParam);
			List<RowData> rows = new ArrayList<RowData>(tmpResult.getResult()
					.size());
			for (ObRow row : tmpResult.getResult()) {
				rows.add(Helper.getRowData(row));
			}
			return new Result<List<RowData>>(tmpResult.getCode(), rows);
		} catch (ConnectException e) {
			root.remove(rowkey, merge);
			return get(getParams);
		} catch(EagainException e) {
			instanceManager.removeCache();
			root = instanceManager.getInstance();
			return get(getParams);
		} catch (TimeOutException e) {
			root.remove(rowkey, merge);
			return get(getParams);
		} catch (NoMergeServerException e) {
			if (instanceManager != null) {
				instanceManager.removeCache();
				root = instanceManager.getInstance();
				return get(getParams);
			}else{
				throw new RuntimeException("no mergeserver");
			}
		}
	}
	public Result<Long> count(String table, QueryInfo query) {
		if (query.getColumns().size() == 0) {
			query.addColumn(Const.ASTERISK);
		}
		CheckParameter.checkTableName(table);
		CheckQueryParams.check(query);
		query.setReadConsistency(isReadConsistency);
		
		MergeServer merge = root.getMergeServer(factory,table, query.getStartKey(), timeout);
		try {
			Result<List<ObRow>> tmpResult = merge.scan(table, query, true);
			long count = -1;
			List<ObRow> rows = tmpResult.getResult();
			if (rows != null && rows.size() > 0) {
				ObRow r = rows.get(0);
				Object rv = r.getValueByColumnName(Const.COUNT_AS_NAME);
				if (rv == null || !(rv instanceof Number)) {
					throw new IllegalAccessError("return type not match, expect number, but " + rv.getClass().getName());
				} else {
					count = ((Number)rv).longValue();
				}
			}
			return new Result<Long>(tmpResult.getCode(), count);
		} catch (ConnectException e) {
			root.remove(query.getStartKey(), merge);
			return this.count(table, query);
		} catch(EagainException e) {
			instanceManager.removeCache();
			root = instanceManager.getInstance();
			return this.count(table, query);
		} catch (TimeOutException e) {
			root.remove(query.getStartKey(), merge);
			return this.count(table, query);
		} catch (NoMergeServerException e) {
			if (instanceManager != null) {
				instanceManager.removeCache();
				root = instanceManager.getInstance();
				return this.count(table, query);
			}else{
				throw new RuntimeException("no mergeserver");
			}
		}
	}
	
	public Result<List<RowData>> query(String table, QueryInfo query) {
		CheckParameter.checkTableName(table);
		CheckQueryParams.check(query);
		MergeServer merge = root.getMergeServer(factory,table, query.getStartKey(), timeout);
		try {
			query.setReadConsistency(isReadConsistency);
			Result<List<ObRow>> tmpResult = merge.scan(table, query);
			List<RowData> rows = new ArrayList<RowData>(tmpResult.getResult()
					.size());
			for (ObRow row : tmpResult.getResult()) {
				rows.add(Helper.getRowData(row));
			}
			Result<List<RowData>> result = new Result<List<RowData>>(tmpResult.getCode(), rows);
			return result;
		} catch (ConnectException e) {
			log.warn("query this mergeserver failed,try another");
			root.remove(query.getStartKey(), merge);
			return this.query(table, query);
		} catch(EagainException e) {
			instanceManager.removeCache();
			root = instanceManager.getInstance();
			return this.query(table, query);
		} catch (TimeOutException e) { 
			root.remove(query.getStartKey(), merge);
			return this.query(table, query);
		} catch (NoMergeServerException e) {
			if (instanceManager != null) {
				instanceManager.removeCache();
				root = instanceManager.getInstance();
				return this.query(table, query);
			}else{
				throw new RuntimeException("no mergeserver");
			}
		}
	}
	
	public Result<List<RowData>> scanTabletList(String table, QueryInfo query) {
		CheckParameter.checkTableName(table);
		CheckQueryParams.check(query);
		try {
			Result<List<ObRow>> tmpResult = root.scanTabletList(table, query);
			List<RowData> rows = new ArrayList<RowData>(tmpResult.getResult()
					.size());
			for (ObRow row : tmpResult.getResult()) {
				rows.add(Helper.getRowData(row));
			}
			return new Result<List<RowData>>(tmpResult.getCode(), rows);
		} catch (ConnectException e) {
			// for rootserver's scan tablet list, if connection error, just quit
			throw new RuntimeException(e);
		}
	}
	
	public Result<ObSchemaManagerV2> fetchSchema() {
		return root.getSchema(factory, timeout);
	}
	
	public Result<Boolean> clearActiveMemTale() {
		UpdateServer ups = root.getUpdateServer(factory, timeout);
		return ups.cleanActiveMemTable();		
	}

	public String toString(){
		return root.toString();
	}
	
	private Result<Boolean> apply(ObMutator mutator) {
		UpdateServer update = null;
		Result<Boolean> ret = null;
		int retry_times = 0;
		while (true) {
			if (retry_times > 3)
				break;
			try {
				update = root.getUpdateServer(factory, timeout);
				ret = update.apply(mutator);
				break;
			} catch (EagainException e) {
				log.warn("try again" + e);
				instanceManager.removeCache();
				root = instanceManager.getInstance();
			} catch (ConnectException e) {
				log.warn("cannot connect update server,try again");
				instanceManager.removeCache();
				root = instanceManager.getInstance();
				++fail_times;
			} catch (TimeOutException e) {
				log.warn("timeout,try again");
				instanceManager.removeCache();
				root = instanceManager.getInstance();
				++fail_times;
			}
			
			if (fail_times > MAX_FAIL_TIMES) {
				fail_times = 0;
				continue;
			}
			++retry_times;
		}
		return ret;
	}
	
}