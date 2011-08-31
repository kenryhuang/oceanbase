package com.taobao.oceanbase.server;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.oceanbase.ClientImpl;
import com.taobao.oceanbase.network.Server;
import com.taobao.oceanbase.network.exception.EagainException;
import com.taobao.oceanbase.network.packet.BasePacket;
import com.taobao.oceanbase.network.packet.GetPacket;
import com.taobao.oceanbase.network.packet.ObScanRequest;
import com.taobao.oceanbase.network.packet.ResponsePacket;
import com.taobao.oceanbase.util.Const;
import com.taobao.oceanbase.util.Helper;
import com.taobao.oceanbase.util.NavigableCache;
import com.taobao.oceanbase.vo.QueryInfo;
import com.taobao.oceanbase.vo.Result;
import com.taobao.oceanbase.vo.ResultCode;
import com.taobao.oceanbase.vo.RowData;
import com.taobao.oceanbase.vo.inner.ObAggregateColumn;
import com.taobao.oceanbase.vo.inner.ObBorderFlag;
import com.taobao.oceanbase.vo.inner.ObCell;
import com.taobao.oceanbase.vo.inner.ObGetParam;
import com.taobao.oceanbase.vo.inner.ObGroupByParam;
import com.taobao.oceanbase.vo.inner.ObRange;
import com.taobao.oceanbase.vo.inner.ObRow;
import com.taobao.oceanbase.vo.inner.ObScanParam;
import com.taobao.oceanbase.vo.inner.ObAggregateColumn.ObAggregateFuncType;

public class MergeServer {
	
	private static final Log log = LogFactory.getLog(MergeServer.class);

	private Server server;
	private AtomicInteger request = new AtomicInteger(0);
	
//	private class CacheKey {
//		CacheKey(ObScanParam p) {
//			param = p;
//		}
//		
//		@Override
//		public boolean equals(Object o) {
//			if (! (o instanceof CacheKey)){
//				return false;
//			}
//			CacheKey rk = (CacheKey)o;
//			ObScanParam r = rk.param;
//			
//			if (param.getSize() == r.getSize()){
//				int serialize_size = param.getSize();
//				ByteBuffer lb = ByteBuffer.allocate(serialize_size);
//				ByteBuffer rb = ByteBuffer.allocate(serialize_size);
//				param.serialize(lb);
//				r.serialize(rb);
//				return lb.compareTo(rb) == 0;
////				if (param.getBeginVersion() == r.getBeginVersion() &&
////					param.getEndVersion() == r.getEndVersion() &&
////					param.getTable() == r.getTable()  &&
////					param.getRange().getStartKey().compareTo(r.getRange().getStartKey()) == 0 &&
////					param.getRange().getEndKey().compareTo(r.getRange().getEndKey()) == 0 &&
////					param.getRange().getBorderFlag().getData() == r.getRange().getBorderFlag().getData()
////					) {
////					return true;
////				}
//			}
//			return false;
//		}
//		@Override
//		public int hashCode() {
//			int serialize_size = param.getSize();
//			ByteBuffer b = ByteBuffer.allocate(serialize_size);
//			param.serialize(b);
//			return Helper.FNVHash(b.array());
//		}
//	
//		
//		public ObScanParam param;
//		
//	}
//	
//	private class CacheValue{
//		CacheValue(Result<List<ObRow>> r) {
//			result = r;
//			time = System.currentTimeMillis();
//		}
//		public Result<List<ObRow>> result;
//		public long time;
//	};
//	
//	private ConcurrentHashMap<CacheKey,CacheValue> valueCache = new ConcurrentHashMap<CacheKey,CacheValue>();

	public MergeServer(Server server) {
		this.server = server;
	}

//	public Result<RowData> get(String table, String rowkey, Set<String> columns) {
//		ObGetParam param = new ObGetParam(columns.size());
//		for (String column : columns) {
//			param.addCell(new ObCell(table, rowkey, column, null));
//		}
//		BasePacket packet = new GetPacket(getRequest(), param);
//		ResponsePacket response = server.commit(packet);
//		List<ObRow> rows = response.getScanner().getRows();
//		ResultCode code = ResultCode.getResultCode(response.getResultCode());
//		if (code == ResultCode.OB_NOT_MASTER)
//			throw new EagainException("consistey read not on master,clean cache and try again");
//		if(code != ResultCode.OB_SUCCESS)
//			return new Result<RowData>(code, null);
//		if (rows.size() != 1)
//			throw new RuntimeException("expected 1 record : " + rows);
//		//row not exist
//		if (rows.get(0).getColumns().size() == 0)
//			return new Result<RowData>(code, null);
//		RowData data = Helper.getRowData(rows.get(0));
//		return new Result<RowData>(code, data);
//	}
	
	public Result<List<ObRow>> get(ObGetParam getParam) {
		BasePacket packet = new GetPacket(getRequest(), getParam);
		ResponsePacket response = server.commit(packet);
		List<ObRow> rows = response.getScanner().getRows();
		ResultCode code = ResultCode.getResultCode(response.getResultCode());
		if (code == ResultCode.OB_NOT_MASTER)
			throw new EagainException("consistey read not on master,clean cache and try again");
		return new Result<List<ObRow>>(code, rows.size() > 0 ? rows : null);
	}
	public Result<List<ObRow>> scan(String table, QueryInfo query) {
		return scan(table, query, false);
	}
	public Result<List<ObRow>> scan(String table, QueryInfo query, boolean isCount) {
		boolean start = query.isInclusiveStart();
		boolean end = query.isInclusiveEnd();
		boolean min = query.isMinValue();
		boolean max = query.isMaxValue();
		ObRange range = new ObRange(query.getStartKey(), query.getEndKey(),
				new ObBorderFlag(start, end, min, max));
		ObScanParam param = new ObScanParam();
		param.set(table, range);
		param.setReadConsistency(query.isReadConsistency());
		
		if (query.getPageNum() == 0) {
			param.setLimit(0, query.getLimit());
		} else {
			param.setLimit(query.getPageSize() * (query.getPageNum() - 1),
					Math.min(query.getLimit(), query.getPageSize()));
		}
		for (String column : query.getColumns()) {
			param.addColumns(column);
		}
		if (isCount) {
			ObGroupByParam gb = new ObGroupByParam();
			gb.addAggregateColumn(new ObAggregateColumn(Const.ASTERISK,
					Const.COUNT_AS_NAME, ObAggregateFuncType.COUNT));
			param.setGroupByParam(gb);
		}
		for (Map.Entry<String, Boolean> entry : query.getOrderBy().entrySet()) {
			param.addOrderBy(entry.getKey(), entry.getValue());
		}
		if (query.getFilter() != null) {
			if (query.getFilter().isValid()) {
				param.setFilter(query.getFilter());
			} else {
				throw new IllegalArgumentException("condition is invalid");
			}
		}
		param.setScanDirection(query.getScanFlag());
//		if (valueCache.containsKey(new CacheKey(param))) { 
//			if ((System.currentTimeMillis() - valueCache.get(new CacheKey(param)).time) < 5000) {
//				log.info("hit client cache");
//				return valueCache.get(new CacheKey(param)).result;
//			}else {
//				log.info("cache expired");
//				valueCache.remove(new CacheKey(param));
//			}
//		}
		//param.setGroupByParam(query.getGroupbyParam());
		return scan(param);
	}

	private Result<List<ObRow>> scan(ObScanParam param) {
		BasePacket packet = new ObScanRequest(getRequest(), param);
		//log.info("start actually scan" + System.currentTimeMillis());
		ResponsePacket response = server.commit(packet);
		//log.info("received from server cost@" + System.currentTimeMillis());
		ResultCode code = ResultCode.getResultCode(response.getResultCode());
		if (code == ResultCode.OB_NOT_MASTER)
			throw new EagainException("consistey read not on master,clean cache and try again");
		//log.info("received from server cost@" + System.currentTimeMillis());
		Result<List<ObRow>> result =  new Result<List<ObRow>>(code, response.getScanner().getRows());
//		if (code == ResultCode.OB_SUCCESS){
//			if (valueCache.size() > 200) {
//				HashSet<CacheKey> tmpSet = new HashSet<CacheKey>();
//				long current = System.currentTimeMillis();
//				for ( Map.Entry<CacheKey, CacheValue> e : valueCache.entrySet()) {
//					if (current - e.getValue().time > 5000){
//						tmpSet.add(e.getKey());
//					}
//				}
//				valueCache.entrySet().removeAll(tmpSet);
//				tmpSet.clear();
//			}
//			log.info("put to cache");
//			valueCache.put(new CacheKey(param), new CacheValue(result));
//		}
		return result;
	}

	private int getRequest() {
		int seq = request.incrementAndGet();
		return seq != 0 ? seq : getRequest();
	}

	public int hashCode() {
		return server.getAddress().hashCode();
	}

	public boolean equals(Object server) {
		if (server != null && server instanceof Server) {
			return this.hashCode() == server.hashCode();
		}
		return false;
	}

	protected SocketAddress getAddress() {
		return server.getAddress();
	}
	
	@Override
	public String toString() {
		return server.toString();
	}
}