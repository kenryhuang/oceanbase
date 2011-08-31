/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_ms_rpc_proxy.h for ...
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *
 */
#ifndef OB_MERGER_RPC_PROXY_H_
#define OB_MERGER_RPC_PROXY_H_

#include "tbsys.h"
#include "common/ob_string.h"
#include "common/ob_server.h"
#include "common/ob_iterator.h"

namespace oceanbase
{
  namespace common
  {
    class ObGetParam;
    class ObScanner;
    class ObRange;
    class ObScanParam;
    class ObSchemaManagerV2;
  }

  namespace mergeserver
  {
    class ObMergerRpcStub;
    class ObMergerSchemaManager;
    class ObMergerTabletLocation;
    class ObMergerServiceMonitor;
    class ObMergerTabletLocationList;
    class ObMergerTabletLocationCache;
    // this class is the outer interface class based on rpc stub class,
    // which is the real network interactor,
    // it encapsulates all interface for data getting operation, 
    // that sometime need rpc calling to get the right version data.
    // if u want to add a new interactive interface, 
    // please at first add the real detail at rpc stub class
    class ObMergerRpcProxy
    {
    public:
      //
      ObMergerRpcProxy();
      // construct func
      // param  @retry_times rpc call retry times when error occured
      //        @timeout every rpc call timeout
      //        @root_server root server addr for some interface
      //        @update_server update server addr for some interface
      //        @mege_server merge server local host addr for input
      ObMergerRpcProxy(const int64_t retry_times, const int64_t timeout, 
          const common::ObServer & root_server, const common::ObServer & update_server,
          const common::ObServer & merge_server);
      
      virtual ~ObMergerRpcProxy();

    public:
      // param  @rpc_buff rpc send and response buff
      //        @rpc_frame client manger for network interaction
      int init(ObMergerRpcStub * rpc_stub, ObMergerSchemaManager * schema, 
               ObMergerTabletLocationCache * cache, ObMergerServiceMonitor * monitor = NULL);

      // register merge server
      // param  @merge_server localhost merge server addr
      int register_merger(const common::ObServer & merge_server);
      
      // merge server heartbeat with root server
      // param  @merge_server localhost merge server addr
      int async_heartbeat(const common::ObServer & merge_server);

      static const int64_t LOCAL_NEWEST = 0;    // local cation newest version

      // get the scheam data according to the timetamp, in some cases depend on the timestamp value
      // if timestamp is LOCAL_NEWEST, it meanse only get the local latest version
      // otherwise, it means that at first find in the local versions, if not exist then need rpc
      // waring: after using manager, u must release this schema version for washout
      // param  @timeout every rpc call timeout
      //        @manager the real schema pointer returned
      int get_schema(const int64_t timestamp, const common::ObSchemaManagerV2 ** manager);

      // fetch new schema if find new version
      int fetch_schema_version(int64_t & timestamp);

      // waring: release schema after using for dec the manager reference count
      //        @manager the real schema pointer returned
      int release_schema(const common::ObSchemaManagerV2 * manager);

      // some get func as temperary interface
      const ObMergerRpcStub * get_rpc_stub() const
      {
        return rpc_stub_;
      }
      int64_t get_rpc_timeout(void) const
      {
        return rpc_timeout_;
      }
      const common::ObServer & get_root_server(void) const
      {
        return root_server_;
      }
      const common::ObServer & get_update_server(void) const
      {
        return update_server_;
      }

    public:  
      // retry interval time
      static const int64_t RETRY_INTERVAL_TIME = 20; // 20 ms usleep

      // least fetch schema time interval
      static const int64_t LEAST_FETCH_SCHEMA_INTERVAL = 1000 * 1000; // 1s 
      
      // get data from one chunk server, which is positioned according to the param
      // param  @get_param get param
      //        @list tablet location list as output
      //        @scanner return result
      virtual int cs_get(const common::ObGetParam & get_param, 
          ObMergerTabletLocation & addr, common::ObScanner & scanner, common::ObIterator * &it_out);

      // scan data from one chunk server, which is positioned according to the param
      // the outer class cellstream can give the whole data according to scan range param
      // param  @scan_param scan param
      //        @list tablet location list as output
      //        @scanner return result
      virtual int cs_scan(const common::ObScanParam & scan_param, 
          ObMergerTabletLocation & addr, common::ObScanner & scanner, common::ObIterator * &it_out);

      // get data from update server
      // param  @get_param get param
      //        @scanner return result
      virtual int ups_get(const ObMergerTabletLocation & addr,
          const common::ObGetParam & get_param, common::ObScanner & scanner);

      // scan data from update server
      // param  @scan_param get param
      //        @scanner return result
      virtual int ups_scan(const ObMergerTabletLocation & addr,
          const common::ObScanParam & scan_param, common::ObScanner & scanner);

      // lock and check whether need fetch new schema
      // param  @timestamp new schema timestamp
      //        @manager the new schema pointer
      int fetch_new_schema(const int64_t timestamp, const common::ObSchemaManagerV2 ** manager);
      
      // output scanner result for debug
      static void output(common::ObScanner & result);

    private:
      // check inner stat
      bool check_inner_stat(void) const; 
      
      // check scan param
      // param  @scan_param scan parameter
      static bool check_scan_param(const common::ObScanParam & scan_param);

      // check range param
      // param  @range_param range parameter 
      static bool check_range_param(const common::ObRange & range_param);

      // get new schema through root server rpc call
      // param  @timestamp old schema timestamp
      //        @manager the new schema pointer
      int get_new_schema(const int64_t timestamp, const common::ObSchemaManagerV2 ** manager);

      // get search cache key
      // warning: the seach_key buffer is allocated in this function if it's not range.start_key_,
      // after use must dealloc search_key.ptr() in user space if temp is not null
      int get_search_key(const common::ObScanParam & scan_param, common::ObString & search_key, char ** new_buffer);

      // resest the search key's buffer to null after using it
      void reset_search_key(common::ObString & search_key, char * buffer);

      // del cache item according to tablet range
      int del_cache_item(const common::ObScanParam & scan_param);
      // del cache item according to table id + search rowkey
      int del_cache_item(const uint64_t table_id, const common::ObString & search_key);
      
      // update cache item according to tablet range
      int update_cache_item(const common::ObScanParam & scan_param, const ObMergerTabletLocationList & list);
      // update cache item according to table id + search rowkey
      int update_cache_item(const uint64_t table_id, const common::ObString & search_key, 
          const ObMergerTabletLocationList & list);

      // set item addr invalid according to tablet range
      int set_item_invalid(const common::ObScanParam & scan_param, const ObMergerTabletLocation & addr);
      // set item addr invalid according to table id + search rowkey
      int set_item_invalid(const uint64_t table_id, const common::ObString & search_key, 
          const ObMergerTabletLocation & list);

      // get tablet location according the table_id and row_key [include mode]
      // at first search the cache, then ask the root server, at last update the cache
      // param  @table_id table id of tablet
      //        @row_key row key included in tablet range
      //        @location tablet location
      int get_tablet_location(const uint64_t table_id, const common::ObString & row_key,
          ObMergerTabletLocationList & location);

      // get the first tablet location according range's first table_id.row_key [range.mode]
      // param  @range scan range
      //        @location the first range tablet location
      int get_first_tablet_location(const common::ObScanParam & param,
          ObMergerTabletLocationList & location);

      // scan tablet location through root_server rpc call
      // param  @table_id table id of root table 
      //        @row_key row key included in tablet range
      //        @location tablet location
      int scan_root_table(const uint64_t table_id, const common::ObString & row_key,
          ObMergerTabletLocationList & location);
      
      /// max len
      static const int64_t MAX_RANGE_LEN = 128;
      static const int64_t MAX_ROWKEY_LEN = 8;
    
    private:
      bool init_;                                   // rpc proxy init stat
      int64_t rpc_timeout_;                         // rpc call timeout
      int64_t rpc_retry_times_;                     // rpc retry times
      char max_range_[MAX_RANGE_LEN];               // for debug print range string
      char max_rowkey_[MAX_ROWKEY_LEN];             // 8 0xFF as max rowkey
      common::ObServer root_server_;                // root server addr
      common::ObServer merge_server_;               // merge server addr
      common::ObServer update_server_;              // update server addr
      ObMergerServiceMonitor * monitor_;            // service monitor data
      const ObMergerRpcStub * rpc_stub_;            // rpc stub bottom module
      ObMergerSchemaManager * schema_manager_;      // merge server schema cache
      int64_t fetch_schema_timestamp_;              // last fetch schema from root timestamp
      ObMergerTabletLocationCache * tablet_cache_;  // merge server tablet location cache
      tbsys::CThreadMutex schema_lock_;             // lock for update schema manager
      tbsys::CThreadMutex cache_lock_;              // lock for update chunk server cache
    };
  }
}



#endif // OB_MERGER_RPC_PROXY_H_


