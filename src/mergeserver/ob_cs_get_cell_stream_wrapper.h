/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_cs_get_cell_stream_wrapper.h for ...
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#ifndef OCEANBASE_MERGESERVER_OB_CS_GET_CELL_STREAM_WRAPPER_H_ 
#define OCEANBASE_MERGESERVER_OB_CS_GET_CELL_STREAM_WRAPPER_H_
#include "common/ob_client_manager.h"
#include "common/ob_server.h"
#include "common/thread_buffer.h"
#include "mergeserver/ob_ms_rpc_stub.h"
#include "mergeserver/ob_ms_rpc_proxy.h"
#include "mergeserver/ob_ms_schema_manager.h"
#include "mergeserver/ob_ms_tablet_location.h"
#include "mergeserver/ob_ms_get_cell_stream.h"
#include "mergeserver/ob_ms_scan_cell_stream.h"
namespace oceanbase
{
  namespace mergeserver
  {
    class ObMSGetCellStreamWrapper
    {
    public:
      /// @param timeout network timeout
      /// @param update_server address of update server
      ObMSGetCellStreamWrapper(const int64_t retry_times, const int64_t timeout,  
                               const oceanbase::common::ObServer & update_server);
      virtual ~ObMSGetCellStreamWrapper();

      int init(const oceanbase::common::ThreadSpecificBuffer * rpc_buffer, 
               const oceanbase::common::ObClientManager * rpc_frame);

      /// @fn get cell stream used for join
      oceanbase::mergeserver::ObMSGetCellStream *get_ups_get_cell_stream();
      /// @fn get cell stream used for merge
      oceanbase::mergeserver::ObMSScanCellStream *get_ups_scan_cell_stream();
    private:
      oceanbase::common::ObServer root_server_;
      oceanbase::common::ObServer merge_server_;
      oceanbase::mergeserver::ObMergerRpcStub  rpc_stub_;
      oceanbase::mergeserver::ObMergerRpcProxy rpc_proxy_;
      /// @property this is a big one
      oceanbase::mergeserver::ObMergerSchemaManager *schema_mgr_;
      oceanbase::mergeserver::ObMergerTabletLocationCache location_cache_;
      oceanbase::mergeserver::ObMSGetCellStream get_cell_stream_;
      oceanbase::mergeserver::ObMSScanCellStream scan_cell_stream_;
    };
  }
}   
#endif /* OCEANBASE_MERGESERVER_OB_CS_GET_CELL_STREAM_WRAPPER_H_ */


