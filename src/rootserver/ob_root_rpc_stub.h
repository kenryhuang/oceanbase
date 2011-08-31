/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_root_rpc_stub.h for ...
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *
 */
#ifndef OCEANBASE_ROOT_RPC_STUB_H_
#define OCEANBASE_ROOT_RPC_STUB_H_

#include "common/ob_fetch_runnable.h"
#include "common/ob_common_rpc_stub.h"

namespace oceanbase
{
  namespace rootserver
  {
    class OBRootWorker;
    class ObRootRpcStub : public common::ObCommonRpcStub
    {
      public:
        ObRootRpcStub();
        ~ObRootRpcStub();

      public:
        int slave_register(const common::ObServer& master, const common::ObServer& slave_addr, common::ObFetchParam& fetch_param, const int64_t timeout);
        void set_root_worker(const OBRootWorker* root_worker);

      private:
        int get_thread_buffer_(common::ObDataBuffer& data_buffer);
    
      private:
        static const int32_t DEFAULT_VERSION = 1;
        const OBRootWorker* root_worker_;
    };
  } /* rootserver */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_ROOT_RPC_STUB_H_ */

