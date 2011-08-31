/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_root_rpc_stub.cpp for ...
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *
 */
#include "rootserver/ob_root_worker.h"
#include "rootserver/ob_root_rpc_stub.h"

namespace oceanbase
{
  namespace rootserver
  {
    using namespace common;
    ObRootRpcStub::ObRootRpcStub() : root_worker_(NULL)
    {
      client_mgr_ = NULL;
    }

    ObRootRpcStub::~ObRootRpcStub()
    {
    }

    int ObRootRpcStub::slave_register(const common::ObServer& master, const common::ObServer& slave_addr, common::ObFetchParam& fetch_param, const int64_t timeout)
    {
      int err = OB_SUCCESS;
      ObDataBuffer data_buff;

      if (NULL == client_mgr_)
      {
        TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
        err = OB_ERROR;
      }
      else
      {
        err = get_thread_buffer_(data_buff);
      }

      // step 1. serialize slave addr
      if (OB_SUCCESS == err)
      {
        err = slave_addr.serialize(data_buff.get_data(), data_buff.get_capacity(),
            data_buff.get_position());
      }

      // step 2. send request to register
      if (OB_SUCCESS == err)
      {
        err = client_mgr_->send_request(master, 
            OB_SLAVE_REG, DEFAULT_VERSION, timeout, data_buff);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "send request to register failed"
              "err[%d].", err);
        }
      }

      // step 3. deserialize the response code
      int64_t pos = 0;
      if (OB_SUCCESS == err)
      {
        ObResultCode result_code;
        err = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
        }
        else
        {
          err = result_code.result_code_;
        }
      }

      // step 3. deserialize fetch param
      if (OB_SUCCESS == err)
      {
        err = fetch_param.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "deserialize fetch param failed, err[%d]", err);
        }
      }

      return err;
    }

    int ObRootRpcStub::get_thread_buffer_(common::ObDataBuffer& data_buffer)
    {
      int ret = OB_SUCCESS;

      ThreadSpecificBuffer::Buffer* rpc_buffer = NULL;

      if (root_worker_ == NULL)
      {
        TBSYS_LOG(ERROR, "root work in rpc stub in NULL");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        rpc_buffer = root_worker_->get_rpc_buffer();
        if (NULL == rpc_buffer)
        {
          TBSYS_LOG(ERROR, "get thread rpc failed, buffer[%p]", rpc_buffer);
          ret = OB_ERROR;
        }
        else
        {
          rpc_buffer->reset();
          data_buffer.set_data(rpc_buffer->current(), rpc_buffer->remain());
        }
      }

      return ret;
    }

    void ObRootRpcStub::set_root_worker(const OBRootWorker* root_worker)
    {
      root_worker_ = root_worker;
    }
  } /* rootserver */
} /* oceanbase */

