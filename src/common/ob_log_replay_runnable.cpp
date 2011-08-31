/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_log_replay_runnable.cpp for ...
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *
 */
#include "ob_log_replay_runnable.h"

#include "ob_define.h"
#include "ob_log_entry.h"

#include "utility.h"

using namespace oceanbase::common;

ObLogReplayRunnable::ObLogReplayRunnable()
{
  is_initialized_ = false;
}

ObLogReplayRunnable::~ObLogReplayRunnable()
{
}

int ObLogReplayRunnable::init(const char* log_dir, const uint64_t log_file_id_start, ObRoleMgr *role_mgr, int64_t replay_wait_time)
{
  int ret = OB_SUCCESS;

  if (is_initialized_)
  {
    TBSYS_LOG(ERROR, "ObLogReplayRunnable has been initialized");
    ret = OB_INIT_TWICE;
  }

  if (OB_SUCCESS == ret)
  {
    if (NULL == role_mgr)
    {
      TBSYS_LOG(ERROR, "Parameter is invalid[role_mgr=%p]", role_mgr);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = log_reader_.init(log_dir, log_file_id_start, true);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "ObLogReader init error[ret=%d], ObLogReplayRunnable init failed", ret);
    }
    else
    {
      role_mgr_ = role_mgr;
      replay_wait_time_ = replay_wait_time;
      set_max_log_file_id(0);
      is_initialized_ = true;
    }
  }

  return ret;
}

void ObLogReplayRunnable::run(tbsys::CThread* thread, void* arg)
{
  int ret = OB_SUCCESS;

  UNUSED(thread);
  UNUSED(arg);

  char* log_data = NULL;
  int64_t data_len = 0;
  LogCommand cmd = OB_LOG_UNKNOWN;
  uint64_t seq;

  if (!is_initialized_)
  {
    TBSYS_LOG(ERROR, "ObLogReplayRunnable has not been initialized");
    ret = OB_NOT_INIT;
  }
  else
  {
    while (!_stop)
    {
      ret = log_reader_.read_log(cmd, seq, log_data, data_len);
      if (OB_READ_NOTHING == ret)
      {
        if (ObRoleMgr::MASTER == role_mgr_->get_role())
        {
          stop();
        }
        else
        {
          usleep(replay_wait_time_);
        }
        continue;
      }
      else if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "ObLogReader read_data error[ret=%d]", ret);
        break;
      }
      else
      {
        if (OB_LOG_NOP != cmd)
        {
          ret = replay(cmd, seq, log_data, data_len);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "replay log error[ret=%d]", ret);
            hex_dump(log_data, data_len, false, TBSYS_LOG_LEVEL_ERROR);
            break;
          }
        }
      }
    }
  }

  // stop server
  if (NULL != role_mgr_) // double check
  {
    if (OB_SUCCESS != ret)
    {
      role_mgr_->set_state(ObRoleMgr::ERROR);
    }
  }
  TBSYS_LOG(INFO, "ObLogReplayRunnable finished[stop=%d ret=%d]", _stop, ret);
}


