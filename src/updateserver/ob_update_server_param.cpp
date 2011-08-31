/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_update_server_param.cpp for ...
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *
 */
#include "common/compress/ob_compressor.h"
#include "common/ob_thread_mempool.h"
#include "ob_update_server_param.h"

namespace
{
  const char* OBUPS_UPS_SECTION  = "update_server";
  const char* OBUPS_VIP = "vip";
  const char* OBUPS_PORT = "port";
  const char* OBUPS_DEVNAME = "dev_name";
  const char* OBUPS_APP_NAME = "app_name";
  const char* OBUPS_LOG_DIR_PATH = "log_dir_path";
  const char* OBUPS_LOG_SIZE_MB = "log_size_mb";
  const char* OBUPS_READ_THREAD_COUNT = "read_thread_count";
  const char* OBUPS_READ_TASK_QUEUE_SIZE = "read_task_queue_size";
  const char* OBUPS_WRITE_TASK_QUEUE_SIZE = "write_task_queue_size";
  const char* OBUPS_WRITE_GROUP_COMMIT_NUM = "write_group_commit_num";
  const char* OBUPS_LOG_TASK_QUEUE_SIZE = "log_task_queue_size";
  const char* OBUPS_LEASE_TASK_QUEUE_SIZE = "lease_task_queue_size";
  const char* OBUPS_STORE_THREAD_QUEUE_SIZE = "store_thread_queue_size";
  const char* OBUPS_LOG_SYNC_TYPE = "log_sync_type";
  const char* OBUPS_FETCH_SCHEMA_TIMES = "fetch_schema_times";
  const char* OBUPS_FETCH_SCHEMA_TIMEOUT_US = "fetch_schema_timeout_us";
  const char* OBUPS_RESP_ROOT_TIMES = "resp_root_times";
  const char* OBUPS_RESP_ROOT_TIMEOUT_US = "resp_root_timeout_us";
  const char* OBUPS_STATE_CHECK_PERIOD_US = "state_check_period_us";
  const char* OBUPS_REGISTER_TIMES = "register_times";
  const char* OBUPS_REGISTER_TIMEOUT_US = "register_timeout_us";
  const char* OBUPS_REPLAY_WAIT_TIME_US = "replay_wait_time_us";
  const char* OBUPS_LOG_SYNC_LIMIT_KB = "log_sync_limit_kb";
  const char* OBUPS_LOG_SYNC_RETRY_TIMES = "log_sync_retry_times";
  const char* OBUPS_TOTAL_MEMORY_LIMIT = "total_memory_limit";
  const char* OBUPS_TABLE_MEMORY_LIMIT = "table_memory_limit";
  const char* OBUPS_LEASE_INTERVAL_US = "lease_interval_us";
  const char* OBUPS_LEASE_RESERVED_TIME_US = "lease_reserved_time_us";
  const char* OBUPS_LEASE_ON = "lease_on";
  const char* OBUPS_LOG_SYNC_TIMEOUT_US = "log_sync_timeout_us";
  const char* OBUPS_PACKET_MAX_TIMEWAIT = "packet_max_timewait";
  const char* OBUPS_MAX_THREAD_MEMBLOCK_NUM = "max_thread_memblock_num";
  const char* OBUPS_LOG_FETCH_OPTION = "log_fetch_option";
  const char* OBUPS_TRANS_PROC_TIME_WARN_US = "transaction_process_time_warning_us";
  const char* OBUPS_STORE_ROOT = "store_root";
  const char* OBUPS_RAID_REGEX = "raid_regex";
  const char* OBUPS_DIR_REGEX = "dir_regex";
  const char* OBUPS_BLOCKCACHE_SIZE_MB = "blockcache_size_mb";
  const char* OBUPS_BLOCKINDEX_CACHE_SIZE_MB = "blockindex_cache_size_mb";
  const char* OBUPS_ACTIVE_MEM_LIMIT_GB = "active_mem_limit_gb";
  const char* OBUPS_MINOR_NUM_LIMIT = "minor_num_limit";
  const char* OBUPS_FROZEN_MEM_LIMIT_GB = "frozen_mem_limit_gb";
  const char* OBUPS_SSTABLE_TIME_LIMIT_S = "sstable_time_limit_s";
  const char* OBUPS_SLAVE_SYNC_SSTABLE_NUM = "slave_sync_sstable_num";
  const char* OBUPS_SSTABLE_COMPRESSOR_NAME = "sstable_compressor_name";
  const char* OBUPS_SSTABLE_BLOCK_SIZE = "sstable_block_size";
  const char* OBUPS_MAJOR_FREEZE_DUTY_TIME = "major_freeze_duty_time";
  const char* OBUPS_MAJOR_FREEZE_DUTY_TIME_FROMAT = "%H:%M";
  const char* OBUPS_MIN_MAJOR_FREEZE_INTERVAL_S = "min_major_freeze_interval_s";
  const char* OBUPS_REPLAY_CHECKSUM_FLAG = "replay_checksum_flag";

  const char* OBUPS_RS_SECTION  = "root_server";

  const char* OBUPS_STANDALONE_SECTION = "standalone";
  const char* OBUPS_STANDALONE_SCHEMA = "test_schema";
  const char* OBUPS_STANDALONE_SCHEMA_VERSION = "test_schema_version";

  const char* DEFAULT_LOG_FETCH_OPTION = "-e \"ssh -oStrictHostKeyChecking=no\" -avz --inplace";

  const char* OBUPS_UPS_INNER_PORT = "ups_inner_port";
  const char* OBUPS_LOW_PRIV_NETWORK_LOWER_LIMIT = "low_priv_network_lower_limit";
  const char* OBUPS_LOW_PRIV_NETWORK_UPPER_LIMIT = "low_priv_network_upper_limit";
  const char* OBUPS_HIGH_PRIV_MAX_WAIT_TIME_US = "high_priv_max_wait_time_us";
  const char* OBUPS_HIGH_PRIV_ADJUST_TIME_US = "high_priv_adjust_time_us";
  const char* OBUPS_HIGH_PRIV_WAIT_TIME_US = "high_priv_wait_time_us";
}

namespace oceanbase
{
  namespace updateserver
  {
    using namespace oceanbase::common;

    ObUpdateServerParam::ObUpdateServerParam()
    {
      memset(this, 0x00, sizeof(*this));
      memset(&major_freeze_duty_time_, -1, sizeof(major_freeze_duty_time_));
    }

    ObUpdateServerParam::~ObUpdateServerParam()
    {
    }

    int ObUpdateServerParam::load_from_config()
    {
      int err = OB_SUCCESS;

      ups_port_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_PORT, 0);
      if (ups_port_ <= 0)
      {
        TBSYS_LOG(ERROR, "update server port (%d) cannot <= 0.", ups_port_);
        err = OB_ERROR;
      }

      if (OB_SUCCESS == err)
      {
        err = load_string(dev_name_, OB_MAX_DEV_NAME_SIZE, OBUPS_UPS_SECTION, OBUPS_DEVNAME, true);
      }

      if (OB_SUCCESS == err)
      {
        err = load_string(app_name_, OB_MAX_APP_NAME_LENGTH,
            OBUPS_UPS_SECTION, OBUPS_APP_NAME, true);
      }

      if (OB_SUCCESS == err)
      {
        err = load_string(log_dir_path_, OB_MAX_FILE_NAME_LENGTH, OBUPS_UPS_SECTION,
            OBUPS_LOG_DIR_PATH, true);
      }

      if (OB_SUCCESS == err)
      {
        log_size_mb_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_LOG_SIZE_MB,
            DEFAULT_LOG_SIZE_MB);
      }

      if (OB_SUCCESS == err)
      {
        err = load_string(rs_vip_, OB_MAX_IP_SIZE, OBUPS_RS_SECTION, OBUPS_VIP, true);
      }

      if (OB_SUCCESS == err)
      {
        rs_port_ = TBSYS_CONFIG.getInt(OBUPS_RS_SECTION, OBUPS_PORT, 0);
      }

      if (OB_SUCCESS == err)
      {
        fetch_schema_times_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION,
            OBUPS_FETCH_SCHEMA_TIMES, DEFAULT_FETCH_SCHEMA_TIMES);
      }

      if (OB_SUCCESS == err)
      {
        fetch_schema_timeout_us_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION,
            OBUPS_FETCH_SCHEMA_TIMEOUT_US, DEFAULT_FETCH_SCHEMA_TIMEOUT_US);
      }

      if (OB_SUCCESS == err)
      {
        resp_root_times_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION,
            OBUPS_RESP_ROOT_TIMES, DEFAULT_RESP_ROOT_TIMES);
      }

      if (OB_SUCCESS == err)
      {
        resp_root_timeout_us_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION,
            OBUPS_RESP_ROOT_TIMEOUT_US, DEFAULT_RESP_ROOT_TIMEOUT_US);
      }

      if (OB_SUCCESS == err)
      {
        state_check_period_us_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION,
            OBUPS_STATE_CHECK_PERIOD_US, DEFAULT_STATE_CHECK_PERIOD_US);
      }

      if (OB_SUCCESS == err)
      {
        err = load_string(ups_vip_, OB_MAX_IP_SIZE, OBUPS_UPS_SECTION,
            OBUPS_VIP, true);
      }

      if (OB_SUCCESS == err)
      {
        log_sync_type_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION,
            OBUPS_LOG_SYNC_TYPE, DEFAULT_LOG_SYNC_TYPE);
      }

      if (OB_SUCCESS == err)
      {
        log_sync_limit_kb_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION,
            OBUPS_LOG_SYNC_LIMIT_KB, DEFAULT_LOG_SYNC_LIMIT_KB);
      }

      if (OB_SUCCESS == err)
      {
        log_sync_retry_times_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION,
            OBUPS_LOG_SYNC_RETRY_TIMES, DEFAULT_LOG_SYNC_RETRY_TIMES);
      }

      if (OB_SUCCESS == err)
      {
        total_memory_limit_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION,
            OBUPS_TOTAL_MEMORY_LIMIT, DEFAULT_TOTAL_MEMORY_LIMIT);
        total_memory_limit_ *= GB_UNIT;
      }

      if (OB_SUCCESS == err)
      {
        table_memory_limit_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION,
            OBUPS_TABLE_MEMORY_LIMIT, DEFAULT_TABLE_MEMORY_LIMIT);
        table_memory_limit_ *= GB_UNIT;
      }

      if (OB_SUCCESS == err)
      {
        read_thread_count_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_READ_THREAD_COUNT,
            DEFAULT_READ_THREAD_COUNT);
        if (read_thread_count_ <= 0)
        {
          TBSYS_LOG(ERROR, "read thread count (%ld) cannot <= 0.", read_thread_count_);
          err = OB_ERROR;
        }
      }

      if (OB_SUCCESS == err)
      {
        read_task_queue_size_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_READ_TASK_QUEUE_SIZE,
            DEFAULT_TASK_READ_QUEUE_SIZE);
      }

      if (OB_SUCCESS == err)
      {
        write_task_queue_size_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_WRITE_TASK_QUEUE_SIZE,
            DEFAULT_TASK_WRITE_QUEUE_SIZE);
      }

      if (OB_SUCCESS == err)
      {
        write_thread_batch_num_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION,
            OBUPS_WRITE_GROUP_COMMIT_NUM, DEFAULT_WRITE_GROUP_COMMIT_NUM);
      }

      if (OB_SUCCESS == err)
      {
        log_task_queue_size_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_LOG_TASK_QUEUE_SIZE,
            DEFAULT_TASK_LOG_QUEUE_SIZE);
      }

      if (OB_SUCCESS == err)
      {
        lease_task_queue_size_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_LEASE_TASK_QUEUE_SIZE,
            DEFAULT_TASK_LEASE_QUEUE_SIZE);
      }

      if (OB_SUCCESS == err)
      {
        store_thread_queue_size_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_STORE_THREAD_QUEUE_SIZE,
            DEFAULT_STORE_THREAD_QUEUE_SIZE);
      }

      if (OB_SUCCESS == err)
      {
        register_times_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_REGISTER_TIMES,
            DEFAULT_REGISTER_TIMES);
      }

      if (OB_SUCCESS == err)
      {
        register_timeout_us_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_REGISTER_TIMEOUT_US,
            DEFAULT_REGISTER_TIMEOUT_US);
      }

      if (OB_SUCCESS == err)
      {
        replay_wait_time_us_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_REPLAY_WAIT_TIME_US,
            DEFAULT_REPLAY_WAIT_TIME_US);
      }

      if (OB_SUCCESS == err)
      {
        lease_interval_us_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_LEASE_INTERVAL_US,
            DEFAULT_LEASE_INTERVAL_US);
      }

      if (OB_SUCCESS == err)
      {
        lease_reserved_time_us_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_LEASE_RESERVED_TIME_US,
            DEFAULT_LEASE_RESERVED_TIME_US);
      }

      if (OB_SUCCESS == err)
      {
        log_sync_timeout_us_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_LOG_SYNC_TIMEOUT_US,
            DEFAULT_LOG_SYNC_TIMEOUT_US);
      }

      if (OB_SUCCESS == err)
      {
        packet_max_timewait_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_PACKET_MAX_TIMEWAIT,
            DEFAULT_PACKET_MAX_TIMEWAIT);
      }

      if (OB_SUCCESS == err)
      {
        lease_on_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_LEASE_ON,
            DEFAULT_LEASE_ON);
      }

      if (OB_SUCCESS == err)
      {
        max_thread_memblock_num_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_MAX_THREAD_MEMBLOCK_NUM,
            DEFAULT_MAX_THREAD_MEMBLOCK_NUM);
        thread_mempool_set_max_free_num(max_thread_memblock_num_);
      }

      if (OB_SUCCESS == err)
      {
        trans_proc_time_warn_us_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_TRANS_PROC_TIME_WARN_US,
            DEFAULT_TRANS_PROC_TIME_WARN_US);
      }

      if (OB_SUCCESS == err)
      {
        err = load_string(store_root_, OB_MAX_FILE_NAME_LENGTH, OBUPS_UPS_SECTION, OBUPS_STORE_ROOT, true);
      }

      if (OB_SUCCESS == err)
      {
        err = load_string(raid_regex_, OB_MAX_FILE_NAME_LENGTH, OBUPS_UPS_SECTION, OBUPS_RAID_REGEX, true);
      }

      if (OB_SUCCESS == err)
      {
        err = load_string(dir_regex_, OB_MAX_FILE_NAME_LENGTH, OBUPS_UPS_SECTION, OBUPS_DIR_REGEX, true);
      }

      if (OB_SUCCESS == err)
      {
        err = load_string(sstable_compressor_name_, OB_MAX_FILE_NAME_LENGTH, OBUPS_UPS_SECTION, OBUPS_SSTABLE_COMPRESSOR_NAME, true);
        if (OB_SUCCESS == err)
        {
          ObCompressor *compressor = create_compressor(sstable_compressor_name_);
          if (NULL == compressor)
          {
            TBSYS_LOG(ERROR, "cannot load compressor library name=[%s]", sstable_compressor_name_);
            err = OB_ERROR;
          }
          else
          {
            destroy_compressor(compressor);
          }
        }
      }

      if (OB_SUCCESS == err)
      {
        blockcache_size_mb_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_BLOCKCACHE_SIZE_MB,
            DEFAULT_BLOCKCACHE_SIZE_MB);
      }

      if (OB_SUCCESS == err)
      {
        blockindex_cache_size_mb_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_BLOCKINDEX_CACHE_SIZE_MB,
            DEFAULT_BLOCKINDEX_CACHE_SIZE_MB);
      }

      if (OB_SUCCESS == err)
      {
        active_mem_limit_gb_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_ACTIVE_MEM_LIMIT_GB,
            DEFAULT_ACTIVE_MEM_LIMIT_GB);
      }

      if (OB_SUCCESS == err)
      {
        minor_num_limit_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_MINOR_NUM_LIMIT,
            DEFAULT_MINOR_NUM_LIMIT);
      }

      if (OB_SUCCESS == err)
      {
        frozen_mem_limit_gb_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_FROZEN_MEM_LIMIT_GB,
            DEFAULT_FROZEN_MEM_LIMIT_GB);
      }

      if (OB_SUCCESS == err)
      {
        sstable_time_limit_s_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_SSTABLE_TIME_LIMIT_S,
            DEFAULT_SSTABLE_TIME_LIMIT_S);
      }

      if (OB_SUCCESS == err)
      {
        min_major_freeze_interval_s_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_MIN_MAJOR_FREEZE_INTERVAL_S,
            DEFAULT_MIN_MAJOR_FREEZE_INTERVAL_S);
      }

      if (OB_SUCCESS == err)
      {
        replay_checksum_flag_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_REPLAY_CHECKSUM_FLAG,
            DEFAULT_REPLAY_CHECKSUM_FLAG);
      }

      if (OB_SUCCESS == err)
      {
        slave_sync_sstable_num_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_SLAVE_SYNC_SSTABLE_NUM,
            DEFAULT_SLAVE_SYNC_SSTABLE_NUM);
      }

      if (OB_SUCCESS == err)
      {
        sstable_block_size_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_SSTABLE_BLOCK_SIZE,
            DEFAULT_SSTABLE_BLOCK_SIZE);
      }

      if (OB_SUCCESS == err)
      {
        const char *str = TBSYS_CONFIG.getString(OBUPS_UPS_SECTION, OBUPS_MAJOR_FREEZE_DUTY_TIME);
        if (NULL != str)
        {
          strptime(str, OBUPS_MAJOR_FREEZE_DUTY_TIME_FROMAT, &major_freeze_duty_time_);
        }
      }

      if (OB_SUCCESS == err)
      {
        err = load_string(fetch_opt_, sizeof(fetch_opt_),
            OBUPS_UPS_SECTION, OBUPS_LOG_FETCH_OPTION, false);
        if (OB_SUCCESS == err)
        {
          if ('\0' == fetch_opt_[0])
          {
            strcpy(fetch_opt_, DEFAULT_LOG_FETCH_OPTION);
          }
        }
      }
      
      if (OB_SUCCESS == err)
      {
        ups_inner_port_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_UPS_INNER_PORT, 0);
        if (ups_inner_port_ <= 0)
        {
          TBSYS_LOG(ERROR, "update server inner port (%d) cannot <= 0.", ups_inner_port_);
          err = OB_ERROR;
        }
      }

      if (OB_SUCCESS == err)
      {
        low_priv_network_lower_limit_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_LOW_PRIV_NETWORK_LOWER_LIMIT,
            DEFAULT_LOW_PRIV_NETWORK_LOWER_LIMIT);
        low_priv_network_upper_limit_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_LOW_PRIV_NETWORK_UPPER_LIMIT,
            DEFAULT_LOW_PRIV_NETWORK_UPPER_LIMIT);
        high_priv_max_wait_time_us_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_HIGH_PRIV_MAX_WAIT_TIME_US,
            DEFAULT_HIGH_PRIV_MAX_WAIT_TIME_US);
        high_priv_adjust_time_us_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_HIGH_PRIV_ADJUST_TIME_US,
            DEFAULT_HIGH_PRIV_ADJUST_TIME_US);
        high_priv_wait_time_us_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_HIGH_PRIV_WAIT_TIME_US,
            DEFAULT_HIGH_PRIV_WAIT_TIME_US);
      }

      if (OB_SUCCESS == err)
      {
        int64_t mem_total = (blockcache_size_mb_ + blockindex_cache_size_mb_) * MB_UNIT + table_memory_limit_;
        if (mem_total >= total_memory_limit_)
        {
          TBSYS_LOG(WARN, "blockcache_size + blockindex_cache + table_memory_limit %ld cannot larger than total_memory_limit=%ld",
                    mem_total, total_memory_limit_);
          err = OB_INVALID_ARGUMENT;
        }
      }

      if (OB_SUCCESS == err)
      {
        int64_t table_total = (active_mem_limit_gb_ + frozen_mem_limit_gb_) * GB_UNIT;
        if (table_total >= table_memory_limit_)
        {
          TBSYS_LOG(WARN, "active_mem_limit + frozen_mem_limit %ld cannot larger than table_memory_limit=%ld",
                    table_total, table_memory_limit_);
          err = OB_INVALID_ARGUMENT;
        }
      }

      return err;
    }

    int ObUpdateServerParam::reload_from_config(const ObString& conf_file)
    {
      int err = OB_SUCCESS;

      char config_file_str[OB_MAX_FILE_NAME_LENGTH];
      int64_t length = conf_file.length();
      strncpy(config_file_str, conf_file.ptr(), length);
      config_file_str[length] = '\0';

      tbsys::CConfig config;
      int64_t tmp_active_mem_limit = active_mem_limit_gb_;
      int64_t tmp_frozen_mem_limit = frozen_mem_limit_gb_;

      if(config.load(config_file_str)) 
      {
        fprintf(stderr, "load file %s error\n", config_file_str);
        err = OB_ERROR;
      }

      if (OB_SUCCESS == err)
      {
        // reload vip
        const char* value = config.getString(OBUPS_UPS_SECTION, OBUPS_VIP);
        strncpy(ups_vip_, value, OB_MAX_IP_SIZE);
        ups_vip_[OB_MAX_IP_SIZE - 1] = '\0';

        TBSYS_LOG(INFO, "new_vip=%s", ups_vip_);
      }

      if (OB_SUCCESS == err)
      {
        tmp_active_mem_limit = config.getInt(OBUPS_UPS_SECTION, OBUPS_ACTIVE_MEM_LIMIT_GB,
            DEFAULT_ACTIVE_MEM_LIMIT_GB);
        tmp_frozen_mem_limit = config.getInt(OBUPS_UPS_SECTION, OBUPS_FROZEN_MEM_LIMIT_GB,
            DEFAULT_FROZEN_MEM_LIMIT_GB);
        int64_t table_total = (tmp_active_mem_limit + tmp_frozen_mem_limit) * GB_UNIT;
        if (table_total >= table_memory_limit_)
        {
          TBSYS_LOG(WARN, "active_mem_limit + frozen_mem_limit %ld cannot larger than table_memory_limit=%ld",
                    table_total, table_memory_limit_);
        }
        else
        {
          TBSYS_LOG(INFO, "active_mem_limit switch from %ld to %ld, frozen_mem_limit switch from %ld to %ld",
                    active_mem_limit_gb_, tmp_active_mem_limit,
                    frozen_mem_limit_gb_, tmp_frozen_mem_limit);
          active_mem_limit_gb_ = tmp_active_mem_limit;
          frozen_mem_limit_gb_ = tmp_frozen_mem_limit;
        }
      }

      if (OB_SUCCESS == err)
      {
        int64_t old_value = minor_num_limit_;
        minor_num_limit_ = config.getInt(OBUPS_UPS_SECTION, OBUPS_MINOR_NUM_LIMIT,
            DEFAULT_MINOR_NUM_LIMIT);
        TBSYS_LOG(INFO, "minor_num_limit switch from %ld to %ld", old_value, minor_num_limit_);
      }

      if (OB_SUCCESS == err)
      {
        int64_t old_value = sstable_time_limit_s_;
        sstable_time_limit_s_ = config.getInt(OBUPS_UPS_SECTION, OBUPS_SSTABLE_TIME_LIMIT_S,
            DEFAULT_SSTABLE_TIME_LIMIT_S);
        TBSYS_LOG(INFO, "sstable_time_limit switch from %ld to %ld", old_value, sstable_time_limit_s_);
      }

      if (OB_SUCCESS == err)
      {
        int64_t old_value = min_major_freeze_interval_s_;
        min_major_freeze_interval_s_ = config.getInt(OBUPS_UPS_SECTION, OBUPS_MIN_MAJOR_FREEZE_INTERVAL_S,
            DEFAULT_MIN_MAJOR_FREEZE_INTERVAL_S);
        TBSYS_LOG(INFO, "min_major_freeze_interval switch from %ld to %ld", old_value, min_major_freeze_interval_s_);
      }

      if (OB_SUCCESS == err)
      {
        err = load_string(sstable_compressor_name_, OB_MAX_FILE_NAME_LENGTH, OBUPS_UPS_SECTION, OBUPS_SSTABLE_COMPRESSOR_NAME, true);
        if (OB_SUCCESS == err)
        {
          ObCompressor *compressor = create_compressor(sstable_compressor_name_);
          if (NULL == compressor)
          {
            TBSYS_LOG(ERROR, "cannot load compressor library name=[%s]", sstable_compressor_name_);
            err = OB_ERROR;
          }
          else
          {
            TBSYS_LOG(INFO, "sstable_compressor_name switch to [%s]", sstable_compressor_name_);
            destroy_compressor(compressor);
          }
        }
      }

      if (OB_SUCCESS == err)
      {
        int64_t old_value = sstable_block_size_;
        sstable_block_size_ = config.getInt(OBUPS_UPS_SECTION, OBUPS_SSTABLE_BLOCK_SIZE,
            DEFAULT_SSTABLE_BLOCK_SIZE);
        TBSYS_LOG(INFO, "sstable_block_size switch from %ld to %ld", old_value, sstable_block_size_);
      }

      if (OB_SUCCESS == err)
      {
        int old_value_hour = major_freeze_duty_time_.tm_hour;
        int old_value_min = major_freeze_duty_time_.tm_min;
        const char *str = config.getString(OBUPS_UPS_SECTION, OBUPS_MAJOR_FREEZE_DUTY_TIME);
        if (NULL != str)
        {
          strptime(str, OBUPS_MAJOR_FREEZE_DUTY_TIME_FROMAT, &major_freeze_duty_time_);
        }
        TBSYS_LOG(INFO, "major_freeze_duty_time switch from [%d:%d] to [%d:%d]",
                  old_value_hour, old_value_min, major_freeze_duty_time_.tm_hour, major_freeze_duty_time_.tm_min);
      }

      return err;
    }

   int ObUpdateServerParam::load_standalone_conf()
    {
      int err = OB_SUCCESS;

      read_thread_count_ = TBSYS_CONFIG.getInt(OBUPS_STANDALONE_SECTION, OBUPS_READ_THREAD_COUNT,
          DEFAULT_READ_THREAD_COUNT);
      if (read_thread_count_ <= 0)
      {
        TBSYS_LOG(ERROR, "read thread count (%ld) cannot <= 0.", read_thread_count_);
        err = OB_ERROR;
      }

      if (OB_SUCCESS == err)
      {
        read_task_queue_size_ = TBSYS_CONFIG.getInt(OBUPS_STANDALONE_SECTION, OBUPS_READ_TASK_QUEUE_SIZE,
            DEFAULT_TASK_READ_QUEUE_SIZE);
      }

      if (OB_SUCCESS == err)
      {
        write_task_queue_size_ = TBSYS_CONFIG.getInt(OBUPS_STANDALONE_SECTION, OBUPS_WRITE_TASK_QUEUE_SIZE,
            DEFAULT_TASK_WRITE_QUEUE_SIZE);
      }

      if (OB_SUCCESS == err)
      {
        write_thread_batch_num_ = TBSYS_CONFIG.getInt(OBUPS_STANDALONE_SECTION,
            OBUPS_WRITE_GROUP_COMMIT_NUM, DEFAULT_WRITE_GROUP_COMMIT_NUM);
      }

      if (OB_SUCCESS == err)
      {
        err = load_string(standalone_schema_, sizeof(standalone_schema_),
            OBUPS_STANDALONE_SECTION, OBUPS_STANDALONE_SCHEMA, false);
      }

      if (OB_SUCCESS == err)
      {
        test_schema_version_ = TBSYS_CONFIG.getInt(OBUPS_STANDALONE_SECTION,
            OBUPS_STANDALONE_SCHEMA_VERSION, 0);
        if (test_schema_version_ <= 0)
        {
          TBSYS_LOG(WARN, "invalid schema version, test_schema_version=%ld",
              test_schema_version_);
          err = OB_ERROR;
        }
      }
      return err;
    }

    int ObUpdateServerParam::load_string(char* dest, const int32_t size,
        const char* section, const char* name, bool not_null)
    {
      int ret = OB_SUCCESS;
      if (NULL == dest || 0 >= size || NULL == section || NULL == name)
      {
        ret = OB_ERROR;
      }

      dest[0] = '\0';
      const char* value = NULL;
      if (OB_SUCCESS == ret)
      {
        value = TBSYS_CONFIG.getString(section, name);
        if (not_null && (NULL == value || 0 >= strlen(value)))
        {
          TBSYS_LOG(ERROR, "%s.%s has not been set.", section, name);
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret && NULL != value)
      {
        if ((int32_t)strlen(value) >= size)
        {
          TBSYS_LOG(ERROR, "%s.%s too long, length (%ld) > %d",
              section, name, strlen(value), size);
          ret = OB_SIZE_OVERFLOW;
        }
        else
        {
          strncpy(dest, value, strlen(value));
        }
      }

      return ret;
    }
  }
}



