/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_root_server2.cpp for ...
 *
 * Authors:
 *   daoan <daoan@taobao.com>
 *
 */

#include <new>
#include <string.h>

#include <tbsys.h>

#include "common/ob_schema.h"
#include "common/ob_range.h"
#include "common/ob_scanner.h"
#include "common/ob_define.h"
#include "common/ob_action_flag.h"
#include "common/ob_atomic.h"

#include "rootserver/ob_root_server2.h"
#include "rootserver/ob_root_worker.h"
namespace 
{
  const char* STR_SECTION_ROOT_SERVER = "root_server";
  const char* STR_SECTION_UPDATE_SERVER = "update_server";
  const char* STR_IP = "vip";
  const char* STR_PORT = "port";
  const char* STR_UPS_INNER_PORT = "ups_inner_port";

  const char* STR_SECTION_SCHEMA_INFO = "schema";
  const char* STR_FILE_NAME = "file_name";
  const char* STR_SECTION_CHUNK_SERVER = "chunk_server";
  const char* STR_LEASE = "lease";
  const char* STR_MIGRATE_WAIT_SECONDS = "migrate_wait_seconds";
  const char* STR_SAFE_LOST_ONE_DURATION = "safe_lost_one_duration";
  const char* STR_SAFE_WAIT_INIT_DURATION= "wait_init_duration";
  const char* STR_MAX_MERGE_DURATION= "max_merge_duration";
  const char* STR_CS_COMMAND_INTERVAL= "cs_command_interval_us";
  const char* STR_DISK_HIGH_LEVEL = "disk_high_level";
  const char* STR_DISK_TRIGGER_LEVEL = "disk_trigger_level";
  const char* STR_SHARED_ADJACENT = "shared_adjacent";
  const char* STR_SAFE_COPY_COUNT_IN_INIT = "__safe_copy_count_in_init";
  const char* STR_SAFE_COPY_COUNT_IN_MERGE = "__safe_copy_count_in_merge";
  const char* STR_CREATE_TABLE_IN_INIT = "__create_table_in_init";
const char* STR_TABLET_REPLICAS_NUM = "tablet_replicas_num";

  const int WAIT_SECONDS = 1;
  const int PACKAGE_TIME_OUT = 1;
  const int SLAVE_SLEEP_TIME = 1 * 1000; // 1 ms
  //const int64_t REMAIN_TIME = 1000000 * 500;  //500s
  const int RETURN_BACH_COUNT = 8;
  const int MAX_RETURN_BACH_ROW_COUNT = 1000;

  const char* ROOT_1_PORT =   "1_port";
  const char* ROOT_1_MS_PORT =   "1_ms_port";
  const char* ROOT_1_IPV6_1 = "1_ipv6_1";
  const char* ROOT_1_IPV6_2 = "1_ipv6_2";
  const char* ROOT_1_IPV6_3 = "1_ipv6_3";
  const char* ROOT_1_IPV6_4 = "1_ipv6_4";
  const char* ROOT_1_IPV4   = "1_ipv4";
  const char* ROOT_1_TABLET_VERSION= "1_tablet_version";

  const char* ROOT_2_PORT =   "2_port";
  const char* ROOT_2_MS_PORT =   "2_ms_port";
  const char* ROOT_2_IPV6_1 = "2_ipv6_1";
  const char* ROOT_2_IPV6_2 = "2_ipv6_2";
  const char* ROOT_2_IPV6_3 = "2_ipv6_3";
  const char* ROOT_2_IPV6_4 = "2_ipv6_4";
  const char* ROOT_2_IPV4   = "2_ipv4";
  const char* ROOT_2_TABLET_VERSION= "2_tablet_version";

  const char* ROOT_3_PORT =   "3_port";
  const char* ROOT_3_MS_PORT =   "3_ms_port";
  const char* ROOT_3_IPV6_1 = "3_ipv6_1";
  const char* ROOT_3_IPV6_2 = "3_ipv6_2";
  const char* ROOT_3_IPV6_3 = "3_ipv6_3";
  const char* ROOT_3_IPV6_4 = "3_ipv6_4";
  const char* ROOT_3_IPV4   = "3_ipv4";
  const char* ROOT_3_TABLET_VERSION= "3_tablet_version";

  const char* ROOT_OCCUPY_SIZE ="occupy_size";
  const char* ROOT_RECORD_COUNT ="record_count";
  const char* ROOT_CRC_SUM ="crc_sum";

  char max_row_key[oceanbase::common::OB_MAX_ROW_KEY_LENGTH];  


  const int ADD_ERROR = -1;
  const int ADD_OK = 0;
  const int SHOULD_DO_MIGRATE = 1;
  const int CAN_NOT_FIND_SUTABLE_SERVER = 2;

  const int MIGRATE_WAIT_SECONDS = 60;

  const int NO_REPORTING = 0;
  const int START_REPORTING = 1;

  const int HB_RETRY_FACTOR = 100 * 1000;
  const int MAX_TRIGEER_WAIT_SECONDS = 60 * 30;  
}
namespace oceanbase 
{ 
  namespace rootserver 
  {
    const int ObRootServer2::STATUS_INIT;
    const int ObRootServer2::STATUS_NEED_REPORT;
    const int ObRootServer2::STATUS_NEED_BUILD;
    const int ObRootServer2::STATUS_CHANGING;
    const int ObRootServer2::STATUS_NEED_BALANCE;
    const int ObRootServer2::STATUS_BALANCING;
    const int ObRootServer2::STATUS_SLEEP;
    const int ObRootServer2::STATUS_INTERRUPT_BALANCING;
    const char* ObRootServer2::ROOT_TABLE_EXT = "rtable";
    const char* ObRootServer2::CHUNKSERVER_LIST_EXT = "clist";

    using namespace common;
    ObRootServer2::ObRootServer2()
      :ups_inner_port_(0),lease_duration_(0), schema_manager_(NULL), 
      root_table_for_query_(NULL), tablet_manager_for_query_(NULL),
      root_table_for_build_(NULL), tablet_manager_for_build_(NULL), 
      have_inited_(false), new_table_created_(false), migrate_wait_seconds_(0),
      server_status_(STATUS_INIT), build_sync_flag_(BUILD_SYNC_FLAG_NONE), 
      safe_lost_one_duration_(0), wait_init_time_(1000L * 1000L * 60),
      max_merge_duration_(1000L * 1000L * 7200),cs_merge_command_interval_mseconds_(60 * 1000L * 1000L),
      first_cs_had_registed_(false), receive_stop_(false), drop_this_build_(false), 
      safe_copy_count_in_init_(2), safe_copy_count_in_merge_(2),create_table_in_init_(0),
      last_frozen_mem_version_(-1), pre_frozen_mem_version_(-1),last_frozen_time_(0),
       tablet_replicas_num_(DEFAULT_TABLET_REPLICAS_NUM),
       root_table_modifier_(this), balance_worker_(this), heart_beat_checker_(this)
    {
      worker_ = NULL;
      log_worker_ = NULL;
      time_stamp_changing_ = -1;
      frozen_mem_version_ = -1;
      config_file_name_[0] = '\0';
      schema_file_name_[0] = '\0';
    }
    ObRootServer2::~ObRootServer2()
    {
      receive_stop_ = true;
      heart_beat_checker_.stop();
      heart_beat_checker_.wait();
      balance_worker_.stop();
      balance_worker_.wait();
      root_table_modifier_.stop();
      root_table_modifier_.wait();
      if (schema_manager_)
      {
        delete schema_manager_;
        schema_manager_ = NULL;
      }
      if (root_table_for_query_)
      {
        delete root_table_for_query_;
        root_table_for_query_= NULL;
      }
      if (root_table_for_build_)
      {
        delete root_table_for_build_;
        root_table_for_build_ = NULL;
      }
      if (tablet_manager_for_query_)
      {
        delete tablet_manager_for_query_;
        tablet_manager_for_query_ = NULL;
      }
      if (tablet_manager_for_build_)
      {
        delete tablet_manager_for_build_;
        tablet_manager_for_build_ = NULL;
      }
      have_inited_ = false;
    }
    bool ObRootServer2::init(const char* config_file_name, const int64_t now, OBRootWorker* worker)
    {
      bool res = true;
      worker_ = worker;
      log_worker_ = worker_->get_log_manager()->get_log_worker();

      if (have_inited_ == false && config_file_name != NULL)
      {
        schema_manager_ = new(std::nothrow)ObSchemaManagerV2(now);
        TBSYS_LOG(INFO, "init schema_version=%ld", now);
        if (schema_manager_ == NULL)
        {
          TBSYS_LOG(ERROR, "new ObSchemaManagerV2() error");
          res = false;
        }
        if (res)
        {
          tablet_manager_for_query_ = new(std::nothrow)ObTabletInfoManager();
          if (tablet_manager_for_query_ == NULL)
          {
            TBSYS_LOG(ERROR, "new ObTabletInfoManager error");
            res = false;
          }
          else
          {
            root_table_for_query_ = new(std::nothrow)ObRootTable2(tablet_manager_for_query_);
            TBSYS_LOG(INFO, "new root table created, root_table_for_query=%p", root_table_for_query_);
            if (root_table_for_query_ == NULL) 
            {
              TBSYS_LOG(ERROR, "new ObRootTable2 error");
              res = false;
            }
          }
        }
        strncpy(config_file_name_, config_file_name, OB_MAX_FILE_NAME_LENGTH);
        config_file_name_[OB_MAX_FILE_NAME_LENGTH - 1] = '\0';
        if (res)
        {
          res = reload_config(config_file_name_);
        }
        //this means if init failed, you should aband this ObRootServer2
        //do not call init again. 
        have_inited_ = true; 
      }
      else
      {
        if (have_inited_)
        {
          TBSYS_LOG(ERROR, "can not be inited again");
        } 
        else
        {
          TBSYS_LOG(ERROR, "config file name can not be empty");
        }
        res = false;
      }

      return res;
    }

    void ObRootServer2::start_threads()
    {
      root_table_modifier_.start();
      balance_worker_.start();
      heart_beat_checker_.start();
    }

    bool ObRootServer2::reload_config(const char* config_file_name) 
    {
      int res = false;
      if (config_file_name != NULL)
      {
        res = true;
        tbsys::CConfig config;
        if (res) 
        {
          if (EXIT_FAILURE == config.load(config_file_name))
          {
            TBSYS_LOG(ERROR, "load config file %s error", config_file_name);
            res = false;
          }
        }
        if (!have_inited_)
        {
          //update server info; schema; and vip only can be changed in the first time we load config file
          if (res)
          {
            //get info of update server
            const char* ip =  config.getString(STR_SECTION_UPDATE_SERVER, STR_IP, NULL);
            int port = config.getInt(STR_SECTION_UPDATE_SERVER, STR_PORT, 0);
            if (ip == NULL ||  0 == port || !update_server_status_.server_.set_ipv4_addr(ip, port))
            {
              TBSYS_LOG(ERROR, "load update server info error config file is %s ", config_file_name);
              res = false;
            }
            update_server_status_.status_ = ObServerStatus::STATUS_SERVING;

            if (res)
            {
              ups_inner_port_ = config.getInt(STR_SECTION_UPDATE_SERVER, STR_UPS_INNER_PORT, 0);
              if (0 == ups_inner_port_)
              {
                TBSYS_LOG(ERROR, "load update server inner port error");
                res = false;
              }
            }
          }
          UNUSED(STR_SECTION_ROOT_SERVER);
          //if (res)
          //{
          //  //get root server's vip
          //  const char* vip =  config.getString(STR_SECTION_ROOT_SERVER, STR_IP, NULL);

          //}
          if (res) 
          {
            //give schema file name
            const char* schema_file_name = config.getString(STR_SECTION_SCHEMA_INFO, STR_FILE_NAME, NULL);
            if (NULL == schema_file_name)
            {
              TBSYS_LOG(ERROR, "load schema file name error config file is %s ", config_file_name);
              res = false;
            }
            else 
            {
              strncpy(schema_file_name_, schema_file_name, OB_MAX_FILE_NAME_LENGTH);
              schema_file_name_[OB_MAX_FILE_NAME_LENGTH - 1] = '\0';
            }
          }
          if (res)
          {
            // init schema manager
            tbsys::CConfig config;
            if (!schema_manager_->parse_from_file(schema_file_name_, config))
            {
              TBSYS_LOG(ERROR, "parse schema error chema file is %s ", schema_file_name_);
              res = false;
            }
            else
            {
              TBSYS_LOG(INFO, "load schema from file, file=%s", schema_file_name_);
            }
          }
        }

        if (res)
        {
          const char* p = config.getString(STR_SECTION_CHUNK_SERVER, STR_LEASE, "10000000");
          int64_t lease_duration = strtoll(p, NULL, 10);
          if (lease_duration < 1000000)
          {
            TBSYS_LOG(ERROR, "lease duration %ld is unacceptable", lease_duration);
            res = false;
          }
          else
          {
            lease_duration_ = lease_duration;
          }
        }

        if (res)
        {
          migrate_wait_seconds_ = config.getInt(STR_SECTION_ROOT_SERVER, STR_MIGRATE_WAIT_SECONDS, MIGRATE_WAIT_SECONDS);
          if (migrate_wait_seconds_ < MIGRATE_WAIT_SECONDS )
          {
            TBSYS_LOG(WARN, "change migrate_wait_seconds to %d second(s)", MIGRATE_WAIT_SECONDS);
            migrate_wait_seconds_ = MIGRATE_WAIT_SECONDS;
          }

          int64_t safe_lost_one_duration = config.getInt(STR_SECTION_CHUNK_SERVER, STR_SAFE_LOST_ONE_DURATION, 3600);
          TBSYS_LOG(INFO, "safe lost one duration is %ld second(s)", safe_lost_one_duration);
          safe_lost_one_duration_ = safe_lost_one_duration * 1000L * 1000L;

          int64_t wait_init_time = config.getInt(STR_SECTION_CHUNK_SERVER, STR_SAFE_WAIT_INIT_DURATION, 60);
          TBSYS_LOG(INFO, "wait_init_time is %ld second(s)", wait_init_time);
          wait_init_time_ = wait_init_time * 1000L * 1000L;

          int64_t max_merge_duration = config.getInt(STR_SECTION_CHUNK_SERVER, STR_MAX_MERGE_DURATION, 7200);
          TBSYS_LOG(INFO, "max_merge_duration is %ld second(s)", max_merge_duration);
          max_merge_duration_ = max_merge_duration * 1000L * 1000L;

          int64_t cs_merge_command_interval_mseconds = config.getInt(STR_SECTION_ROOT_SERVER, STR_CS_COMMAND_INTERVAL, 60 * 1000L * 1000L);
          TBSYS_LOG(INFO, "cs_merge_command_interval_mseconds is %ld m_second(s)", cs_merge_command_interval_mseconds);
          cs_merge_command_interval_mseconds_ = cs_merge_command_interval_mseconds;


          int level = config.getInt(STR_SECTION_CHUNK_SERVER, STR_DISK_HIGH_LEVEL, 80);
          if (level <= 0 || level > 99)
          {
            TBSYS_LOG(ERROR, "unacceptable value of disk_high_level %d change to %d", level, 80);
            level = 80;
          }
          balance_prameter_.disk_high_level_ = level;

          level = config.getInt(STR_SECTION_CHUNK_SERVER, STR_DISK_TRIGGER_LEVEL, 70);
          if (level <= 0 || level > balance_prameter_.disk_high_level_ )
          {
            TBSYS_LOG(ERROR, "unacceptable value of  disk_trigger_level_ %d change to %d", 
                level, balance_prameter_.disk_high_level_);
            level = balance_prameter_.disk_high_level_;
          }
          balance_prameter_.disk_trigger_level_ = level;

          int shared_adjacent = config.getInt(STR_SECTION_CHUNK_SERVER, STR_SHARED_ADJACENT, 10);

          int safe_copy_count_in_init = config.getInt(STR_SECTION_ROOT_SERVER, STR_SAFE_COPY_COUNT_IN_INIT, 2);
          if (safe_copy_count_in_init > 0 && safe_copy_count_in_init <= OB_SAFE_COPY_COUNT)
          {
            safe_copy_count_in_init_ = safe_copy_count_in_init;
          }
          TBSYS_LOG(INFO, "safe_copy_count_in_init_=%d", safe_copy_count_in_init_);
          int safe_copy_count_in_merge = config.getInt(STR_SECTION_ROOT_SERVER, STR_SAFE_COPY_COUNT_IN_MERGE, 2);
          if (safe_copy_count_in_merge > 0 && safe_copy_count_in_merge <= OB_SAFE_COPY_COUNT)
          {
            safe_copy_count_in_merge_ = safe_copy_count_in_merge;
          }
          int tmp = config.getInt(STR_SECTION_ROOT_SERVER, STR_TABLET_REPLICAS_NUM, DEFAULT_TABLET_REPLICAS_NUM);
          if (safe_copy_count_in_init_ > tmp)
          {
            TBSYS_LOG(ERROR, "tablet_replicas_num=%d safe_copy_count_in_init=%d", tmp, safe_copy_count_in_init_);
            res = false;
          }
          else if (0 < tmp && OB_SAFE_COPY_COUNT >= tmp)
          {
            tablet_replicas_num_ = tmp;
            TBSYS_LOG(INFO, "tablet_replicas_num=%d", tablet_replicas_num_);
          }
          else
          {
            TBSYS_LOG(WARN, "invalid tablet_replicas_num, num=%d", tmp);
          }
          create_table_in_init_ = config.getInt(STR_SECTION_ROOT_SERVER, STR_CREATE_TABLE_IN_INIT,0);

          if (shared_adjacent <= 0)
          {
            TBSYS_LOG(ERROR, "unacceptable value of  shared_adjacent_ %d change to %d", 
                shared_adjacent, 10);
            shared_adjacent = 10;
          }
          balance_prameter_.shared_adjacent_ = shared_adjacent;
        }
      }
      return res;
    }
    bool ObRootServer2::start_report(bool init_flag)
    {
      TBSYS_LOG(DEBUG, "start_report init flag is %d", init_flag);
      bool ret = false;

      //      int rc = OB_SUCCESS;
      //
      //      if (rc == OB_SUCCESS)
      //      {
      //        tbsys::CThreadGuard guard(&status_mutex_);
      //        switch (server_status_)
      //        {
      //          //case STATUS_INIT:
      //          //  if (init_flag == true)
      //          //  {
      //          //    server_status_ = STATUS_NEED_REPORT;
      //          //  }
      //          //  ret = true;
      //          //  break;
      //          case STATUS_SLEEP:
      //            server_status_ = STATUS_NEED_REPORT;
      //            ret = true;
      //            break;
      //          case STATUS_BALANCING:
      //            server_status_ = STATUS_INTERRUPT_BALANCING;
      //            break;
      //
      //          default:
      //            break;
      //        }
      //        if (is_master())
      //        {
      //          if (ret && 0 == time_stamp_changing_) 
      //          {
      //            int64_t ts = tbsys::CTimeUtil::getTime();
      //            time_stamp_changing_ = ts; // set chaning timestamp
      //          }
      //          rc = log_worker_.start_report(time_stamp_changing_, init_flag);
      //        }
      //      }
      //
      TBSYS_LOG(DEBUG, "start_report return is %d", ret);
      return ret;
    }
    void ObRootServer2::drop_current_build()
    {
      drop_this_build_ = true;
      if (is_master())
      {
        log_worker_->drop_current_build();
      }
    }
    bool ObRootServer2::start_switch()
    {
      bool ret = false;

      int rc = OB_SUCCESS;

      tbsys::CThreadGuard guard(&status_mutex_);
      int64_t ts = tbsys::CTimeUtil::getTime();


      if (rc == OB_SUCCESS)
      {
        TBSYS_LOG(INFO, " server_status_ = %d", server_status_);
        switch (server_status_)
        {
          case STATUS_SLEEP:
          case STATUS_NEED_REPORT:
          case STATUS_NEED_BUILD:
            if (is_master())
            {
              time_stamp_changing_ = ts; // set chaning timestamp
              rc = log_worker_->start_switch_root_table(time_stamp_changing_);
            }
            if (rc == OB_SUCCESS) 
            {
              server_status_ = STATUS_NEED_BUILD;
              ret = true;
            }
            break;
          case STATUS_BALANCING:
            if (is_master())
            {
              time_stamp_changing_ = ts; // set chaning timestamp
              rc = log_worker_->start_switch_root_table(time_stamp_changing_);
            }
            if (rc == OB_SUCCESS) 
            {
              server_status_ = STATUS_INTERRUPT_BALANCING;
            }
            break;
          default:
            break;
        }
      }

      return ret;
    }

    bool ObRootServer2::get_schema(common::ObSchemaManagerV2& out_schema) const
    {
      bool res = false;
      tbsys::CRLockGuard guard(schema_manager_rwlock_);
      if (schema_manager_ != NULL)
      {
        out_schema = *schema_manager_;
        res = true;
      }
      return res;
    }
    int64_t ObRootServer2::get_schema_version() const
    {
      int64_t t1 = 0;
      tbsys::CRLockGuard guard(schema_manager_rwlock_);
      if (schema_manager_ != NULL)
      {
        t1 = schema_manager_->get_version();
      }

      return t1;
    }

    void ObRootServer2::print_alive_server() const
    {
      TBSYS_LOG(INFO, "start dump server info");
      //tbsys::CRLockGuard guard(server_manager_rwlock_); // do not need this lock
      ObChunkServerManager::const_iterator it = server_manager_.begin();
      int32_t index = 0;
      for (; it != server_manager_.end(); ++it)
      {
        it->dump(index++);
      }
      TBSYS_LOG(INFO, "dump server info complete");
      return;
    }
    int ObRootServer2::get_cs_info(ObChunkServerManager* out_server_manager) const
    {
      int ret = OB_SUCCESS;
      tbsys::CRLockGuard guard(server_manager_rwlock_);
      if (out_server_manager != NULL)
      {
        *out_server_manager = server_manager_;
      }
      else
      {
        ret = OB_ERROR;
      }
      return ret;
    }
    void ObRootServer2::dump_root_table() const
    {
      tbsys::CRLockGuard guard(root_table_rwlock_);
      if (root_table_for_query_ != NULL)
      {
        root_table_for_query_->dump();
      }
    }
    void ObRootServer2::reload_config()
    {
      reload_config(config_file_name_);
    }

    int ObRootServer2::update_server_freeze_mem(int64_t& frozen_mem_version)
    {
      int res = OB_SUCCESS;
      if (update_server_status_.status_ != ObServerStatus::STATUS_SERVING)
      {
        TBSYS_LOG(WARN, "update server status error now is %ld force it to STATUS_SERVING", update_server_status_.status_);
        update_server_status_.status_ = ObServerStatus::STATUS_SERVING;
      }
      build_sync_flag_ = BUILD_SYNC_FLAG_FREEZE_MEM;
      while (update_server_status_.status_ == ObServerStatus::STATUS_SERVING && !drop_this_build_)
      {
        if (!is_master())
        {
          usleep(SLAVE_SLEEP_TIME); // wait for update server response
          continue;
        }

        TBSYS_LOG(INFO, "wait for update server change to STATUS_REPORTING ");
        if (OB_SUCCESS == worker_->up_freeze_mem(update_server_status_.server_, frozen_mem_version))
        {
          echo_update_server_freeze_mem();
        }
        else 
        {
          sleep(PACKAGE_TIME_OUT);
        }
      }
      if (is_master()) sleep(WAIT_SECONDS);
      while (update_server_status_.status_ == ObServerStatus::STATUS_REPORTING && !drop_this_build_)
      {
        // when the update server has done his job, he will send message, and 
        // status will be changed to STATUS_REPORTED
        TBSYS_LOG(DEBUG,"wait for update server finish his job");
        if (is_master()) worker_->up_freeze_mem(update_server_status_.server_, frozen_mem_version);
        sleep(WAIT_SECONDS);
      }
      if (update_server_status_.status_ != ObServerStatus::STATUS_REPORTED)
      {
        TBSYS_LOG(WARN,"update_server_status %ld", update_server_status_.status_);
        res = OB_ERROR;
      }
      return res;
    }
    int ObRootServer2::cs_start_merge(const int64_t frozen_mem_version, const int32_t build_flag)
    {
      int res = OB_SUCCESS;
      bool have_server_not_prepaired = false;
      do
      {
        if (drop_this_build_) break;
        have_server_not_prepaired = false;
        TBSYS_LOG(DEBUG,"check if all alive server have received start merge command");
        {
          //tbsys::CRLockGuard guard(server_manager_rwlock_); //do not use this lock
          ObChunkServerManager::const_iterator it = server_manager_.begin();
          bool send_request = false;
          for (; it != server_manager_.end(); ++it)
          {
            send_request = false;
            if(it->status_ == ObServerStatus::STATUS_SERVING || it->status_ == ObServerStatus::STATUS_WAITING_REPORT)
            {
              send_request = true;
            }
            if(send_request)
            {
              if (is_master())
              {
                // send packet only if I am master
                ObServer server(it->server_);
                server.set_port(it->port_cs_);
                if (OB_SUCCESS == worker_->cs_start_merge(server, frozen_mem_version, build_flag))
                {
                  echo_start_merge_received(server);
                  usleep(cs_merge_command_interval_mseconds_);
                }
              }
              have_server_not_prepaired = true;
            }
          }
        }// release lock
        if (have_server_not_prepaired) sleep(PACKAGE_TIME_OUT);
      }while(have_server_not_prepaired);

      return res;
    }
    void ObRootServer2::execute_batch_migrate()
    {
      const ObBatchMigrateInfo* it = batch_migrate_helper_.begin();
      int64_t monotonic_finish_time = tbsys::CTimeUtil::getMonotonicTime() + migrate_wait_seconds_ * 1000000L;
      ObServer src_server;
      ObServer dest_server;
      ObServerStatus* src_status = NULL;
      ObServerStatus* dest_status = NULL;
      if (it < batch_migrate_helper_.end()) 
      {
        TBSYS_LOG(INFO, "execute_batch_migrate start");
        for (; it != batch_migrate_helper_.end(); ++it)
        {
          src_status = server_manager_.get_server_status(it->get_src_server_index());
          dest_status = server_manager_.get_server_status(it->get_dest_server_index());
          if (src_status == NULL || dest_status == NULL)
          {
            TBSYS_LOG(ERROR, "you should not get this bugss!");
          }
          else
          {
            src_server = server_manager_.get_cs(it->get_src_server_index());
            dest_server = server_manager_.get_cs(it->get_dest_server_index());

            if (OB_SUCCESS == worker_->cs_migrate(*(it->get_range()), src_server, dest_server, it->get_keep_src()))
            {
              //set migrate_out_finish_time_ and migrate_in_finish_time_ again
              src_status->migrate_out_finish_time_ = monotonic_finish_time;
              dest_status->migrate_in_finish_time_ = monotonic_finish_time;

              static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
              it->get_range()->to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
              char f_server[OB_IP_STR_BUFF];
              char t_server[OB_IP_STR_BUFF];
              src_server.to_string(f_server, OB_IP_STR_BUFF);
              dest_server.to_string(t_server, OB_IP_STR_BUFF);
              TBSYS_LOG(INFO, "migrate/copy tablet, tablet=%s src=%s dest=%s keep_src=%d", 
                        row_key_dump_buff, f_server, t_server, it->get_keep_src());
            }

          }
        } // end for

        //if (batch_migrate_helper_.begin() != batch_migrate_helper_.end())
        //{
        //  //sleep only when batch_migrate_helper is not empty
        //  sleep(migrate_wait_seconds_);
        //}
        //TBSYS_LOG(DEBUG, "execute_batch_migrate end");
      }
      batch_migrate_helper_.reset();
    }
    /*
     * 当某tablet的备份数不足时候, 补足备份
     */
    //return value 
    //const int ADD_OK = 0;
    //const int SHOULD_DO_MIGRATE = 1;
    //const int CAN_NOT_FIND_SUTABLE_SERVER = 2;
    int ObRootServer2::add_copy(const ObRootTable2* root_table, ObRootTable2::iterator& it, 
                                const int64_t monotonic_now,const int64_t last_version)
    {
      int return_code = ADD_ERROR;
      if (root_table != NULL)
      {
        return_code = ADD_OK;
        const common::ObTabletInfo* tablet_info = NULL;
        tablet_info = root_table->get_tablet_info(it);
        if (tablet_info == NULL)
        {
          TBSYS_LOG(ERROR, "you should not reach this bugs");
          return_code = ADD_ERROR;
        }
        else 
        {
          ObCandidateServerBySharedManager2::effectiveServer effective_server;
          int src_server_index = OB_INVALID_INDEX;
          int dest_server_index = OB_INVALID_INDEX;
          const ObRange* range = &(tablet_info->range_);
          int64_t occupy_size = tablet_info->occupy_size_;
          int added = 0;
          ObServerStatus* server_status = NULL;
          bool chosen_server_busy = false; //is the server we chosen in migrating? 
          for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; ++i)
          {
            if ((it->server_info_indexes_[i] != OB_INVALID_INDEX) && 
                    (it->tablet_version_[i] >= last_version))
            {
              effective_server.server_indexes_[added] = it->server_info_indexes_[i];
              server_status = server_manager_.get_server_status(it->server_info_indexes_[i]);
              if (server_status != NULL)
              {
                if (server_status->migrate_out_finish_time_ < monotonic_now)
                {
                  src_server_index = it->server_info_indexes_[i];
                }
                else 
                {
                  chosen_server_busy = true;
                }
              }
              added++;
              if (added >= OB_SAFE_COPY_COUNT)
              {
                break;
              }
            }
          }
          if (src_server_index != OB_INVALID_INDEX)
          {
            {
              tbsys::CRLockGuard guard(server_manager_rwlock_);
              candidate_shared_helper_.init(effective_server, &server_manager_);
            }
            candidate_shared_helper_.scan_root_table(root_table_for_query_);
            candidate_shared_helper_.sort();
            // now candidate_shared_helper_ will contain all candidate server sorted by shared count
          }

          int32_t lowest_shared_count = -1;
          int32_t lowest_disk_used_percent = -1;
          server_status = NULL;
          //chosen_server_busy = false;
          ObCandidateServerBySharedManager2::sharedInfo* candidate_it = candidate_shared_helper_.begin();
          bool consider_it = false;

          for (; candidate_it != candidate_shared_helper_.end() && src_server_index != OB_INVALID_INDEX; 
              ++candidate_it)
          {
            if (server_manager_.can_migrate_in(candidate_it->server_index_, occupy_size, &balance_prameter_))
            {
              consider_it = false;
              if (lowest_shared_count == -1)
              {
                lowest_shared_count = candidate_it->shared_count_;
                consider_it = true;
              }
              else if (lowest_shared_count > 0)
              {
                if (((candidate_it->shared_count_ - lowest_shared_count) * 100/lowest_shared_count )
                    < balance_prameter_.shared_adjacent_)
                {
                  consider_it = true;
                }
              }
              if (consider_it)
              {
                server_status = server_manager_.get_server_status(candidate_it->server_index_);
                if (server_status != NULL && 
                    server_status->disk_info_.get_percent() > 0)
                {
                  if (lowest_disk_used_percent == -1 || lowest_disk_used_percent > server_status->disk_info_.get_percent())
                  {
                    if (server_status->migrate_in_finish_time_ > monotonic_now)
                    {
                      chosen_server_busy = true;
                    }
                    else 
                    {
                      lowest_disk_used_percent = server_status->disk_info_.get_percent();
                      dest_server_index = candidate_it->server_index_;
                    }
                  }
                }
              }
            }
          }
          if (range == NULL || src_server_index == OB_INVALID_INDEX || dest_server_index == OB_INVALID_INDEX)
          {
            if (!chosen_server_busy)
            {
              //chosen_server_busy is false means we can not find suitable server
              //chosen_server_busy is true means for now, we can not find suitable server
              TBSYS_LOG(ERROR, "root server can not find suitable server for lost tablet");
              if (range != NULL) 
              {
                static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
                range->to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
                TBSYS_LOG(INFO, "%s", row_key_dump_buff);
                //range->hex_dump(TBSYS_LOG_LEVEL_INFO);
              }
            }
            return_code = CAN_NOT_FIND_SUTABLE_SERVER;
          }
          else
          {
            ObBatchMigrateInfo bmi(range, src_server_index, dest_server_index, true);
            tbsys::CRLockGuard guard(server_manager_rwlock_);
            int ret = batch_migrate_helper_.add(bmi, monotonic_now, server_manager_);
            if (ret == ObBatchMigrateInfoManager::ADD_REACH_MAX) 
            {
              return_code = SHOULD_DO_MIGRATE;
            }
            else if (ObBatchMigrateInfoManager::ADD_OK == ret)
            {
              it->migrate_monotonic_time_ = monotonic_now + migrate_wait_seconds_;
            }
          }
        }
      }
      return return_code;
    }

// @return 0 do not copy, 1 copy immediately, -1 delayed copy
inline int ObRootServer2::need_copy(int32_t available_num, int32_t lost_num)
{
  OB_ASSERT(0 <= available_num);
  OB_ASSERT(0 <= lost_num);
  int ret = 0;
  if (0 == available_num || 0 == lost_num)
  {
    ret = 0;
  }
  else if (1 == available_num)
  {
    ret = 1;
  }
  else if (1 < available_num && available_num < lost_num)
  {
    ret = 1;
  }
  else
  {
    ret = -1;
  }
  return ret;
}

    void ObRootServer2::add_lost_tablet()
    {
      TBSYS_LOG(DEBUG, "start add_lost_tablet");
      batch_migrate_helper_.reset();
      int64_t now = tbsys::CTimeUtil::getTime();
      if (root_table_for_query_ != NULL) 
      {
        bool still_have = false;
        ObRange last_range;
        int lost_copy = 0;
        int add_ret = 0;
        do 
        {
          still_have = false;
          {
            int64_t monotonic_now = tbsys::CTimeUtil::getMonotonicTime();
            int64_t last_version = 0;
            ObRootTable2::iterator it;
            tbsys::CRLockGuard guard(root_table_rwlock_);
            for (it = root_table_for_query_->begin(); it < root_table_for_query_->sorted_end(); ++it)
            {
              int32_t valid_replicas_num = 0;
              for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; i++)
              {
                if (OB_INVALID_INDEX != it->server_info_indexes_[i])
                {
                  valid_replicas_num ++;
                  if (it->tablet_version_[i] > last_version)
                  {
                    last_version = it->tablet_version_[i];
                  }
                }
              }
              lost_copy = 0;
              if (valid_replicas_num < tablet_replicas_num_)
              {
                lost_copy = tablet_replicas_num_ - valid_replicas_num;
              }
              else if (valid_replicas_num > tablet_replicas_num_)
              {
                TBSYS_LOG(DEBUG, "too many replicas, available=%d defined=%d", valid_replicas_num, tablet_replicas_num_);
              }
              int did_need_copy = need_copy(valid_replicas_num, lost_copy);
              if ((safe_copy_count_in_init_ > 1)
                  && (1 == did_need_copy
                      || (-1 == did_need_copy && now - it->last_dead_server_time_ > safe_lost_one_duration_)))
              {
                if ((it->migrate_monotonic_time_ < monotonic_now))
                {
                  add_ret = add_copy(root_table_for_query_, it, monotonic_now,last_version);
                  if(add_ret == CAN_NOT_FIND_SUTABLE_SERVER)
                  {
                    //NULL;
                  }
                  else 
                  {
                    still_have = true;
                    if(add_ret == SHOULD_DO_MIGRATE)
                    {
                      break;
                    }
                  }
                }
              } // end if
            } // end for
          }//release lock;
          {
            tbsys::CThreadGuard guard(&status_mutex_);
            if (server_status_ == STATUS_INTERRUPT_BALANCING)
            {
              break;
            }
          }
          if (still_have) execute_batch_migrate();
        } while (false);
      }
    }
    /*
     * 需要从某server移出tablet以减轻其压力
     */
    //return value 
    //const int ADD_OK = 0;
    //const int SHOULD_DO_MIGRATE = 1;
    //const int CAN_NOT_FIND_SUTABLE_SERVER = 2;
    int ObRootServer2::move_out_tablet(ObServerStatus* migrate_out_server, const int64_t monotonic_now)
    {
      int return_code = ADD_OK;
      candidate_disk_helper_.reset();
      ObRootTable2::const_iterator root_table_index_it = root_table_for_query_->sorted_end();
      int32_t src_server_index = migrate_out_server - server_manager_.begin();
      int32_t dest_server_index = OB_INVALID_INDEX;
      {
        tbsys::CRLockGuard guard(server_manager_rwlock_);
        ObChunkServerManager::iterator it = server_manager_.begin();
        for (; it != server_manager_.end(); ++it) 
        {
          if (it != migrate_out_server)
          {
            candidate_disk_helper_.add_server(it);
          }
        }
      }
      if (candidate_disk_helper_.get_length() <= 0)
      {
        TBSYS_LOG(DEBUG, "disk is not enougth");
        //return_code = CAN_NOT_FIND_SUTABLE_SERVER;
      }
      ObServerStatus* migrate_in_server = NULL;
      for (int candidate_index = 0; 
          candidate_index < candidate_disk_helper_.get_length() && migrate_in_server == NULL;
          candidate_index++)
      {
        dest_server_index = OB_INVALID_INDEX;
        if (ADD_OK == return_code)
        {
          migrate_in_server = candidate_disk_helper_.get_server(candidate_index);

          if (migrate_in_server->disk_info_.get_percent() >= balance_prameter_.disk_trigger_level_ ||
              migrate_in_server->migrate_in_finish_time_ > monotonic_now)
          {
            char server_str[OB_IP_STR_BUFF];
            migrate_in_server->server_.to_string(server_str, OB_IP_STR_BUFF);
            TBSYS_LOG(DEBUG, "disk is not enougth or server is busy, "
                      "dest_cs=%s percent=%d trigger_level=%d migrate_finish=%ld now=%ld", 
                      server_str, 
                      migrate_in_server->disk_info_.get_percent(), 
                      balance_prameter_.disk_trigger_level_, 
                      migrate_in_server->migrate_in_finish_time_, monotonic_now);
            migrate_in_server = NULL;
            continue;
          }
        }
        if (migrate_in_server != NULL)
        {
          //try this server
          ObCandidateServerBySharedManager2::effectiveServer effective_server;
          effective_server.server_indexes_[0] = migrate_in_server - server_manager_.begin();
          dest_server_index = migrate_in_server - server_manager_.begin();
          {
            tbsys::CRLockGuard guard(server_manager_rwlock_);
            candidate_shared_helper_.init(effective_server, &server_manager_);
          }
          {
            candidate_shared_helper_.scan_root_table(root_table_for_query_);
          }
          candidate_shared_helper_.sort();

          ObRootTable2::const_iterator it;
          int32_t lowest_shared_count = -1;
          int32_t shared_count_in_range = 0;
          bool consider_this = false;
          ObCandidateServerBySharedManager2::sharedInfo* shared_info = NULL;
          root_table_index_it = root_table_for_query_->sorted_end();
          for (it = root_table_for_query_->begin(); it < root_table_for_query_->sorted_end(); it++)
          {
            //we will find a suitable tablet
            consider_this = false;
            for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; i++)
            {
              if (it->server_info_indexes_[i] == src_server_index)
              {
                consider_this = true;
              }
              if (it->migrate_monotonic_time_ >= monotonic_now)
              {
                consider_this = false;
                break;
              }
              if (it->server_info_indexes_[i] == dest_server_index)
              {
                consider_this = false;
                break;
              }
              //only the tablet in src_server and not in dest server can be consided;
            }
            if (consider_this)
            {
              shared_count_in_range = 0;
              for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; i++)
              {
                if (it->server_info_indexes_[i] != src_server_index && it->server_info_indexes_[i] != OB_INVALID_INDEX)
                {
                  shared_info = candidate_shared_helper_.find(it->server_info_indexes_[i]);
                  if (shared_info != NULL)
                  {
                    shared_count_in_range += shared_info->shared_count_;
                  }
                }
              }
              if (lowest_shared_count == -1 || lowest_shared_count > shared_count_in_range)
              {
                lowest_shared_count = shared_count_in_range;
                root_table_index_it = it;
              }
            }
          } // end for
        }
        if (migrate_in_server == NULL || root_table_index_it == root_table_for_query_->sorted_end() 
            || src_server_index == OB_INVALID_INDEX || dest_server_index == OB_INVALID_INDEX)
        {
          //not find suitable tablet;
          migrate_in_server = NULL;
        }
        else
        {
          const common::ObTabletInfo* tablet_info = NULL;
          tablet_info = ((const ObRootTable2*)root_table_for_query_)->get_tablet_info(root_table_index_it);
          if (NULL == tablet_info)
          {
            TBSYS_LOG(ERROR, "why this is NULL bugs!!!");
          }
          else
          {
            if (server_manager_.can_migrate_in(dest_server_index, tablet_info->occupy_size_, &balance_prameter_))
            {
              ObBatchMigrateInfo bmi(&tablet_info->range_, src_server_index, dest_server_index, false);
              int ret = batch_migrate_helper_.add(bmi, monotonic_now, server_manager_);
              if (ret == ObBatchMigrateInfoManager::ADD_REACH_MAX) 
              {
                return_code = SHOULD_DO_MIGRATE;
                break;
              }
              else if (ObBatchMigrateInfoManager::ADD_OK == ret)
              {
                root_table_index_it->migrate_monotonic_time_ = monotonic_now + migrate_wait_seconds_;
              }
            }
            else
            {
              migrate_in_server = NULL;
            }
          }
        }
      }
      if (NULL == migrate_in_server)
      {
        TBSYS_LOG(ERROR, "can not find suitable server");
        return_code =CAN_NOT_FIND_SUTABLE_SERVER;
      }
      return return_code;
    }
    void ObRootServer2::do_balance()
    {
      add_lost_tablet();
      ObServer server;
      char msg[OB_IP_STR_BUFF];
      ObServerStatus* migrate_server = NULL;
      int move_ret = 0;
      int64_t monotonic_now = 0;
      do 
      {
        migrate_server = NULL;
        {
          monotonic_now = tbsys::CTimeUtil::getMonotonicTime();
          //tbsys::CRLockGuard guard(server_manager_rwlock_); // do not need this lock
          ObChunkServerManager::iterator it = server_manager_.begin();
          for (; it != server_manager_.end(); ++it) 
          {
            if (it->status_ != ObServerStatus::STATUS_DEAD &&
                it->disk_info_.get_percent() > balance_prameter_.disk_high_level_ &&
                it->migrate_out_finish_time_ < monotonic_now //this means this server is not in migrating out
               )
            {
              migrate_server = it;
              server = it->server_;
              server.set_port(it->port_cs_);
              server.to_string(msg,OB_IP_STR_BUFF);
              TBSYS_LOG(ERROR, "server %s reach disk high leve", msg);
              move_ret = move_out_tablet(migrate_server, monotonic_now);
              if (move_ret == SHOULD_DO_MIGRATE)
              {
                break;
              }
              else if (move_ret == CAN_NOT_FIND_SUTABLE_SERVER)
              {
                migrate_server = NULL;
              }
            }
          }
        }//release lock
        {
          tbsys::CThreadGuard guard(&status_mutex_);
          if (server_status_ == STATUS_INTERRUPT_BALANCING)
          {
            break;
          }
        }
        execute_batch_migrate();
      } while ( false);
      //deal with trigger level
      do 
      {
        {
          tbsys::CRLockGuard guard(root_table_rwlock_);
          migrate_server = NULL;
          {
            monotonic_now = tbsys::CTimeUtil::getMonotonicTime();
            //tbsys::CRLockGuard guard(server_manager_rwlock_);  //do not need this lock
            ObChunkServerManager::iterator it = server_manager_.begin();
            for (; it != server_manager_.end(); ++it) 
            {
              if (it->status_ != ObServerStatus::STATUS_DEAD &&
                  it->disk_info_.get_percent() > balance_prameter_.disk_trigger_level_ &&
                  it->migrate_out_finish_time_ < monotonic_now //this means this server is not in migrating out
                 )
              {
                migrate_server = it;
                server = it->server_;
                server.set_port(it->port_cs_);
                server.to_string(msg,OB_IP_STR_BUFF);
                TBSYS_LOG(WARN, "server %s reach disk trigger leve", msg);
                move_ret = move_out_tablet(migrate_server, monotonic_now);
                if (move_ret == SHOULD_DO_MIGRATE)
                {
                  break;
                }else if (move_ret == CAN_NOT_FIND_SUTABLE_SERVER)
                {
                  migrate_server = NULL;
                }
              }
            }
          }
        }//release lock
        {
          tbsys::CThreadGuard guard(&status_mutex_);
          if (server_status_ == STATUS_INTERRUPT_BALANCING)
          {
            break;
          }
        }
        execute_batch_migrate();
      } while (false);
    }

    /*
     * 从本地读取新schema, 判断兼容性
     */
    int ObRootServer2::switch_schema(int64_t time_stamp)
    {
      TBSYS_LOG(INFO, "switch_schema time_stamp is %ld", time_stamp);
      int res = OB_SUCCESS;
      tbsys::CConfig config;
      common::ObSchemaManagerV2* new_schema_manager = NULL;
      new_schema_manager = new(std::nothrow)ObSchemaManagerV2(time_stamp);

      if (new_schema_manager == NULL)
      {
        TBSYS_LOG(ERROR, "new ObSchemaManagerV2() error");
        res = OB_ERROR;
      }
      else if (! new_schema_manager->parse_from_file(schema_file_name_, config))
      {
        TBSYS_LOG(ERROR, "parse schema error chema file is %s ", schema_file_name_);
        res = OB_ERROR;
      }
      else 
      {
        tbsys::CWLockGuard guard(schema_manager_rwlock_);
        if (! schema_manager_->is_compatible(*new_schema_manager))
        {
          TBSYS_LOG(ERROR, "new schema can not compitable with the old one"
              " schema file name is  %s ", schema_file_name_);
          res = OB_ERROR;
        }
        else
        {
          common::ObSchemaManagerV2* old_schema_manager = NULL;
          old_schema_manager = schema_manager_;
          schema_manager_ = new_schema_manager;
          new_schema_manager = old_schema_manager;
        }
      } //releae lock
      if (new_schema_manager != NULL) 
      {
        delete new_schema_manager;
        new_schema_manager = NULL;
      }
      return res;
    }

    int ObRootServer2::init_root_table_by_report()
    {
      TBSYS_LOG(INFO, "init_root_table_by_report begin");
      int  res = OB_SUCCESS;
      if (tablet_manager_for_build_ != NULL)
      {
        delete tablet_manager_for_build_;
        tablet_manager_for_build_ = NULL;
      }
      if (root_table_for_build_ != NULL)
      {
        delete root_table_for_build_;
        root_table_for_build_ = NULL;
      }
      //root_table_for_query we need create new table
      tablet_manager_for_build_ = new(std::nothrow)ObTabletInfoManager;
      if (tablet_manager_for_build_ == NULL) 
      {
        TBSYS_LOG(ERROR, "new ObTabletInfoManager error");
        res = OB_ERROR;
      }
      root_table_for_build_ = new(std::nothrow)ObRootTable2(tablet_manager_for_build_);
      TBSYS_LOG(INFO, "new root_table_for_build=%p", root_table_for_build_);
      if (root_table_for_build_ == NULL) 
      {
        TBSYS_LOG(ERROR, "new ObRootTable2 error");
        res = OB_ERROR;
      }
      {
        tbsys::CThreadGuard guard(&status_mutex_);
        server_status_ = STATUS_CHANGING;
        TBSYS_LOG(INFO, "server_status_ = %d", server_status_);
      }
      //wail until have cs registed 
      while (OB_SUCCESS == res && !first_cs_had_registed_ && !receive_stop_)
      {
        TBSYS_LOG(INFO, "wait for the first cs regist");
        sleep(1);
      }
      bool finish_init = false;
      while(OB_SUCCESS == res && !finish_init && !receive_stop_)
      {
        TBSYS_LOG(INFO, "clock click ");
        sleep(wait_init_time_/ (1000L * 1000L));
        ObTabletInfoManager* tablet_manager_tmp_ = NULL;
        ObRootTable2* root_table_tmp_ = NULL;
        if (OB_SUCCESS == res)
        {
          tablet_manager_tmp_ = new(std::nothrow)ObTabletInfoManager;
          if (tablet_manager_tmp_ == NULL) 
          {
            TBSYS_LOG(ERROR, "new ObTabletInfoManager error");
            res = OB_ERROR;
          }
          root_table_tmp_ = new(std::nothrow)ObRootTable2(tablet_manager_tmp_);
          if (root_table_tmp_ == NULL) 
          {
            TBSYS_LOG(ERROR, "new ObRootTable2 error");
            res = OB_ERROR;
          }
        }
        if (OB_SUCCESS == res)
        {
          tbsys::CThreadGuard guard_build(&root_table_build_mutex_);
          root_table_for_build_->sort();
          bool check_ok = false;
          check_ok = (OB_SUCCESS == root_table_for_build_->shrink_to(root_table_tmp_));
          if (check_ok)
          {
            root_table_tmp_->sort();
          }
          else
          {
            TBSYS_LOG(ERROR, "shrink table error");
          }
          if (check_ok)
          {
            check_ok = root_table_tmp_->check_lost_range();
            if (!check_ok)
            {
              TBSYS_LOG(ERROR, "check root table failed we will wait for another %ld seconds", wait_init_time_/(1000L * 1000L));
            }
          }
          if (check_ok)
          {
            check_ok = root_table_tmp_->check_tablet_copy_count(safe_copy_count_in_init_);
            if (!check_ok)
            {
              TBSYS_LOG(ERROR, "check root table copy_count fail we will wait for another %ld seconds", wait_init_time_/(1000L * 1000L));
            }
          }
          if (check_ok) 
          {
            //if all server is reported and 
            //we have no tablets, we create it

            bool all_server_is_reported = true;
            int32_t server_count = 0;
            { 
              tbsys::CRLockGuard guard(server_manager_rwlock_);
              ObChunkServerManager::iterator it = server_manager_.begin();
              for (; it != server_manager_.end(); ++it)
              {
                if(it->port_cs_ != 0 && (it->status_ == ObServerStatus::STATUS_REPORTING ||
                                         it->status_ == ObServerStatus::STATUS_DEAD))
                {
                  all_server_is_reported = false;
                  break;
                }
                ++server_count;
              }
            }

            if (server_count > 0 && all_server_is_reported &&
                root_table_tmp_->is_empty() && (create_table_in_init_ != 0) && is_master() )
            {
              TBSYS_LOG(INFO,"chunkservers have no data,create new table");
              create_new_table_in_init(schema_manager_,root_table_tmp_);
            }

            //check we have we have enougth table
            tbsys::CRLockGuard guard(schema_manager_rwlock_);
            for (const ObTableSchema* it = schema_manager_->table_begin(); it != schema_manager_->table_end(); ++it)
            {
              if (it->get_table_id() != OB_INVALID_ID && !it->is_pure_update_table())
              {
                if(!root_table_tmp_->table_is_exist(it->get_table_id()))
                {
                  TBSYS_LOG(ERROR, "table_id = %lu has not been created are you sure about this?", it->get_table_id() );
                  check_ok = false;
                  break;
                }
              }
            }
          }
          if (check_ok)
          {
            delete root_table_for_build_;
            root_table_for_build_ = NULL;
            delete tablet_manager_for_build_;
            tablet_manager_for_build_ = NULL;
            tbsys::CWLockGuard guard(root_table_rwlock_);
            delete root_table_for_query_;
            root_table_for_query_ = root_table_tmp_;
            TBSYS_LOG(INFO, "new root table created, root_table_for_query=%p", root_table_for_query_);
            root_table_tmp_ = NULL;
            delete tablet_manager_for_query_;
            tablet_manager_for_query_ = tablet_manager_tmp_;
            tablet_manager_tmp_ = NULL;
            finish_init = true;
          }
        }
        if (tablet_manager_tmp_ != NULL)
        {
          delete tablet_manager_tmp_;
          tablet_manager_tmp_ = NULL;
        }
        if (root_table_tmp_ != NULL)
        {
          delete root_table_tmp_;
          root_table_tmp_ = NULL;
        }
      }//end while
      if (root_table_for_build_ != NULL) 
      {
        delete root_table_for_build_;
        root_table_for_build_ = NULL;
      }
      if (tablet_manager_for_build_ != NULL) 
      {
        delete tablet_manager_for_build_;
        tablet_manager_for_build_ = NULL;
      }
      {
        tbsys::CWLockGuard guard(server_manager_rwlock_);
        ObChunkServerManager::iterator it = server_manager_.begin();
        for (; it != server_manager_.end(); ++it)
        {
          if(it->status_ == ObServerStatus::STATUS_REPORTED || it->status_ == ObServerStatus::STATUS_REPORTING)
          {
            it->status_ = ObServerStatus::STATUS_SERVING;
          }
        }
      }

      build_sync_flag_ = BUILD_SYNC_INIT_OK;
      {
        tbsys::CThreadGuard guard(&(status_mutex_));
        if (server_status_ == STATUS_CHANGING)
        {
          server_status_ = STATUS_SLEEP;
          TBSYS_LOG(INFO, "server_status_ = %d", server_status_);
        }
      }

      if (res == OB_SUCCESS && finish_init && is_master())
      {
        TBSYS_LOG(INFO, "before do check point, res: %d", res);
        tbsys::CRLockGuard rt_guard(root_table_rwlock_);
        tbsys::CRLockGuard cs_guard(server_manager_rwlock_);
        tbsys::CThreadGuard st_guard(&status_mutex_);
        tbsys::CThreadGuard log_guard(worker_->get_log_manager()->get_log_sync_mutex());
        int ret = worker_->get_log_manager()->do_check_point();
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "do check point after init root table failed, err: %d", ret);
        }
        else
        {
          TBSYS_LOG(INFO, "do check point after init root table done");
        }
      }
      TBSYS_LOG(INFO, "init_root_table_by_report finished, res=%d", res);
      return res;
    }

    /*
     * chunk serve和merege server注册
     * @param out status 0 do not start report 1 start report
     */
    int ObRootServer2::regist_server(const ObServer& server, bool is_merge, int32_t& status, int64_t time_stamp)
    {
      int ret = OB_NOT_INIT;
      if (server_status_ != STATUS_INIT ) // not in init
      {
        ret = OB_SUCCESS;
        if (TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_INFO)
        {
          char server_char[OB_IP_STR_BUFF];
          server.to_string(server_char, OB_IP_STR_BUFF);
          TBSYS_LOG(INFO, "regist server %s is_merge %d", server_char, is_merge);
        }
        if (is_master())
        {
          if (time_stamp < 0)
            time_stamp = tbsys::CTimeUtil::getTime();

          if (is_merge)
          {
            ret = log_worker_->regist_ms(server, time_stamp);
          }
          else
          {
            ret = log_worker_->regist_cs(server, time_stamp);
          }
        }

        if (ret == OB_SUCCESS)
        {
          {
            time_stamp = tbsys::CTimeUtil::getMonotonicTime();
            int rc = 0;
            tbsys::CWLockGuard guard(server_manager_rwlock_);
            rc = server_manager_.receive_hb(server, time_stamp, is_merge, true);
          }
          first_cs_had_registed_ = true;
          //now we always want cs report its tablet
          status = START_REPORTING;
          if (!is_merge && START_REPORTING == status)
          {
            tbsys::CThreadGuard mutex_guard(&root_table_build_mutex_); //this for only one thread modify root_table
            tbsys::CWLockGuard root_table_guard(root_table_rwlock_);
            tbsys::CWLockGuard server_info_guard(server_manager_rwlock_);
            ObChunkServerManager::iterator it;
            it = server_manager_.find_by_ip(server);
            if (it != server_manager_.end())
            {
              {
                //remove this server's tablet
                if (root_table_for_query_ != NULL) 
                {
                  root_table_for_query_->server_off_line(it - server_manager_.begin(), 0);
                }
              }
              if (it->status_ == ObServerStatus::STATUS_SERVING || it->status_ == ObServerStatus::STATUS_WAITING_REPORT)
              {
                // chunk server will start report
                it->status_ = ObServerStatus::STATUS_REPORTING;
              }
            }
          }

        }
        TBSYS_LOG(INFO, "regist ret %d", ret);
      }
      return ret;
    }

    /*
     * chunk server更新自己的磁盘情况信息
     */
    int ObRootServer2::update_capacity_info(const common::ObServer& server, const int64_t capacity, const int64_t used)
    {
      int ret = OB_SUCCESS;
      if (is_master())
      {
        ret = log_worker_->report_cs_load(server, capacity, used);
      }

      if (ret == OB_SUCCESS)
      {
        ObServerDiskInfo disk_info;
        disk_info.set_capacity(capacity);
        disk_info.set_used(used);
        tbsys::CWLockGuard guard(server_manager_rwlock_);
        ret = server_manager_.update_disk_info(server, disk_info);
      }

      if (TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_INFO)
      {
        char server_str[OB_IP_STR_BUFF];
        server.to_string(server_str, OB_IP_STR_BUFF);
        TBSYS_LOG(INFO, "server %s update capacity info capacity is %ld, used is %ld ret is %d", server_str, capacity, used, ret);
      }

      return ret;
    }

    /*
     * 迁移完成操作
     */
    int ObRootServer2::migrate_over(const ObRange& range, const common::ObServer& src_server, const common::ObServer& dest_server, const bool keep_src, const int64_t tablet_version)
    {
      int ret = OB_SUCCESS;
      if (TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_INFO)
      {
        static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
        range.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
        TBSYS_LOG(INFO, "%s", row_key_dump_buff);
        //range.hex_dump(TBSYS_LOG_LEVEL_INFO);
        char f_server[OB_IP_STR_BUFF];
        char t_server[OB_IP_STR_BUFF];
        src_server.to_string(f_server, OB_IP_STR_BUFF);
        dest_server.to_string(t_server, OB_IP_STR_BUFF);
        TBSYS_LOG(INFO, "migrate_over received from server %s to server %s keep src =%d", f_server, t_server, keep_src);
      }
      int src_server_index = get_server_index(src_server);
      int dest_server_index = get_server_index(dest_server);
      if (src_server_index == OB_INVALID_INDEX || dest_server_index == OB_INVALID_INDEX)
      {
        TBSYS_LOG(ERROR," can not find server info");
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        ObRootTable2::const_iterator start_it;
        ObRootTable2::const_iterator end_it;
        tbsys::CThreadGuard mutex_gard(&root_table_build_mutex_);
        tbsys::CWLockGuard guard(root_table_rwlock_);
        int find_ret = root_table_for_query_->find_range(range, start_it, end_it);
        if (OB_SUCCESS == find_ret && start_it == end_it)
        {
          //int64_t max_tablet_version = root_table_for_query_->get_max_tablet_version(start_it);
          //if (max_tablet_version > tablet_version)
          //{
          //  TBSYS_LOG(INFO ,"migrated tablet is old, ignore this max_tablet_version =%ld this version = %ld",
          //      max_tablet_version, tablet_version);
          //}
          //else
          {
            if (is_master())
            {
              ret = log_worker_->cs_migrate_done(range, src_server, dest_server, keep_src, tablet_version);
            }
            start_it->migrate_monotonic_time_ = 0;
            if (keep_src)
            {
              ret = root_table_for_query_->modify(start_it, dest_server_index, tablet_version);
            }
            else
            {
              ret = root_table_for_query_->replace(start_it, src_server_index, dest_server_index, tablet_version);
            }
            if (OB_SUCCESS == find_ret)
            {
              ObServerStatus* src_status = server_manager_.get_server_status(src_server_index);
              ObServerStatus* dest_status = server_manager_.get_server_status(dest_server_index);
              const common::ObTabletInfo* tablet_info = NULL;
              tablet_info = ((const ObRootTable2*)root_table_for_query_)->get_tablet_info(start_it);
              if (src_status != NULL && dest_status != NULL && tablet_info != NULL)
              {
                src_status->migrate_out_finish_time_ = 0;
                dest_status->migrate_in_finish_time_ = 0;
                if (!keep_src)
                {
                  src_status->disk_info_.set_used(src_status->disk_info_.get_used() - tablet_info->occupy_size_);
                }
                dest_status->disk_info_.set_used(dest_status->disk_info_.get_used() + tablet_info->occupy_size_);
              }
            }
          }
        }
        else
        {
          TBSYS_LOG(INFO, "can not find the right range ignore this");
        }
      }
      return ret;
    }
    int ObRootServer2::make_out_cell(ObCellInfo& out_cell, ObRootTable2::const_iterator first, 
        ObRootTable2::const_iterator last, ObScanner& scanner, const int32_t max_row_count, const int32_t max_key_len) const
    {
      static ObString s_root_1_port(strlen(ROOT_1_PORT), strlen(ROOT_1_PORT), (char*)ROOT_1_PORT);
      static ObString s_root_1_ms_port(strlen(ROOT_1_MS_PORT), strlen(ROOT_1_MS_PORT), (char*)ROOT_1_MS_PORT);
      static ObString s_root_1_ipv6_1(strlen(ROOT_1_IPV6_1), strlen(ROOT_1_IPV6_1), (char*)ROOT_1_IPV6_1);
      static ObString s_root_1_ipv6_2(strlen(ROOT_1_IPV6_2), strlen(ROOT_1_IPV6_2), (char*)ROOT_1_IPV6_2);
      static ObString s_root_1_ipv6_3(strlen(ROOT_1_IPV6_3), strlen(ROOT_1_IPV6_3), (char*)ROOT_1_IPV6_3);
      static ObString s_root_1_ipv6_4(strlen(ROOT_1_IPV6_4), strlen(ROOT_1_IPV6_4), (char*)ROOT_1_IPV6_4);
      static ObString s_root_1_ipv4(strlen(ROOT_1_IPV4), strlen(ROOT_1_IPV4), (char*)ROOT_1_IPV4);
      static ObString s_root_1_tablet_version(strlen(ROOT_1_TABLET_VERSION), strlen(ROOT_1_TABLET_VERSION), (char*)ROOT_1_TABLET_VERSION);

      static ObString s_root_2_port(   strlen(ROOT_2_PORT),    strlen(ROOT_2_PORT),    (char*)ROOT_2_PORT);
      static ObString s_root_2_ms_port(strlen(ROOT_2_MS_PORT), strlen(ROOT_2_MS_PORT), (char*)ROOT_2_MS_PORT);
      static ObString s_root_2_ipv6_1( strlen(ROOT_2_IPV6_1),  strlen(ROOT_2_IPV6_1),  (char*)ROOT_2_IPV6_1);
      static ObString s_root_2_ipv6_2( strlen(ROOT_2_IPV6_2),  strlen(ROOT_2_IPV6_2),  (char*)ROOT_2_IPV6_2);
      static ObString s_root_2_ipv6_3( strlen(ROOT_2_IPV6_3),  strlen(ROOT_2_IPV6_3),  (char*)ROOT_2_IPV6_3);
      static ObString s_root_2_ipv6_4( strlen(ROOT_2_IPV6_4),  strlen(ROOT_2_IPV6_4),  (char*)ROOT_2_IPV6_4);
      static ObString s_root_2_ipv4(   strlen(ROOT_2_IPV4),    strlen(ROOT_2_IPV4),    (char*)ROOT_2_IPV4);
      static ObString s_root_2_tablet_version(strlen(ROOT_2_TABLET_VERSION), strlen(ROOT_2_TABLET_VERSION), (char*)ROOT_2_TABLET_VERSION);

      static ObString s_root_3_port(   strlen(ROOT_3_PORT),    strlen(ROOT_3_PORT),    (char*)ROOT_3_PORT);
      static ObString s_root_3_ms_port(strlen(ROOT_3_MS_PORT), strlen(ROOT_3_MS_PORT), (char*)ROOT_3_MS_PORT);
      static ObString s_root_3_ipv6_1( strlen(ROOT_3_IPV6_1),  strlen(ROOT_3_IPV6_1),  (char*)ROOT_3_IPV6_1);
      static ObString s_root_3_ipv6_2( strlen(ROOT_3_IPV6_2),  strlen(ROOT_3_IPV6_2),  (char*)ROOT_3_IPV6_2);
      static ObString s_root_3_ipv6_3( strlen(ROOT_3_IPV6_3),  strlen(ROOT_3_IPV6_3),  (char*)ROOT_3_IPV6_3);
      static ObString s_root_3_ipv6_4( strlen(ROOT_3_IPV6_4),  strlen(ROOT_3_IPV6_4),  (char*)ROOT_3_IPV6_4);
      static ObString s_root_3_ipv4(   strlen(ROOT_3_IPV4),    strlen(ROOT_3_IPV4),    (char*)ROOT_3_IPV4);
      static ObString s_root_3_tablet_version(strlen(ROOT_3_TABLET_VERSION), strlen(ROOT_3_TABLET_VERSION), (char*)ROOT_3_TABLET_VERSION);

      static ObString s_root_occupy_size (strlen(ROOT_OCCUPY_SIZE), strlen(ROOT_OCCUPY_SIZE), (char*)ROOT_OCCUPY_SIZE);
      static ObString s_root_record_count(strlen(ROOT_RECORD_COUNT), strlen(ROOT_RECORD_COUNT), (char*)ROOT_RECORD_COUNT);
      static ObString s_root_crc_sum(strlen(ROOT_CRC_SUM), strlen(ROOT_CRC_SUM), (char*)ROOT_CRC_SUM);
      static char c = 0;

      int ret = OB_SUCCESS;
      if (c == 0)
      {
        memset(max_row_key, 0xff, OB_MAX_ROW_KEY_LENGTH);
        c = 1;
      }
      const common::ObTabletInfo* tablet_info = NULL;
      int count = 0;
      ObRootTable2::const_iterator it = first;
      for (; it <= last; it++) 
      {
        if (count > max_row_count) break;
        tablet_info = ((const ObRootTable2*)root_table_for_query_)->get_tablet_info(it);
        if (tablet_info == NULL)
        {
          TBSYS_LOG(ERROR, "you should not reach this bugs");
          break;
        }
        out_cell.row_key_ = tablet_info->range_.end_key_;
        if (tablet_info->range_.border_flag_.is_max_value())
        {
          out_cell.row_key_.assign(max_row_key, max_key_len);
        }
        TBSYS_LOG(DEBUG, "add a row key to out cell, length = %d", out_cell.row_key_.length());
        count++;
        //start one row
        out_cell.column_name_ = s_root_occupy_size;
        out_cell.value_.set_int(tablet_info->row_count_);
        if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
        {
          break;
        }

        out_cell.column_name_ = s_root_record_count;
        out_cell.value_.set_int(tablet_info->occupy_size_);
        if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
        {
          break;
        }

        //out_cell.column_name_ = s_root_crc_sum;
        //out_cell.value_.set_int(tablet_info->crc_sum_);
        //if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
        //{
        //  break;
        //}

        const ObServerStatus* server_status = NULL;
        if (it->server_info_indexes_[0] != OB_INVALID_INDEX && 
            (server_status = server_manager_.get_server_status(it->server_info_indexes_[0])) != NULL)
        {
          out_cell.column_name_ = s_root_1_port;
          out_cell.value_.set_int(server_status->port_cs_);

          if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
          {
            break;
          }
          if (server_status->server_.get_version() != ObServer::IPV4)
          {
            ret = OB_NOT_SUPPORTED;
            break;
          }
          out_cell.column_name_ = s_root_1_ms_port;
          out_cell.value_.set_int(server_status->port_ms_);
          if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
          {
            break;
          }

          out_cell.column_name_ = s_root_1_ipv4;
          out_cell.value_.set_int(server_status->server_.get_ipv4());
          if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
          {
            break;
          }

          out_cell.column_name_ = s_root_1_tablet_version;
          out_cell.value_.set_int(it->tablet_version_[0]);
          if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
          {
            break;
          }
        }
        if (it->server_info_indexes_[1] != OB_INVALID_INDEX && 
            (server_status = server_manager_.get_server_status(it->server_info_indexes_[1])) != NULL)
        {
          out_cell.column_name_ = s_root_2_port;
          out_cell.value_.set_int(server_status->port_cs_);
          if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
          {
            break;
          }
          if (server_status->server_.get_version() != ObServer::IPV4)
          {
            ret = OB_NOT_SUPPORTED;
            break;
          }
          out_cell.column_name_ = s_root_2_ms_port;
          out_cell.value_.set_int(server_status->port_ms_);
          if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
          {
            break;
          }

          out_cell.column_name_ = s_root_2_ipv4;
          out_cell.value_.set_int(server_status->server_.get_ipv4());
          if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
          {
            break;
          }

          out_cell.column_name_ = s_root_2_tablet_version;
          out_cell.value_.set_int(it->tablet_version_[1]);
          if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
          {
            break;
          }
        }
        if (it->server_info_indexes_[2] != OB_INVALID_INDEX && 
            (server_status = server_manager_.get_server_status(it->server_info_indexes_[2])) != NULL)
        {
          out_cell.column_name_ = s_root_3_port;
          out_cell.value_.set_int(server_status->port_cs_);
          if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
          {
            break;
          }
          if (server_status->server_.get_version() != ObServer::IPV4)
          {
            ret = OB_NOT_SUPPORTED;
            break;
          }
          out_cell.column_name_ = s_root_3_ms_port;
          out_cell.value_.set_int(server_status->port_ms_);
          if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
          {
            break;
          }

          out_cell.column_name_ = s_root_3_ipv4;
          out_cell.value_.set_int(server_status->server_.get_ipv4());
          if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
          {
            break;
          }

          out_cell.column_name_ = s_root_3_tablet_version;
          out_cell.value_.set_int(it->tablet_version_[2]);
          if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
          {
            break;
          }
        }

      }
      return ret;
    }

    int ObRootServer2::find_root_table_key(const common::ObString& table_name, const common::ObString& key, common::ObScanner& scanner) const
    {
      int32_t max_key_len = 0;
      uint64_t table_id = get_table_info(table_name, max_key_len);
      return find_root_table_key(table_id, table_name, max_key_len, key, scanner);
    }
    int ObRootServer2::find_root_table_key(const uint64_t table_id, const common::ObString& key, common::ObScanner& scanner) const
    {
      char table_name_buff[OB_MAX_TABLE_NAME_LENGTH];
      ObString table_name(OB_MAX_TABLE_NAME_LENGTH, 0, table_name_buff);
      int32_t max_key_len = 0;
      int ret = get_table_info(table_id, table_name, max_key_len);
      if (OB_SUCCESS == ret) 
      {
        ret = find_root_table_key(table_id, table_name, max_key_len, key, scanner);
      }
      return ret;
    }
    int ObRootServer2::find_root_table_key(const uint64_t table_id, const ObString& table_name, const int32_t max_key_len, const common::ObString& key, ObScanner& scanner) const
    {
      int ret = OB_SUCCESS;
      if (table_id == OB_INVALID_ID || 0 == table_id)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      if (OB_SUCCESS == ret) 
      {
        ObCellInfo out_cell;
        out_cell.table_name_ = table_name;
        tbsys::CRLockGuard guard(root_table_rwlock_);
        if (root_table_for_query_ == NULL) 
        {
          ret = OB_NOT_INIT;
        }
        else
        {
          ObRootTable2::const_iterator first;
          ObRootTable2::const_iterator last;
          ObRootTable2::const_iterator ptr;
          ret = root_table_for_query_->find_key(table_id, key, RETURN_BACH_COUNT, first, last, ptr);
          TBSYS_LOG(DEBUG, "first %p last %p ptr %p", first, last, ptr);
          if (ret == OB_SUCCESS)
          {
            if (first == ptr)
            {
              // make a fake startkey
              out_cell.value_.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
              ret = scanner.add_cell(out_cell);
              out_cell.value_.reset();
            }
            if (OB_SUCCESS == ret)
            {
              ret = make_out_cell(out_cell, first, last, scanner, RETURN_BACH_COUNT, max_key_len);
            }
          }
        }
      }
      return ret;
    }
    int ObRootServer2::find_root_table_range(const ObString& table_name, const common::ObRange& key_range, ObScanner& scanner) const
    {

      int ret = OB_SUCCESS;
      int32_t max_key_len = 0;
      uint64_t table_id = get_table_info(table_name, max_key_len);

      if (0 == table_id || OB_INVALID_ID == table_id)
      {
        TBSYS_LOG(WARN,"table name are invaild");
        ret = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == ret) 
      {
        ObCellInfo out_cell;
        out_cell.table_name_ = table_name;
        tbsys::CRLockGuard guard(root_table_rwlock_);
        if (root_table_for_query_ == NULL) 
        {
          ret = OB_NOT_INIT;
          TBSYS_LOG(WARN,"scan request in initialize phase");
        }
        else
        {
          ObRootTable2::const_iterator first;
          ObRootTable2::const_iterator last;
          ObRange search_range = key_range;
          search_range.table_id_ = table_id;
          ret = root_table_for_query_->find_range(search_range, first, last);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(WARN,"cann't find this range,ret[%d]",ret);
          }
          else 
          {
            if ((ret = make_out_cell(out_cell, first, last, scanner, 
                                     MAX_RETURN_BACH_ROW_COUNT, max_key_len)) != OB_SUCCESS)
            {
              TBSYS_LOG(WARN,"make out cell failed,ret[%d]",ret);
            }
          }
        }
      }
      return ret;
    }

    int ObRootServer2::echo_update_server_freeze_mem()
    {
      int res = OB_ERROR;
      if (update_server_status_.status_ == ObServerStatus::STATUS_SERVING)
      {
        if (is_master())
        {
          log_worker_->us_mem_freezing(update_server_status_.server_, frozen_mem_version_);
        }
        update_server_status_.status_ = ObServerStatus::STATUS_REPORTING;
        res = OB_SUCCESS;
      }
      return res;
    }
    int ObRootServer2::echo_start_merge_received(const common::ObServer& server)
    {
      int res = OB_ERROR;
      ObChunkServerManager::iterator it;
      it = server_manager_.find_by_ip(server);
      if (it != server_manager_.end())
      {
        if (it->status_ == ObServerStatus::STATUS_SERVING || it->status_ == ObServerStatus::STATUS_WAITING_REPORT)
        {
          if (is_master())
          {
            log_worker_->cs_start_merging(server);
          }

          it->status_ = ObServerStatus::STATUS_REPORTING;
          res = OB_SUCCESS;
        }
      }
      return res;
    }

    /*
     * 切换过程中, update server冻结内存表 或者chunk server 进行merge等耗时操作完成
     * 发送消息调用此函数
     */
    int ObRootServer2::waiting_job_done(const common::ObServer& server, const int64_t frozen_mem_version)
    {
      int res = OB_ERROR;
      UNUSED(frozen_mem_version);
      if (TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_INFO)
      {
        char f_server[OB_IP_STR_BUFF];
        server.to_string(f_server, OB_IP_STR_BUFF);
        TBSYS_LOG(INFO, "waiting_job_done server is %s update_server_status_.status_ %d", 
            f_server, update_server_status_.status_);
      }
      if (update_server_status_.server_ == server)
      {
        if (update_server_status_.status_ == ObServerStatus::STATUS_REPORTING)
        {
          if (is_master())
          {
            log_worker_->us_mem_frozen(server, frozen_mem_version);
          }
          update_server_status_.status_ = ObServerStatus::STATUS_REPORTED;
          res = OB_SUCCESS;
        }
      }
      else
      {
        if (is_master())
        {
          log_worker_->cs_merge_over(server, frozen_mem_version);
        }
        ObChunkServerManager::iterator it;
        tbsys::CWLockGuard guard(server_manager_rwlock_);
        it = server_manager_.find_by_ip(server);
        if (it != server_manager_.end())
        {
          TBSYS_LOG(INFO, " it->status_ ObServerStatus::STATUS_REPORTING %ld %d,froze version:%ld", it->status_, ObServerStatus::STATUS_REPORTING,frozen_mem_version_);
          if (it->status_ == ObServerStatus::STATUS_REPORTING)
          {
            it->status_ = ObServerStatus::STATUS_REPORTED;
            if (frozen_mem_version_ == -1) // not in merge process
            {
              it->status_ = ObServerStatus::STATUS_SERVING;
            }
          }

        }
        res = OB_SUCCESS;
      }
      return res;
    }
    int ObRootServer2::echo_unload_received(const common::ObServer& server)
    {
      int res = OB_ERROR;
      if (update_server_status_.server_ == server)
      {
        if (update_server_status_.status_ == ObServerStatus::STATUS_REPORTED)
        {
          if (is_master())
          {
            log_worker_->unload_us_done(server);
          }
          update_server_status_.status_ = ObServerStatus::STATUS_SERVING;
          res = OB_SUCCESS;
        }
      }
      else
      {
        ObChunkServerManager::iterator it;
        it = server_manager_.find_by_ip(server);
        if (it != server_manager_.end())
        {
          if (it->status_ == ObServerStatus::STATUS_REPORTED || it->status_ == ObServerStatus::STATUS_REPORTING)
          {
            if (is_master())
            {
              log_worker_->unload_cs_done(server);
            }
            it->status_ = ObServerStatus::STATUS_SERVING;
            res = OB_SUCCESS;
          }

        }
      }
      return res;
    }

    int ObRootServer2::get_server_index(const common::ObServer& server) const
    {
      int ret = OB_INVALID_INDEX;
      ObChunkServerManager::const_iterator it;
      tbsys::CRLockGuard guard(server_manager_rwlock_);
      it = server_manager_.find_by_ip(server);
      if (it != server_manager_.end())
      {
        ret = it - server_manager_.begin();
      }
      return ret;
    }
    int ObRootServer2::report_tablets(const ObServer& server, const ObTabletReportInfoList& tablets, const int64_t frozen_mem_version)
    {
      int return_code = OB_SUCCESS;
      int server_index = get_server_index(server);
      if (server_index == OB_INVALID_INDEX)
      {
        TBSYS_LOG(ERROR, "can not find server's info");
        return_code = OB_ERROR;
      }
      else
      {
        char t_server[OB_IP_STR_BUFF];
        server.to_string(t_server, OB_IP_STR_BUFF);
        TBSYS_LOG_US(INFO, "report tablets, server=%d ip=%s count=%ld version=%ld", 
            server_index, t_server, tablets.tablet_list_.get_array_index());
        if (is_master())
        {
          log_worker_->report_tablets(server, tablets, frozen_mem_version);
        }
        return_code = got_reported(tablets, server_index, frozen_mem_version);
        TBSYS_LOG_US(INFO, "got_reported over");
      }
      return return_code;
    }
    /*
     * 收到汇报消息后调用
     */
    int ObRootServer2::got_reported(const ObTabletReportInfoList& tablets, const int server_index, const int64_t frozen_mem_version)
    {
      int64_t index = tablets.tablet_list_.get_array_index();
      ObTabletReportInfo* p_table_info = NULL;
      int ret = OB_SUCCESS;
      tbsys::CThreadGuard guard(&root_table_build_mutex_);
      if (NULL != root_table_for_build_ )
      {
        TBSYS_LOG(INFO, "will add tablet info to root_table_for_build");
        for(int64_t i = 0; i < index; ++i)
        {
          p_table_info = tablets.tablet_list_.at(i);
          if (p_table_info != NULL)
          {
            ret = got_reported_for_build(p_table_info->tablet_info_, server_index, 
                p_table_info->tablet_location_.tablet_version_);
            if (ret != OB_SUCCESS)
            {
              TBSYS_LOG(WARN, "report_tablets error code is %d", ret);
              break;
            }
          }
        }
      }
      else
      {
        TBSYS_LOG(INFO, "will add tablet info to root_table_for_query");
        got_reported_for_query_table(tablets, server_index, frozen_mem_version);
      }
      return ret;
    }
    /*
     * 处理汇报消息, 直接写到当前的root table中
     * 如果发现汇报消息中有对当前root table的tablet的分裂或者合并
     * 要调用采用写拷贝机制的处理函数
     */
    int ObRootServer2::got_reported_for_query_table(const ObTabletReportInfoList& tablets, 
        const int32_t server_index, const int64_t frozen_mem_version)
    {
      UNUSED(frozen_mem_version);
      int ret = OB_SUCCESS;
      int64_t have_done_index = 0;
      bool need_split = false;
      bool need_add = false;

      ObServerStatus* new_server_status = server_manager_.get_server_status(server_index);
      if (new_server_status == NULL)
      {
        TBSYS_LOG(ERROR, "can not find server");
      }
      else
      {
        ObTabletReportInfo* p_table_info = NULL;
        ObRootTable2::const_iterator first;
        ObRootTable2::const_iterator last;
        int64_t index = tablets.tablet_list_.get_array_index();
        int find_ret = OB_SUCCESS;
        common::ObTabletInfo* tablet_info = NULL;
        int range_pos_type = ObRootTable2::POS_TYPE_ERROR; 

        tbsys::CRLockGuard guard(root_table_rwlock_);
        for(have_done_index = 0; have_done_index < index; ++have_done_index)
        {
          p_table_info = tablets.tablet_list_.at(have_done_index);
          if (p_table_info != NULL)
          {
            //TODO(maoqi) check the table of this tablet is exist in schema
            //if (NULL == schema_manager_->get_table_schema(p_table_info->tablet_info_.range_.table_id_))
            //{
            //  continue;
            //}

            tablet_info = NULL;
            find_ret = root_table_for_query_->find_range(p_table_info->tablet_info_.range_, first, last);
            TBSYS_LOG(DEBUG, "root_table_for_query_->find_range ret = %d", find_ret);
            if (OB_SUCCESS == find_ret)
            {
              tablet_info = root_table_for_query_->get_tablet_info(first);
            }
            range_pos_type = ObRootTable2::POS_TYPE_ERROR;
            if (NULL != tablet_info) 
            {
              range_pos_type = root_table_for_query_->get_range_pos_type(p_table_info->tablet_info_.range_, first, last);
            }
            TBSYS_LOG(DEBUG, " range_pos_type = %d", range_pos_type);
            if (range_pos_type == ObRootTable2::POS_TYPE_SPLIT_RANGE)
            {
              need_split = true;  //will create a new table to deal with the left
              break;
            }
            //else if (range_pos_type == ObRootTable2::POS_TYPE_ADD_RANGE)
            //{
            //  need_add = true;
            //  break;
            //}
            if (NULL != tablet_info && 
                (range_pos_type == ObRootTable2::POS_TYPE_SAME_RANGE || range_pos_type == ObRootTable2::POS_TYPE_MERGE_RANGE)
               )
            {
              if (OB_SUCCESS != write_new_info_to_root_table(p_table_info->tablet_info_,
                    p_table_info->tablet_location_.tablet_version_, server_index, first, last, root_table_for_query_))
              {
                TBSYS_LOG(ERROR, "write new tablet error");
                static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
                p_table_info->tablet_info_.range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
                TBSYS_LOG(INFO, "%s", row_key_dump_buff);
                //p_table_info->tablet_info_.range_.hex_dump(TBSYS_LOG_LEVEL_ERROR);
              }
            }
            else
            {
              TBSYS_LOG(INFO, "can not found range ignore this");
              static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
              p_table_info->tablet_info_.range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
              TBSYS_LOG(INFO, "%s", row_key_dump_buff);
              //p_table_info->tablet_info_.range_.hex_dump(TBSYS_LOG_LEVEL_INFO);
            }

          }
        }
      } //end else, release lock
      if (need_split || need_add) 
      {
        ret = got_reported_with_copy(tablets, server_index, have_done_index);
      }
      return ret;
    }
    /*
     * 写拷贝机制的,处理汇报消息
     */
    int ObRootServer2::got_reported_with_copy(const ObTabletReportInfoList& tablets, 
        const int32_t server_index, const int64_t have_done_index)
    {
      int ret = OB_SUCCESS;
      common::ObTabletInfo* tablet_info = NULL;
      ObTabletReportInfo* p_table_info = NULL;
      ObServerStatus* new_server_status = server_manager_.get_server_status(server_index);
      ObRootTable2::const_iterator first;
      ObRootTable2::const_iterator last;
      TBSYS_LOG(DEBUG, "root table write on copy");
      if (new_server_status == NULL)
      {
        TBSYS_LOG(ERROR, "can not find server");
        ret = OB_ERROR;
      }
      else
      {
        ObRootTable2* root_table_for_split = new ObRootTable2(NULL);
        if (root_table_for_split == NULL) 
        {
          TBSYS_LOG(ERROR, "new ObRootTable2 error");
          ret = OB_ERROR;
        }
        else
        {
          *root_table_for_split = *root_table_for_query_;
          int range_pos_type = ObRootTable2::POS_TYPE_ERROR; 
          for (int64_t index = have_done_index; OB_SUCCESS == ret && index < tablets.tablet_list_.get_array_index(); index++)
          {
            p_table_info = tablets.tablet_list_.at(index);
            if (p_table_info != NULL)
            {
              tablet_info = NULL;
              int find_ret = root_table_for_split->find_range(p_table_info->tablet_info_.range_, first, last);
              if (OB_SUCCESS == find_ret)
              {
                tablet_info = root_table_for_split->get_tablet_info(first);
              }
              range_pos_type = ObRootTable2::POS_TYPE_ERROR;
              if (NULL != tablet_info) 
              {
                range_pos_type = root_table_for_split->get_range_pos_type(p_table_info->tablet_info_.range_, first, last);
              }
              if (NULL != tablet_info  )
              {
                if (range_pos_type == ObRootTable2::POS_TYPE_SAME_RANGE || range_pos_type == ObRootTable2::POS_TYPE_MERGE_RANGE)
                {
                  if (OB_SUCCESS != write_new_info_to_root_table(p_table_info->tablet_info_,
                        p_table_info->tablet_location_.tablet_version_, server_index, first, last, root_table_for_split))
                  {
                    TBSYS_LOG(ERROR, "write new tablet error");
                    static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
                    p_table_info->tablet_info_.range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
                    TBSYS_LOG(INFO, "%s", row_key_dump_buff);
                    //p_table_info->tablet_info_.range_.hex_dump(TBSYS_LOG_LEVEL_ERROR);
                  }
                }
                else if (range_pos_type == ObRootTable2::POS_TYPE_SPLIT_RANGE)
                {
                  if (ObRootTable2::get_max_tablet_version(first) >= p_table_info->tablet_location_.tablet_version_)
                  {
                    TBSYS_LOG(ERROR, "same version different range error !! version %ld", 
                        p_table_info->tablet_location_.tablet_version_);
                    static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
                    p_table_info->tablet_info_.range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
                    TBSYS_LOG(INFO, "%s", row_key_dump_buff);
                    //p_table_info->tablet_info_.range_.hex_dump(TBSYS_LOG_LEVEL_ERROR);
                  }
                  else
                  {
                    ret = root_table_for_split->split_range(p_table_info->tablet_info_, first,
                        p_table_info->tablet_location_.tablet_version_, server_index);
                    if (OB_SUCCESS != ret) 
                    {
                      TBSYS_LOG(ERROR, "split range error, ret = %d", ret);
                      static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
                      p_table_info->tablet_info_.range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
                      TBSYS_LOG(INFO, "%s", row_key_dump_buff);
                      //p_table_info->tablet_info_.range_.hex_dump(TBSYS_LOG_LEVEL_ERROR);
                    }
                  }
                }
                //else if (range_pos_type == ObRootTable2::POS_TYPE_ADD_RANGE)
                //{
                //  ret = root_table_for_split->add_range(p_table_info->tablet_info_, first,
                //      p_table_info->tablet_location_.tablet_version_, server_index);
                //}
                else
                {
                  TBSYS_LOG(INFO, "error range be ignored range_pos_type =%d",range_pos_type );
                  static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
                  p_table_info->tablet_info_.range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
                  TBSYS_LOG(INFO, "%s", row_key_dump_buff);
                  //p_table_info->tablet_info_.range_.hex_dump(TBSYS_LOG_LEVEL_INFO);
                }
              }
            }
          }
        }

        if (OB_SUCCESS == ret && root_table_for_split != NULL)
        {
          tbsys::CWLockGuard guard(root_table_rwlock_);
          delete root_table_for_query_;
          root_table_for_query_ = root_table_for_split;
          root_table_for_split = NULL;
        }
        else
        {
          if (root_table_for_split != NULL)
          {
            delete root_table_for_split;
            root_table_for_split = NULL;
          }
        }
      }
      return ret;
    }

    /*
     * 在一个tabelt的各份拷贝中, 寻找合适的备份替换掉
     */
    int ObRootServer2::write_new_info_to_root_table(
        const ObTabletInfo& tablet_info, const int64_t tablet_version, const int32_t server_index, 
        ObRootTable2::const_iterator& first, ObRootTable2::const_iterator& last, ObRootTable2 *p_root_table)
    {
      int ret = OB_SUCCESS;
      int32_t found_it_index = OB_INVALID_INDEX;
      int64_t max_tablet_version = 0;
      ObServerStatus* server_status = NULL;
      ObServerStatus* new_server_status = server_manager_.get_server_status(server_index);
      if (new_server_status == NULL)
      {
        TBSYS_LOG(ERROR, "can not find server");
        ret = OB_ERROR;
      }
      else
      {
        for (ObRootTable2::const_iterator it = first; it <= last; it++)
        {
          ObTabletInfo *p_tablet_write = p_root_table->get_tablet_info(it);
          ObTabletCrcHistoryHelper *crc_helper = p_root_table->get_crc_helper(it);
          if (crc_helper == NULL)
          {
            TBSYS_LOG(ERROR, "%s", "get src helper error should not reach this bugs!!");
            ret = OB_ERROR;
            break;
          }
          max_tablet_version = ObRootTable2::get_max_tablet_version(it);
          if (tablet_version >= max_tablet_version)
          {
            if (first != last)
            {
              TBSYS_LOG(ERROR, "we should not have merge tablet max tabelt is %ld this one is %ld",
                  max_tablet_version, tablet_version);
              ret = OB_ERROR;
              break;
            }
          }
          if (first == last)
          {
            //check crc sum
            ret = crc_helper->check_and_update(tablet_version, tablet_info.crc_sum_);
            if (ret != OB_SUCCESS)
            {
              TBSYS_LOG(ERROR, "check crc sum error crc is %lu tablet is", tablet_info.crc_sum_);
              static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
              tablet_info.range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
              TBSYS_LOG(INFO, "%s", row_key_dump_buff);
              it->dump();
              //tablet_info.range_.hex_dump(TBSYS_LOG_LEVEL_ERROR);
              break;
            }
          }
          //try to over write dead server or old server;
          found_it_index = ObRootTable2::find_suitable_pos(it, server_index, tablet_version);
          if (found_it_index == OB_INVALID_INDEX)
          {
            //find the serve who have max free disk
            for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; i++)
            {
              server_status = server_manager_.get_server_status(it->server_info_indexes_[i]);
              if (server_status != NULL && 
                  new_server_status->disk_info_.get_percent() > 0 &&
                  server_status->disk_info_.get_percent() > new_server_status->disk_info_.get_percent())
              {
                found_it_index = i;
              }
            }
          }
          if (found_it_index != OB_INVALID_INDEX)
          {
            TBSYS_LOG(DEBUG, "write a tablet to root_table found_it_index = %d server_index =%d tablet_version = %ld",
                found_it_index, server_index, tablet_version);

            //tablet_info.range_.hex_dump(TBSYS_LOG_LEVEL_DEBUG);
            static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
            tablet_info.range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
            TBSYS_LOG(DEBUG, "%s", row_key_dump_buff);
            //over write 
            atomic_exchange((uint32_t*) &(it->server_info_indexes_[found_it_index]), server_index);
            atomic_exchange((uint64_t*) &(it->tablet_version_[found_it_index]), tablet_version);
            if (p_tablet_write != NULL)
            {
              atomic_exchange((uint64_t*) &(p_tablet_write->row_count_), tablet_info.row_count_);
              atomic_exchange((uint64_t*) &(p_tablet_write->occupy_size_), tablet_info.occupy_size_);
            }
          }
        }
      }
      return ret;
    }


    /** 
     * @brief check the version of tablets 
     * 
     * @return 
     */
    bool ObRootServer2::all_tablet_is_the_last_frozen_version() const
    {
      ObRootTable2::iterator it;
      bool ret = true;
      int32_t new_copy = 0;
      
      {
        tbsys::CRLockGuard guard(root_table_rwlock_);
        for (it = root_table_for_query_->begin(); it < root_table_for_query_->sorted_end(); ++it)
        {
          new_copy = 0;

          for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; i++)
          {
            if (OB_INVALID_INDEX == it->server_info_indexes_[i]) //the server is down
            {
            }
            else if (it->tablet_version_[i] >= last_frozen_mem_version_)
            {
              ++new_copy;
            }
          }

          if (new_copy < safe_copy_count_in_init_)
          {
            ret = false;
            break;
          }
        } //end all tablet
      } //lock 

      return ret;
    }


    /*
     * 系统初始化的时候, 处理汇报消息, 
     * 信息放到root table for build 结构中
     */
    int ObRootServer2::got_reported_for_build(const ObTabletInfo& tablet, const int32_t server_index, const int64_t version)
    {
      int ret = OB_SUCCESS;
      if (root_table_for_build_ == NULL) 
      {
        TBSYS_LOG(ERROR, "no root_table_for_build_ ignore this");
        ret = OB_ERROR;
      }
      else
      {
        //tablet.range_.hex_dump();
        static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
        tablet.range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
        TBSYS_LOG(DEBUG, "add a tablet, server=%d crc=%lu range=%s", server_index, tablet.crc_sum_, row_key_dump_buff);
        //TODO(maoqi) check the table of this tablet is exist in schema
        //if (schema_manager_->get_table_schema(tablet.range_.table_id_) != NULL)
        //{
        ret = root_table_for_build_->add(tablet, server_index, version);
        //}
      }
      return ret;
    }

// the array size of server_index should be larger than expected_num
void ObRootServer2::get_available_servers_for_new_table(int* server_index, int32_t expected_num, int32_t &results_num)
    {
      //tbsys::CThreadGuard guard(&server_manager_mutex_);
      results_num = 0;
      ObChunkServerManager::iterator it = server_manager_.begin();
      for (; it != server_manager_.end() && results_num < expected_num; ++it)
      {
        if (it->status_ != ObServerStatus::STATUS_DEAD)
        {
          server_index[results_num] = it - server_manager_.begin();
          results_num++;
        }
      }
    }

    void ObRootServer2::use_new_schema()
    {
      int ret = OB_SUCCESS;
      int64_t schema_timestamp = tbsys::CTimeUtil::getTime();
      if (!is_master())
      {
        TBSYS_LOG(WARN, "cannot switch schema as a slave");
      }
      else if (OB_SUCCESS != (ret = switch_schema(schema_timestamp)))
      {
        TBSYS_LOG(ERROR, "failed to load and switch new schema, err=%d", ret);
      }
      else
      {
        ret = log_worker_->sync_schema(schema_timestamp);
        if (OB_SUCCESS == ret)
        {
          TBSYS_LOG(INFO, "sync schema succ, ts=%ld", schema_timestamp);
        }
        else
        {
          TBSYS_LOG(ERROR, "sync schema error, err=%d", ret);
        }
        this->create_new_table();
        ret = worker_->up_switch_schema(update_server_status_.server_, schema_manager_);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "up_switch_schema error, ret=%d schema_manager_=%p", ret, schema_manager_);
        }
      }
    }

    void ObRootServer2::create_new_table()
    {
      if (is_master())
      {
        create_new_table(schema_manager_);
      }
      else
      {
        new_table_created_ = false;
        build_sync_flag_ = BUILD_SYNC_FLAG_CAN_ACCEPT_NEW_TABLE;
        while (!is_master() && !new_table_created_)
        {
          usleep(SLAVE_SLEEP_TIME);
        }
        //if (!new_table_created_)
        //{
        //  create_new_table(schema_manager_);
        //}
        new_table_created_ = false;
      }
    }
int ObRootServer2::slave_create_new_table(const common::ObTabletInfo& tablet, const int32_t* t_server_index, const int32_t replicas_num, const int64_t mem_version)
    {
      int ret = OB_SUCCESS;
      ObRootTable2* root_table_for_create = NULL;
      root_table_for_create = new(std::nothrow) ObRootTable2(NULL);
      if (root_table_for_create == NULL) 
      {
        TBSYS_LOG(ERROR, "new ObRootTable2 error");
        ret = OB_ERROR;
      }
      if (NULL != root_table_for_create)
      {
        *root_table_for_create = *root_table_for_query_;
      }
      if (OB_SUCCESS == ret)
      {

        root_table_for_create->create_table(tablet, t_server_index, replicas_num, mem_version);
        tbsys::CWLockGuard guard(root_table_rwlock_);
        delete root_table_for_query_;
        root_table_for_query_ = root_table_for_create;
        root_table_for_create = NULL;
      }
      if (root_table_for_create != NULL)
      {
        delete root_table_for_create;
      }
      return ret;
    }

    void ObRootServer2::create_new_table_in_init(common::ObSchemaManagerV2* schema,ObRootTable2* root_table_tmp)
    {
      int32_t server_index[OB_SAFE_COPY_COUNT];
      ObTabletInfo tablet;
      tablet.range_.border_flag_.set_inclusive_end();
      tablet.range_.border_flag_.set_min_value();
      tablet.range_.border_flag_.set_max_value();
      ObServerStatus* server_status = NULL;
      ObServer server;
      int created_count = 0;
      int32_t t_server_index[OB_SAFE_COPY_COUNT];
      ObRootTable2* root_table_for_create = root_table_tmp;
      int ret = OB_SUCCESS;
      
      if (root_table_for_create != NULL)
      {
        int64_t mem_froze_version = 0;
        int retry_time = 0;
        while (OB_SUCCESS != (ret = worker_->ups_get_last_frozen_memtable_version(update_server_status_.server_, mem_froze_version)))
        {
          retry_time++;
          if (retry_time >= 3) break;
          sleep(WAIT_SECONDS * retry_time);
        }

        if (OB_SUCCESS == ret)
        {
          for (const ObTableSchema* it=schema->table_begin(); it != schema->table_end(); ++it)
          {
            if (it->get_table_id() != OB_INVALID_ID && !it->is_pure_update_table())
            {
              if(!root_table_for_create->table_is_exist(it->get_table_id()))
              {
                tablet.range_.table_id_ = it->get_table_id();
                for (int i = 0; i < OB_SAFE_COPY_COUNT; ++i)
                {
                  server_index[i] = OB_INVALID_INDEX;
                  t_server_index[i] = OB_INVALID_INDEX;
                }
                int32_t results_num = -1;
                get_available_servers_for_new_table(server_index, tablet_replicas_num_, results_num);
                created_count = 0;
                for (int i = 0; i < results_num; ++i)
                {
                  if (server_index[i] != OB_INVALID_INDEX) 
                  {
                    server_status = server_manager_.get_server_status(server_index[i]);
                    if (server_status != NULL)
                    {
                      server = server_status->server_;
                      server.set_port(server_status->port_cs_);
                      if (OB_SUCCESS == worker_->cs_create_tablet(server, tablet.range_, mem_froze_version))
                      {
                        t_server_index[created_count] = server_index[i];
                        created_count++;
                        TBSYS_LOG(INFO, "create tablet replica, table_id=%lu server=%d version=%ld", 
                                  tablet.range_.table_id_, server_index[i], mem_froze_version);
                      }
                    }
                    else
                    {
                      server_index[i] = OB_INVALID_INDEX;
                    }
                  }
                }
                if (created_count > 0)
                {
                  ret = root_table_for_create->create_table(tablet, t_server_index, created_count, mem_froze_version);
                }
              }
            }
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "get ups_get_last_frozen_memtable_version error %d, ignore create table", ret);
        }
      }
    }

    void ObRootServer2::create_new_table(common::ObSchemaManagerV2* schema)
    {
      int32_t server_index[OB_SAFE_COPY_COUNT];
      ObTabletInfo tablet;
      tablet.range_.border_flag_.set_inclusive_end();
      tablet.range_.border_flag_.set_min_value();
      tablet.range_.border_flag_.set_max_value();
      ObServerStatus* server_status = NULL;
      ObServer server;
      int created_count = 0;
      int32_t t_server_index[OB_SAFE_COPY_COUNT];
      ObRootTable2* root_table_for_create = NULL;
      int ret = OB_SUCCESS;
      tbsys::CThreadGuard guard(&root_table_build_mutex_);
      {
        tbsys::CRLockGuard guard(root_table_rwlock_);
        if (root_table_for_query_ != NULL)
        {
          for (const ObTableSchema* it=schema->table_begin(); it != schema->table_end(); ++it)
          {
            if (it->get_table_id() != OB_INVALID_ID && !it->is_pure_update_table())
            {
              if(!root_table_for_query_->table_is_exist(it->get_table_id()))
              {
                root_table_for_create = new(std::nothrow) ObRootTable2(NULL);
                if (root_table_for_create == NULL) 
                {
                  ret = OB_ERROR;
                  TBSYS_LOG(ERROR, "new ObRootTable2 error");
                }
                break;
              }
            }
          }
          if (NULL != root_table_for_create)
          {
            *root_table_for_create = *root_table_for_query_;
          }
        }
      }
      if (root_table_for_create != NULL)
      {
        int64_t mem_froze_version = 0;
        int retry_time = 0;
        while (OB_SUCCESS != (ret = worker_->ups_get_last_frozen_memtable_version(update_server_status_.server_, mem_froze_version)))
        {
          retry_time++;
          if (retry_time >= 3) break;
          sleep(WAIT_SECONDS);
        }
        if (OB_SUCCESS == ret)
        {
          for (const ObTableSchema* it=schema->table_begin(); it != schema->table_end(); ++it)
          {
            if (it->get_table_id() != OB_INVALID_ID && !it->is_pure_update_table())
            {
              if(!root_table_for_create->table_is_exist(it->get_table_id()))
              {
                tablet.range_.table_id_ = it->get_table_id();
                for (int i = 0; i < OB_SAFE_COPY_COUNT; ++i)
                {
                  server_index[i] = OB_INVALID_INDEX;
                  t_server_index[i] = OB_INVALID_INDEX;
                }
                int32_t results_num = -1;
                get_available_servers_for_new_table(server_index, tablet_replicas_num_, results_num);
                created_count = 0;
                for (int i = 0; i < results_num; ++i)
                {
                  if (server_index[i] != OB_INVALID_INDEX) 
                  {
                    server_status = server_manager_.get_server_status(server_index[i]);
                    if (server_status != NULL)
                    {
                      server = server_status->server_;
                      server.set_port(server_status->port_cs_);
                      if (OB_SUCCESS == worker_->cs_create_tablet(server, tablet.range_, mem_froze_version))
                      {
                        t_server_index[created_count] = server_index[i];
                        created_count++;
                        TBSYS_LOG(INFO, "create tablet replica, table_id=%lu server=%d version=%ld", 
                                  tablet.range_.table_id_, server_index[i], mem_froze_version);
                      }
                    }
                    else
                    {
                      server_index[i] = OB_INVALID_INDEX;
                    }
                  }
                } // end for
                if (created_count > 0)
                {
                  ret = root_table_for_create->create_table(tablet, t_server_index, created_count, mem_froze_version);
                  if (is_master())
                  {
                    log_worker_->add_new_tablet(created_count, tablet, t_server_index, mem_froze_version);
                  }
                }
              }
              else
              {
                TBSYS_LOG(WARN, "table already exist, table_id=%d", it->get_table_id());
              }
            }
          } // end for
          tbsys::CWLockGuard guard(root_table_rwlock_);
          delete root_table_for_query_;
          root_table_for_query_ = root_table_for_create;
          root_table_for_create = NULL;
        }
        else
        {
          TBSYS_LOG(ERROR, "get ups_get_last_frozen_memtable_version error %d, ignore create table", ret);
        }
      }
      if (is_master())
      {
        log_worker_->create_table_done();
      }
    }

    common::ObServer ObRootServer2::get_update_server_info() const
    {
      return update_server_status_.server_;
    }

    int32_t ObRootServer2::get_update_server_inner_port() const
    {
      return ups_inner_port_;
    }

    int64_t ObRootServer2::get_merge_delay_interval() const
    {
      return cs_merge_command_interval_mseconds_ * server_manager_.get_array_length();
    }

    uint64_t ObRootServer2::get_table_info(const common::ObString& table_name, int32_t& max_row_key_length) const
    {
      uint64_t table_id = OB_INVALID_ID;
      max_row_key_length = 0;
      tbsys::CRLockGuard guard(schema_manager_rwlock_);
      if (schema_manager_ != NULL)
      {
        const ObTableSchema* table_schema = schema_manager_->get_table_schema(table_name);
        if (table_schema != NULL)
        {
          table_id = table_schema->get_table_id();
          max_row_key_length = table_schema->get_rowkey_max_length();
        }
      }
      return table_id;
    }
    int ObRootServer2::get_table_info(const uint64_t table_id, common::ObString& table_name, int32_t& max_row_key_length) const
    {
      int ret = OB_ERROR;
      max_row_key_length = 0;
      tbsys::CRLockGuard guard(schema_manager_rwlock_);
      if (schema_manager_ != NULL)
      {
        if (table_id > 0 && table_id != OB_INVALID_ID)
        {
          const ObTableSchema* table_schema = schema_manager_->get_table_schema(table_id);
          if (table_schema != NULL)
          {
            max_row_key_length = table_schema->get_rowkey_max_length();
            int table_name_len = strlen(table_schema->get_table_name());
            if (table_name_len == table_name.write(table_schema->get_table_name(), table_name_len))
            {
              ret = OB_SUCCESS;
            }
          }
        }
      }
      return ret;
    }

    int ObRootServer2::do_check_point(const uint64_t ckpt_id)
    {
      int ret = OB_SUCCESS;

      const char* log_dir = worker_->get_log_manager()->get_log_dir_path();
      char filename[OB_MAX_FILE_NAME_LENGTH];

      int err = 0;
      err = snprintf(filename, OB_MAX_FILE_NAME_LENGTH, "%s/%lu.%s", log_dir, ckpt_id, ROOT_TABLE_EXT);
      if (err < 0 || err >= OB_MAX_FILE_NAME_LENGTH)
      {
        TBSYS_LOG(ERROR, "generate root table file name [%s] failed, error: %s", filename, strerror(errno));
        ret = OB_ERROR;
      }

      if (ret == OB_SUCCESS)
      {
        ret = root_table_for_query_->write_to_file(filename);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "write root table to file [%s] failed, err=%d", filename, ret);
        }
      }

      err = snprintf(filename, OB_MAX_FILE_NAME_LENGTH, "%s/%lu.%s", log_dir, ckpt_id, CHUNKSERVER_LIST_EXT);
      if (err < 0 || err >= OB_MAX_FILE_NAME_LENGTH)
      {
        TBSYS_LOG(ERROR, "generate chunk server list file name [%s] failed, error: %s", filename, strerror(errno));
        ret = OB_ERROR;
      }

      if (ret == OB_SUCCESS)
      {
        ret = server_manager_.write_to_file(filename);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "write chunkserver list to file [%s] failed, err=%d", filename, ret);
        }
      }

      return ret;
    }

    int ObRootServer2::recover_from_check_point(const int server_status, const uint64_t ckpt_id)
    {
      int ret = OB_SUCCESS;

      server_status_ = server_status;
      TBSYS_LOG(INFO, "server status recover from check point is %d", server_status_);

      const char* log_dir = worker_->get_log_manager()->get_log_dir_path();
      char filename[OB_MAX_FILE_NAME_LENGTH];

      int err = 0;
      err = snprintf(filename, OB_MAX_FILE_NAME_LENGTH, "%s/%lu.%s", log_dir, ckpt_id, ROOT_TABLE_EXT);
      if (err < 0 || err >= OB_MAX_FILE_NAME_LENGTH)
      {
        TBSYS_LOG(ERROR, "generate root table file name [%s] failed, error: %s", filename, strerror(errno));
        ret = OB_ERROR;
      }

      if (ret == OB_SUCCESS)
      {
        ret = root_table_for_query_->read_from_file(filename);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "recover root table from file [%s] failed, err=%d", filename, ret);
        }
      }

      err = snprintf(filename, OB_MAX_FILE_NAME_LENGTH, "%s/%lu.%s", log_dir, ckpt_id, CHUNKSERVER_LIST_EXT);
      if (err < 0 || err >= OB_MAX_FILE_NAME_LENGTH)
      {
        TBSYS_LOG(ERROR, "generate chunk server list file name [%s] failed, error: %s", filename, strerror(errno));
        ret = OB_ERROR;
      }

      if (ret == OB_SUCCESS)
      {
        ret = server_manager_.read_from_file(filename);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "recover chunkserver list from file [%s] failed, err=%d", filename, ret);
        }
      }

      TBSYS_LOG(INFO, "recover finished with ret: %d, ckpt_id: %d", ret, ckpt_id);

      return ret;
    }

    int ObRootServer2::report_frozen_memtable(const int64_t frozen_version, bool did_replay)
    {
      int ret = OB_SUCCESS;

      if ( frozen_version < 0 || frozen_version <= last_frozen_mem_version_)
      {
        TBSYS_LOG(WARN, "invalid froze_version, version=%ld last_frozen_version=%ld", 
                  frozen_version, last_frozen_mem_version_);
        ret = OB_ERROR;
      }
      else
      {
        if (!did_replay && is_master() && (!all_tablet_is_the_last_frozen_version()) )
        {
          TBSYS_LOG(WARN,"merge is too slow, last_version=%ld curr_version=%ld", 
                    last_frozen_mem_version_, frozen_version); //just warn
        }
      }

      if (OB_SUCCESS == ret)
      {
        pre_frozen_mem_version_ = last_frozen_mem_version_;
        last_frozen_mem_version_ = frozen_version;
        last_frozen_time_ = did_replay ? 0 : tbsys::CTimeUtil::getMonotonicTime();
        TBSYS_LOG(INFO, "frozen_version=%ld last_frozen_time=%ld did_replay=%d", 
                  last_frozen_mem_version_, last_frozen_time_, did_replay);
      }

      if (OB_SUCCESS == ret)
      {
        if (is_master() && !did_replay)
        {
          log_worker_->sync_us_frozen_version(last_frozen_mem_version_);
        }
      }
      return ret;
    }

    int ObRootServer2::get_server_status() const
    {
      return server_status_;
    }
    int64_t ObRootServer2::get_time_stamp_changing() const 
    {
      return time_stamp_changing_;
    }
    int64_t ObRootServer2::get_lease() const
    {
      return lease_duration_;
    }

void ObRootServer2::wait_init_finished()
{
  static const unsigned long sleep_us = 200*1000;
  while(true)
  {
    {
      tbsys::CThreadGuard guard(&(status_mutex_));
      if (STATUS_INIT != server_status_)
      {
        break;
      }
    }
    usleep(sleep_us);           // sleep 200ms
  }
  TBSYS_LOG(INFO, "rootserver2 init finished");
}

int ObRootServer2::make_checkpointing()
{
  tbsys::CRLockGuard rt_guard(root_table_rwlock_);
  tbsys::CRLockGuard cs_guard(server_manager_rwlock_);
  tbsys::CThreadGuard st_guard(&status_mutex_);
  tbsys::CThreadGuard log_guard(worker_->get_log_manager()->get_log_sync_mutex());
  int ret = worker_->get_log_manager()->do_check_point();
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "failed to make checkpointing, err=%d", ret);
  }
  else
  {
    TBSYS_LOG(INFO, "made checkpointing");
  }
  return ret;
}

    ObRootServer2::rootTableModifier::rootTableModifier(ObRootServer2* root_server):root_server_(root_server)
    {
    }

    void ObRootServer2::rootTableModifier::run(tbsys::CThread *thread, void *arg)
    {
      UNUSED(thread);
      UNUSED(arg);
      TBSYS_LOG(INFO, "root table modifier thread start");
      bool init_process = false;
      {
        tbsys::CThreadGuard guard(&(root_server_->status_mutex_));
        if (root_server_->server_status_ == STATUS_INIT)
        {
          init_process  = true;  //execute init process only the first time the system start up;
        }
      }

      //for now report, switch, and balance are all in this thread, maybe we will use multithread later
      if (init_process)
      {
        if ((OB_SUCCESS != root_server_->init_root_table_by_report()) || 
            (root_server_->build_sync_flag_ != BUILD_SYNC_INIT_OK))
        {
          TBSYS_LOG(ERROR, "system init error");
          exit(0);
        }
      }
      TBSYS_LOG(INFO, "start service now");

      while (!_stop)
      {
        int64_t now = tbsys::CTimeUtil::getMonotonicTime();
        if (root_server_->is_master() && (root_server_->last_frozen_time_ > 0) )
        {
          if (((root_server_->max_merge_duration_ + root_server_->last_frozen_time_) < now) &&
              !root_server_->all_tablet_is_the_last_frozen_version() )
          {
            TBSYS_LOG(ERROR,"merge is too slow,start at:%ld,now:%ld,max_merge_duration_:%ld",root_server_->last_frozen_time_,
                now,root_server_->max_merge_duration_);
          }
          else if (root_server_->all_tablet_is_the_last_frozen_version())
          {
            TBSYS_LOG(INFO,"build new root table ok"); //for qa
            root_server_->last_frozen_time_ = 0;
            // checkpointing after done merge
            root_server_->make_checkpointing();
          }
        }
        sleep(1);
      }
      TBSYS_LOG(INFO, "tableModifer thread exit");
    }

    ObRootServer2::balanceWorker::balanceWorker(ObRootServer2* root_server):root_server_(root_server)
    {
    }
    void ObRootServer2::balanceWorker::run(tbsys::CThread *thread, void *arg)
    {
      UNUSED(thread);
      UNUSED(arg);
      //TODO if this is not the first start up we will do sleep
      for (int i = 0; i < root_server_->migrate_wait_seconds_ && !_stop; i++)
      {
        sleep(1);
      }
      TBSYS_LOG(INFO, "balance worker thread start");
      while (!_stop)
      {
        if (root_server_->is_master())
        {
          //TBSYS_LOG(DEBUG, "this one is master");
          bool balance_it = false;
          {
            tbsys::CThreadGuard guard(&(root_server_->status_mutex_));
            if (root_server_->server_status_ == STATUS_SLEEP)
            {
              balance_it = true;
              root_server_->log_worker_->begin_balance();
              root_server_->server_status_ = STATUS_BALANCING;
            }
          }
          if (balance_it)
          {
            root_server_->do_balance();
            tbsys::CThreadGuard guard(&(root_server_->status_mutex_));
            if (root_server_->server_status_ == STATUS_BALANCING || STATUS_INTERRUPT_BALANCING == root_server_->server_status_)
            {
              root_server_->log_worker_->balance_done();
              root_server_->server_status_ = STATUS_SLEEP;
            }
          }
        }
        //for (int i = 0; i < 5 && !_stop; i++)
        {
          sleep(1);
        }
      }
      TBSYS_LOG(INFO, "balance worker exit");
    }
    ObRootServer2::heartbeatChecker::heartbeatChecker(ObRootServer2* root_server):root_server_(root_server)
    {
    }

    bool ObRootServer2::is_master() const
    {
      ObRoleMgr::Role role = worker_->get_role_manager()->get_role();
      ObRoleMgr::State state = worker_->get_role_manager()->get_state();
      return (role == ObRoleMgr::MASTER) && (state == ObRoleMgr::ACTIVE);
    }

    int ObRootServer2::receive_hb(const common::ObServer& server,ObRole role)
    {
      int64_t  now = tbsys::CTimeUtil::getMonotonicTime();
      return server_manager_.receive_hb(server,now,role == OB_MERGESERVER ? true : false);
    }

    void ObRootServer2::heartbeatChecker::run(tbsys::CThread *thread, void *arg)
    {
      UNUSED(thread);
      UNUSED(arg);
      //ObServer* will_heart_beat = new ObServer[ObChunkServerManager::MAX_SERVER_COUNT];
      //ObArrayHelper<ObServer> server_need_hb;
      ObServer tmp_server;
      int64_t now = 0;
      int64_t preview_rotate_time = 0;
      bool need_report = false;
      bool need_balance = false;
      TBSYS_LOG(INFO, "heart beat checker thread start");
      while (!_stop)
      {
        need_report = false;
        need_balance = false;
        now = tbsys::CTimeUtil::getTime();
        if ((now > preview_rotate_time + 10 *1000 * 1000) &&
            ((now/(1000 * 1000)  - timezone) % (24 * 3600)  == 0))
        {
          preview_rotate_time = now;
          TBSYS_LOG(INFO, "rotateLog");
          TBSYS_LOGGER.rotateLog(NULL, NULL);
        }
        int64_t monotonic_now = tbsys::CTimeUtil::getMonotonicTime();
        //server_need_hb.init(will_heart_beat, ObChunkServerManager::MAX_SERVER_COUNT);
        if (root_server_->is_master())
        {
          ObChunkServerManager::iterator it = root_server_->server_manager_.begin();
          char server_str[OB_IP_STR_BUFF];
          for (; it != root_server_->server_manager_.end(); ++it)
          {
            if (it->status_ != ObServerStatus::STATUS_DEAD)
            {
              if (it->is_alive(monotonic_now, root_server_->lease_duration_))
              {
                if (monotonic_now - it->last_hb_time_ >= (root_server_->lease_duration_ / 2 + it->hb_retry_times_ * HB_RETRY_FACTOR)) 
                {
                  it->hb_retry_times_ ++;
                  tmp_server = it->server_;
                  tmp_server.set_port(it->port_cs_);
                  //need hb
                  //server_need_hb.push_back(tmp_server);
                  if (TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_DEBUG)
                  {
                    tmp_server.to_string(server_str, OB_IP_STR_BUFF);
                    //TBSYS_LOG(DEBUG, "send hb to %s", server_str);
                  }
                  if (root_server_->worker_->hb_to_cs(tmp_server, root_server_->lease_duration_,
                                                      root_server_->last_frozen_mem_version_) == OB_SUCCESS)
                  {
                    //do nothing
                  }
                }
              }
              else
              {
                TBSYS_LOG(INFO,"server is down,monotonic_now:%ld,lease_duration:%ld",monotonic_now,root_server_->lease_duration_);
                root_server_->server_manager_.set_server_down(it);
                root_server_->log_worker_->server_is_down(it->server_, now);

                tbsys::CThreadGuard mutex_guard(&(root_server_->root_table_build_mutex_)); //this for only one thread modify root_table
                tbsys::CWLockGuard guard(root_server_->root_table_rwlock_);
                if (root_server_->root_table_for_query_ != NULL) 
                {
                  root_server_->root_table_for_query_->server_off_line(it - root_server_->server_manager_.begin(), now);
                }
                else
                {
                  TBSYS_LOG(ERROR, "root_table_for_query_ = NULL, server_index=%d", it - root_server_->server_manager_.begin());
                }
              }
            }

            if (it->ms_status_ != ObServerStatus::STATUS_DEAD && it->port_ms_ != 0)
            {
              if (it->is_ms_alive(monotonic_now,root_server_->lease_duration_) )
              {
                if (monotonic_now - it->last_hb_time_ms_ > 
                   (root_server_->lease_duration_ / 2)) 
                {
                    //hb to ms
                    tmp_server = it->server_;
                    tmp_server.set_port(it->port_ms_);
                    root_server_->worker_->hb_to_ms(tmp_server, root_server_->lease_duration_,
                                                    root_server_->get_schema_version());
                }
              }
              else 
              {
                root_server_->server_manager_.set_server_down_ms(it);
              }
            }
          } //end for
        } //end if master
        //async heart beat
        usleep(10000);
      }
      TBSYS_LOG(INFO, "heart beat checker exit");
      //delete [] will_heart_beat;
    }

  }
}


