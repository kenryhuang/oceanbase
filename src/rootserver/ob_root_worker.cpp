/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_root_worker.cpp for ...
 *
 * Authors:
 *   daoan <daoan@taobao.com>
 *
 */

#include <tbsys.h>

#include "common/ob_define.h"
#include "common/ob_server.h"
#include "common/ob_packet.h"
#include "common/ob_result.h"
#include "common/ob_schema.h"
#include "common/ob_tablet_info.h"
#include "common/ob_read_common_data.h"
#include "common/ob_scanner.h"
#include "common/utility.h"
#include "rootserver/ob_root_worker.h"
//#define PRESS_TEST

namespace 
{
  const int WRITE_THREAD_FLAG = 1;
  const int LOG_THREAD_FLAG = 2;
  const int DEFAULT_TASK_READ_QUEUE_SIZE = 500;
  const int DEFAULT_TASK_WRITE_QUEUE_SIZE = 50;
  const int DEFAULT_TASK_LOG_QUEUE_SIZE = 50;
  const int DEFAULT_VIP_CHECK_PERIOD_US = 500 * 1000; // 500ms
  const int64_t DEFAULT_LOG_SYNC_LIMIT_KB = 1024 * 40;
  const int64_t DEFAULT_LOG_SYNC_TIMEOUT_US = 500 * 1000;
  const int64_t DEFAULT_REPLAY_WAIT_TIME_US = 100L * 1000; // 100ms
  const int64_t DEFAULT_REGISTER_TIMES = 10;
  const int64_t DEFAULT_REGISTER_TIMEOUT_US = 3L * 1000 * 1000; // 3s
  const int64_t DEFAULT_LEASE_ON = 1; // default to on
  const int64_t DEFAULT_LEASE_INTERVAL_US = 8 * 1000 * 1000; // 5 seconds
  const int64_t DEFAULT_LEASE_RESERVED_TIME_US = 5 * 1000 * 1000;
  const int64_t LEASE_ON = 1;
  const int64_t DEFAULT_SLAVE_QUIT_TIMEOUT = 1000 * 1000; // 1s

  const char* STR_ROOT_SECTION = "root_server";
  const char* STR_THREAD_COUNT = "thread_count";
  const char* STR_READ_QUEUE_SIZE = "read_queue_size";
  const char* STR_WRITE_QUEUE_SIZE = "write_queue_size";
  const char* STR_LOG_QUEUE_SIZE = "log_queue_size";
  const char* STR_VIP = "vip";
  const char* STR_NETWORK_TIMEOUT = "network_timeout";

  const char* STR_LOG_SYNC_LIMIT_KB = "log_sync_limit_kb";
  const char* STR_LOG_SYNC_TIMEOUT_US = "log_sync_timeout_us";
  const char* STR_REPLAY_WAIT_TIME_US = "replay_wait_time_us";
  const char* STR_REGISTER_TIMES = "register_times";
  const char* STR_REGISTER_TIMEOUT_US = "register_timeout_us";
  const char* STR_VIP_CHECK_PERIOD_US = "vip_check_period_us";

  const char* STR_LEASE_ON = "lease_on";
  const char* STR_LEASE_INTERVAL_US = "lease_interval_us";
  const char* STR_LEASE_RESERVED_TIME_US = "lease_reserved_time_us";

  const int64_t PACKET_TIME_OUT = 100 * 1000;  //100 ms
  const int32_t ADDR_BUF_LEN = 64;
}
namespace oceanbase 
{ 
  using namespace common;
  namespace rootserver
  {
    using namespace oceanbase::common;
    OBRootWorker::OBRootWorker()
      :is_registered_(false),
      task_read_queue_size_(DEFAULT_TASK_READ_QUEUE_SIZE),
      task_write_queue_size_(DEFAULT_TASK_WRITE_QUEUE_SIZE),
      task_log_queue_size_(DEFAULT_TASK_LOG_QUEUE_SIZE),
      network_timeout_(PACKET_TIME_OUT),
      stat_manager_()
    {
      config_file_name_[0] = '\0';
    }
    OBRootWorker::~OBRootWorker()
    {
    }
    int OBRootWorker::set_config_file_name(const char* conf_file_name)
    {
      strncpy(config_file_name_, conf_file_name, OB_MAX_FILE_NAME_LENGTH);
      config_file_name_[OB_MAX_FILE_NAME_LENGTH - 1] = '\0';
      return OB_SUCCESS;
    }
    int OBRootWorker::initialize()
    {
      int ret = OB_SUCCESS;

      if (OB_SUCCESS == ret) 
      {
        ret = client_manager.initialize(get_transport(), get_packet_streamer());
      }

      if (OB_SUCCESS == ret)
      {
        ret = rt_rpc_stub_.init(&client_manager);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "init rpc stub failed, err=%d", ret);
        }
        else
        {
          rt_rpc_stub_.set_root_worker(this);
        }
      }

      const char* vip_str = NULL;
      if (ret == OB_SUCCESS)
      {    
        vip_str =  TBSYS_CONFIG.getString(STR_ROOT_SECTION, STR_VIP, NULL);
        if (vip_str == NULL)
        {
          TBSYS_LOG(ERROR, "vip of rootserver is not set, section: %s, config item: %s", STR_ROOT_SECTION, STR_VIP);
          ret = OB_INVALID_ARGUMENT;
        }
      }    
      uint32_t vip = tbsys::CNetUtil::getAddr(vip_str);

      if (ret == OB_SUCCESS)
      {    
        int32_t local_ip = tbsys::CNetUtil::getLocalAddr(dev_name_);
        if (!self_addr_.set_ipv4_addr(local_ip, port_))
        {
          TBSYS_LOG(ERROR, "rootserver address invalid, ip:%d, port:%d", local_ip, port_);
          ret = OB_ERROR;
        }
      }    

      if (OB_SUCCESS == ret)
      {
        int64_t log_sync_timeout = TBSYS_CONFIG.getInt(STR_ROOT_SECTION, STR_LOG_SYNC_TIMEOUT_US, DEFAULT_LOG_SYNC_TIMEOUT_US);
        int64_t lease_interval = TBSYS_CONFIG.getInt(STR_ROOT_SECTION, STR_LEASE_INTERVAL_US, DEFAULT_LEASE_INTERVAL_US);
        int64_t lease_reserv = TBSYS_CONFIG.getInt(STR_ROOT_SECTION, STR_LEASE_RESERVED_TIME_US, DEFAULT_LEASE_RESERVED_TIME_US);
        ret = slave_mgr_.init(vip, &rt_rpc_stub_, log_sync_timeout, lease_interval, lease_reserv);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to init slave manager, err=%d", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        int thread_count = 0;
        thread_count = TBSYS_CONFIG.getInt(STR_ROOT_SECTION, STR_THREAD_COUNT, 20);
        read_thread_queue_.setThreadParameter(thread_count, this, NULL);
        void* args = reinterpret_cast<void*>(WRITE_THREAD_FLAG);
        write_thread_queue_.setThreadParameter(1, this, args); 

        args = reinterpret_cast<void*>(LOG_THREAD_FLAG);
        log_thread_queue_.setThreadParameter(1, this, args);

        task_read_queue_size_ = TBSYS_CONFIG.getInt(STR_ROOT_SECTION, STR_READ_QUEUE_SIZE, DEFAULT_TASK_READ_QUEUE_SIZE);
        task_write_queue_size_ = TBSYS_CONFIG.getInt(STR_ROOT_SECTION, STR_WRITE_QUEUE_SIZE, DEFAULT_TASK_WRITE_QUEUE_SIZE);
        task_log_queue_size_ = TBSYS_CONFIG.getInt(STR_ROOT_SECTION, STR_LOG_QUEUE_SIZE, DEFAULT_TASK_LOG_QUEUE_SIZE);
        // read_thread_queue_.start();
        // write_thread_queue_.start();
      }

      if (ret == OB_SUCCESS)
      {
        if (tbsys::CNetUtil::isLocalAddr(vip))
        {
          TBSYS_LOG(INFO, "I am holding the VIP, set role to MASTER");
          role_mgr_.set_role(ObRoleMgr::MASTER);
        }
        else
        {
          TBSYS_LOG(INFO, "I am not holding the VIP, set role to SLAVE");
          role_mgr_.set_role(ObRoleMgr::SLAVE);
        }
        rt_master_.set_ipv4_addr(vip, port_);

        int64_t vip_check_period_us = TBSYS_CONFIG.getInt(STR_ROOT_SECTION, STR_VIP_CHECK_PERIOD_US, DEFAULT_VIP_CHECK_PERIOD_US);
        ret = check_thread_.init(&role_mgr_, vip, vip_check_period_us, &rt_rpc_stub_, &rt_master_, &self_addr_);
      }

      if (ret == OB_SUCCESS)
      {
        int64_t now = 0;
        now = tbsys::CTimeUtil::getTime();
        if (!root_server_.init(config_file_name_, now, this))
        {
          ret = OB_ERROR;
        }
      }
      TBSYS_LOG(INFO, "root worker init, ret=%d", ret);
      return ret;
    }

    int OBRootWorker::start_service()
    {
      int ret = OB_ERROR;

      ObRoleMgr::Role role = role_mgr_.get_role();
      if (role == ObRoleMgr::MASTER)
      {
        ret = start_as_master();
      }
      else if (role == ObRoleMgr::SLAVE)
      {
        ret = start_as_slave();
      }
      else
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "unknow role: %d, rootserver start failed", role);
      }

      return ret;
    }

    int OBRootWorker::start_as_master()
    {
      int ret = OB_ERROR;
      TBSYS_LOG(INFO, "[NOTICE] master start step1");
      ret = log_manager_.init(&root_server_, &slave_mgr_);
      if (ret == OB_SUCCESS)
      {
        // try to replay log
        TBSYS_LOG(INFO, "[NOTICE] master start step2");
        ret = log_manager_.replay_log();
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "[NOTICE] master replay log failed, err=%d", ret);
        }
      }

      if (ret == OB_SUCCESS)
      {
        TBSYS_LOG(INFO, "[NOTICE] master start step3");
        log_manager_.get_log_worker()->reset_cs_hb_time();
      }


      if (ret == OB_SUCCESS)
      {
        TBSYS_LOG(INFO, "[NOTICE] master start step4");
        network_timeout_ = TBSYS_CONFIG.getInt(STR_ROOT_SECTION, STR_NETWORK_TIMEOUT, PACKET_TIME_OUT);
        role_mgr_.set_state(ObRoleMgr::ACTIVE);

        read_thread_queue_.start();
        write_thread_queue_.start();
        check_thread_.start();
        TBSYS_LOG(INFO, "[NOTICE] master start-up finished");

        // wait finish
        for (;;)
        {
          if (ObRoleMgr::STOP == role_mgr_.get_state()
              || ObRoleMgr::ERROR == role_mgr_.get_state())
          {
            TBSYS_LOG(INFO, "role manager change state, stat=%d", role_mgr_.get_state());
            break;
          }
          usleep(10 * 1000); // 10 ms
        }
      }

      TBSYS_LOG(INFO, "[NOTICE] going to quit");
      stop();

      return ret;
    }

    int OBRootWorker::start_as_slave()
    {
      int err = OB_SUCCESS;

      log_thread_queue_.start();

      err = log_manager_.init(&root_server_, &slave_mgr_);

      ObFetchParam fetch_param;
      if (err == OB_SUCCESS)
      {
        err = slave_register_(fetch_param);
      }

      if (err == OB_SUCCESS)
      {
        err = fetch_thread_.init(rt_master_, log_manager_.get_log_dir_path(), fetch_param, &role_mgr_, &log_replay_thread_);
        if (err == OB_SUCCESS)
        {
          int64_t log_limit = TBSYS_CONFIG.getInt(STR_ROOT_SECTION, STR_LOG_SYNC_LIMIT_KB, DEFAULT_LOG_SYNC_LIMIT_KB);
          fetch_thread_.set_limit_rate(log_limit);
          fetch_thread_.add_ckpt_ext(ObRootServer2::ROOT_TABLE_EXT); // add root table file
          fetch_thread_.add_ckpt_ext(ObRootServer2::CHUNKSERVER_LIST_EXT); // add chunkserver list file
          fetch_thread_.set_log_manager(&log_manager_);
          fetch_thread_.start();

          if (fetch_param.fetch_ckpt_)
          {
            err = fetch_thread_.wait_recover_done();
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "failed to init fetch log thread");
        }
      }

      if (err == OB_SUCCESS)
      {
        role_mgr_.set_state(ObRoleMgr::ACTIVE);
        root_server_.start_threads();
        root_server_.wait_init_finished();
      }
      
      if (err == OB_SUCCESS)
      {
        int64_t replay_wait_time = TBSYS_CONFIG.getInt(STR_ROOT_SECTION, STR_REPLAY_WAIT_TIME_US, DEFAULT_REPLAY_WAIT_TIME_US);
        err = log_replay_thread_.init(log_manager_.get_log_dir_path(), fetch_param.min_log_id_, &role_mgr_, replay_wait_time);
        if (err == OB_SUCCESS)
        {
          log_replay_thread_.set_log_manager(&log_manager_);
          log_replay_thread_.start();
          TBSYS_LOG(INFO, "slave log_replay_thread started");
        }
        else
        {
          TBSYS_LOG(ERROR, "failed to start log replay thread");
        }
      }

      if (err == OB_SUCCESS)
      {
        check_thread_.start();
        TBSYS_LOG(INFO, "slave check_thread started");

        while (ObRoleMgr::SWITCHING != role_mgr_.get_state())
        {
          if (ObRoleMgr::STOP == role_mgr_.get_state()
              || ObRoleMgr::ERROR == role_mgr_.get_state())
          {
            break;
          }
          usleep(10 * 1000); // 10 ms
        }

        if (ObRoleMgr::SWITCHING == role_mgr_.get_state())
        {
          TBSYS_LOG(WARN, "rootserver slave begin switch to master");
          
          role_mgr_.set_role(ObRoleMgr::MASTER);
          TBSYS_LOG(INFO, "[NOTICE] set role to master");
          log_manager_.get_log_worker()->reset_cs_hb_time();

          log_thread_queue_.stop();
          log_thread_queue_.wait();
          // log replay thread will stop itself when
          // role switched to MASTER and nothing
          // more to replay
          log_replay_thread_.wait();
          fetch_thread_.wait();

          // update role after log replay thread done
          //role_mgr_.set_role(ObRoleMgr::MASTER);

          read_thread_queue_.start();
          write_thread_queue_.start();

          //get last frozen mem table version from updateserver
          int64_t frozen_version = 1;
          if (OB_SUCCESS == ups_get_last_frozen_memtable_version(root_server_.get_update_server_info(),
                frozen_version))
          {
            root_server_.report_frozen_memtable(frozen_version, false);
          }
          else
          {
            TBSYS_LOG(WARN,"get frozen version failed");
          }

          role_mgr_.set_state(ObRoleMgr::ACTIVE);
          TBSYS_LOG(INFO, "set stat to ACTIVE");

          is_registered_ = false;
          TBSYS_LOG(WARN, "rootserver slave switched to master");

          for (;;)
          {
            if (ObRoleMgr::STOP == role_mgr_.get_state()
                || ObRoleMgr::ERROR == role_mgr_.get_state())
            {
              TBSYS_LOG(INFO, "role manager change state, stat=%d", role_mgr_.get_state());
              break;
            }
            usleep(10 * 1000);
          }
        }
      }

      TBSYS_LOG(INFO, "[NOTICE] going to quit");
      stop();
      return err;
    }

    int OBRootWorker::get_ups_frozen_memtable_version()
    {
      int64_t frozen_version = 0;
      int ret = ups_get_last_frozen_memtable_version(root_server_.get_update_server_info(),frozen_version);
      if (OB_SUCCESS == ret)
      {
        root_server_.report_frozen_memtable(frozen_version, false);
      }
      return ret;
    }

    void OBRootWorker::destroy()
    {
      role_mgr_.set_state(ObRoleMgr::STOP);

      if (ObRoleMgr::SLAVE == role_mgr_.get_role())
      {
        if (is_registered_)
        {
          rt_rpc_stub_.slave_quit(rt_master_, self_addr_, DEFAULT_SLAVE_QUIT_TIMEOUT);
          is_registered_ = false;
        }
        log_thread_queue_.stop();
        fetch_thread_.stop();
        log_replay_thread_.stop();
        check_thread_.stop();
      }
      else
      {
        read_thread_queue_.stop();
        write_thread_queue_.stop();
        check_thread_.stop();
      }
      wait_for_queue();
    }

    void OBRootWorker::wait_for_queue()
    {
      if (ObRoleMgr::SLAVE == role_mgr_.get_role())
      {
        log_thread_queue_.wait();
        fetch_thread_.wait();
        log_replay_thread_.wait();
        check_thread_.wait();
      }
      else
      {
        read_thread_queue_.wait();
        write_thread_queue_.wait();
        check_thread_.wait();
      }
    }

    tbnet::IPacketHandler::HPRetCode OBRootWorker::handlePacket(tbnet::Connection *connection, tbnet::Packet *packet)
    {
      tbnet::IPacketHandler::HPRetCode rc = tbnet::IPacketHandler::FREE_CHANNEL;
      if (!packet->isRegularPacket())
      {
        TBSYS_LOG(WARN, "control packet, packet code: %d", ((tbnet::ControlPacket*)packet)->getCommand());
      } 
      else
      {
        ObPacket* req = (ObPacket*) packet;
        req->set_connection(connection);
        bool ps = true;
        int packet_code = req->get_packet_code();
        TBSYS_LOG(DEBUG,"get packet code is %d", packet_code);
        switch(packet_code)
        {
          case OB_SEND_LOG:
          case OB_GRANT_LEASE_REQUEST:
            if (ObRoleMgr::SLAVE == role_mgr_.get_role())
            {
              ps = log_thread_queue_.push(req, task_log_queue_size_, false);
            }
            else
            {
              ps = false;
            }
            break;
          case OB_RENEW_LEASE_REQUEST:
          case OB_SLAVE_QUIT:
            if (ObRoleMgr::MASTER == role_mgr_.get_role())
            {
              ps = read_thread_queue_.push(req, task_read_queue_size_, false);
            }
            else
            {
              ps = false;
            }
            break;
          case OB_REPORT_TABLETS:
          case OB_SERVER_REGISTER:
          case OB_MIGRATE_OVER:
          case OB_REPORT_CAPACITY_INFO:
          case OB_SLAVE_REG:
          case OB_WAITING_JOB_DONE:
            //the packet will cause write to b+ tree
            ps = write_thread_queue_.push(req, task_write_queue_size_, false);
            break;
          case OB_FETCH_SCHEMA:
          case OB_FETCH_SCHEMA_VERSION:
          case OB_GET_REQUEST:
          case OB_SCAN_REQUEST:
          case OB_HEARTBEAT:
          case OB_DUMP_CS_INFO:
          case OB_FETCH_STATS:
          case OB_GET_UPDATE_SERVER_INFO:
          case OB_UPDATE_SERVER_REPORT_FREEZE:
          case OB_GET_UPDATE_SERVER_INFO_FOR_MERGE:
          case OB_GET_MERGE_DELAY_INTERVAL:
            ps = read_thread_queue_.push(req, task_read_queue_size_, false);
            break;
          case OB_PING_REQUEST:
            if (ObRoleMgr::MASTER == role_mgr_.get_role())
            {
              ps = read_thread_queue_.push(req, task_read_queue_size_, false);
            }
            else
            {
              ps = log_thread_queue_.push(req, task_log_queue_size_, false);
            }
            break;
          default:
            ps = false; // so this unknow packet will be freed
            TBSYS_LOG(WARN, "UNKNOW packet %d, ignore this", packet_code);
            break;
        }
        if (!ps)
        {
          TBSYS_LOG(WARN, "pakcet %d can not be distribute to queue", packet_code);
          rc = tbnet::IPacketHandler::KEEP_CHANNEL;
        }
      } 
      return rc;
    }
    bool OBRootWorker::handleBatchPacket(tbnet::Connection *connection, tbnet::PacketQueue &packetQueue)
    {
      UNUSED(connection);
      UNUSED(packetQueue);
      TBSYS_LOG(ERROR, "you should not reach this, not supporrted");
      return true;
    }
    bool OBRootWorker::handlePacketQueue(tbnet::Packet *packet, void *args)
    {
      bool ret = true;
      int return_code = OB_SUCCESS;
      
      ObPacket* ob_packet = reinterpret_cast<ObPacket*>(packet);
      int packet_code = ob_packet->get_packet_code();
      int version = ob_packet->get_api_version();
      uint32_t channel_id = ob_packet->getChannelId();//tbnet need this

      int64_t source_timeout = ob_packet->get_source_timeout();
      if (source_timeout > 0)
      {
        int64_t block_us = tbsys::CTimeUtil::getTime() - ob_packet->get_receive_ts();
        if (block_us > source_timeout)
        {
          TBSYS_LOG(WARN, "packet code=%d block for %ld(us) exceed time out %ld(us) ignore this packet",
              packet_code, block_us, ob_packet->get_receive_ts());
          return_code = OB_RESPONSE_TIME_OUT;
        }
      }
      
      if (OB_SUCCESS == return_code)
      {
        return_code = ob_packet->deserialize();
        if (OB_SUCCESS == return_code) 
        {
          ObDataBuffer* in_buf = ob_packet->get_buffer(); 
          if (in_buf == NULL)
          {
            TBSYS_LOG(ERROR, "in_buff is NUll should not reach this");
          }
          else
          {
            tbnet::Connection* conn = ob_packet->get_connection();
            ThreadSpecificBuffer::Buffer* my_buffer = my_thread_buffer.get_buffer();
            if (my_buffer != NULL)
            {
              my_buffer->reset();
              ObDataBuffer thread_buff(my_buffer->current(), my_buffer->remain());
              if ((void*)WRITE_THREAD_FLAG == args)
              {
                //write stuff
                TBSYS_LOG(DEBUG, "handle packet, packe code is %d", packet_code);
                switch(packet_code)
                {
                  case OB_REPORT_TABLETS:
                    return_code = rt_report_tablets(version, *in_buf, conn, channel_id, thread_buff);
                    break;
                  case OB_SERVER_REGISTER:
                    return_code = rt_register(version, *in_buf, conn, channel_id, thread_buff);
                    break;
                  case OB_MIGRATE_OVER:
                    return_code = rt_migrate_over(version, *in_buf, conn, channel_id, thread_buff);
                    break;
                  case OB_REPORT_CAPACITY_INFO:
                    return_code = rt_report_capacity_info(version, *in_buf, conn, channel_id, thread_buff);
                    break;
                  case OB_SLAVE_REG:
                    return_code = rt_slave_register(version, *in_buf, conn, channel_id, thread_buff);
                    break;
                  case OB_WAITING_JOB_DONE:
                    return_code = rt_waiting_job_done(version, *in_buf, conn, channel_id, thread_buff);
                    break;
                  default:
                    return_code = OB_ERROR;
                    break;
                }
              }
              else if ((void*)LOG_THREAD_FLAG == args)
              {
                switch(packet_code)
                {
                  case OB_PING_REQUEST:
                    return_code = rt_ping(version, *in_buf, conn, channel_id, thread_buff);
                    break;
                  case OB_GRANT_LEASE_REQUEST:
                    return_code = rt_grant_lease(version, *in_buf, conn, channel_id, thread_buff);
                    break;
                  default:
                    in_buf->get_limit() = in_buf->get_position() + ob_packet->get_data_length();
                    return_code = rt_slave_write_log(version, *in_buf, conn, channel_id, thread_buff);
                    break;
                }
              }
              else
              {
                TBSYS_LOG(DEBUG, "handle packet, packe code is %d", packet_code);
                switch(packet_code)
                {
                  case OB_FETCH_SCHEMA:
                    return_code = rt_fetch_schema(version, *in_buf, conn, channel_id, thread_buff);
                    break;
                  case OB_FETCH_SCHEMA_VERSION:
                    return_code = rt_fetch_schema_version(version, *in_buf, conn, channel_id, thread_buff);
                    break;
                  case OB_GET_REQUEST:
                    return_code = rt_get(version, *in_buf, conn, channel_id, thread_buff);
                    break;
                  case OB_SCAN_REQUEST:
                    return_code = rt_scan(version, *in_buf, conn, channel_id, thread_buff);
                    break;
                  case OB_HEARTBEAT:
                    return_code = rt_heartbeat(version, *in_buf, conn, channel_id, thread_buff);
                    break;
                  case OB_DUMP_CS_INFO:
                    return_code = rt_dump_cs_info(version, *in_buf, conn, channel_id, thread_buff);
                    break;
                  case OB_FETCH_STATS:
                    return_code = rt_fetch_stats(version, *in_buf, conn, channel_id, thread_buff);
                    break;
                  case OB_PING_REQUEST:
                    return_code = rt_ping(version, *in_buf, conn, channel_id, thread_buff);
                    break;
                  case OB_GET_UPDATE_SERVER_INFO:
                    return_code = rt_get_update_server_info(version, *in_buf, conn, channel_id, thread_buff);
                    break;
                  case OB_GET_UPDATE_SERVER_INFO_FOR_MERGE:
                    return_code = rt_get_update_server_info(version, *in_buf, conn, channel_id, thread_buff,true);
                    break;
                  case OB_GET_MERGE_DELAY_INTERVAL:
                    return_code = rt_get_merge_delay_interval(version, *in_buf, conn, channel_id, thread_buff);
                    break;
                  case OB_RENEW_LEASE_REQUEST:
                    return_code = rt_renew_lease(version, *in_buf, conn, channel_id, thread_buff);
                    break;
                  case OB_SLAVE_QUIT:
                    return_code = rt_slave_quit(version, *in_buf, conn, channel_id, thread_buff);
                    break;
                  case OB_UPDATE_SERVER_REPORT_FREEZE:
                    return_code = rt_update_server_report_freeze(version, *in_buf, conn, channel_id, thread_buff);
                    break;
                  default:
                    TBSYS_LOG(ERROR, "unknow packet code %d in read queue", packet_code);
                    return_code = OB_ERROR;
                    break;
                }
              }
              if (OB_SUCCESS != return_code)
              {
                TBSYS_LOG(ERROR, "call func error packet_code is %d return code is %d", packet_code, return_code);
              }
            }
            else
            {
              TBSYS_LOG(ERROR, "get thread buffer error, ignore this packet");
            }
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "packet deserialize error packet code is %d from server %s", 
              packet_code, tbsys::CNetUtil::addrToString(ob_packet->get_connection()->getPeerId()).c_str());
        }
      }
      return ret;//if return true packet will be deleted.
    }

    int OBRootWorker::up_switch_schema(const common::ObServer& server, common::ObSchemaManagerV2* schema_manager)
    {
      int ret = OB_SUCCESS;

      if (NULL == schema_manager)
      {
        ret = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == ret)
      {
        if (TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_INFO)
        {
          char server_char[OB_IP_STR_BUFF];
          server.to_string(server_char, OB_IP_STR_BUFF);
          TBSYS_LOG(INFO, "up_switch_schema(%s)", server_char);
        }
      }

      static const int MY_VERSION = 1;
      if (OB_SUCCESS == ret)
      {
        ThreadSpecificBuffer::Buffer* my_buffer = my_thread_buffer.get_buffer();
        if (my_buffer != NULL)
        {
          my_buffer->reset();
          ObDataBuffer thread_buff(my_buffer->current(), my_buffer->remain());
          ret = schema_manager->serialize(thread_buff.get_data(), thread_buff.get_capacity(), thread_buff.get_position());
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "ObSchemaManager serialize error, ret=%d", ret);
          }
          else
          {
            ret = client_manager.send_request(server, OB_SWITCH_SCHEMA, MY_VERSION, network_timeout_, thread_buff);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR, "ClientManager send_request error, ret=%d thread_buff.data=%p", ret, thread_buff.get_data());
            }
          }
          ObDataBuffer out_buffer(thread_buff.get_data(), thread_buff.get_position());
          if (ret == OB_SUCCESS) 
          {
            common::ObResultCode result_msg;
            ret = result_msg.deserialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
            if (ret != OB_SUCCESS)
            {
              TBSYS_LOG(ERROR, "ObResultCode deserialize error, ret=%d buf.data=%p buf.cap=%ld buf.pos=%ld",
                  ret, out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
            }
            else
            {
              ret = result_msg.result_code_;
              if (ret != OB_SUCCESS) 
              {
                TBSYS_LOG(INFO, "rpc return error code is %d msg is %s", result_msg.result_code_, result_msg.message_.ptr());
              }
            }
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "can not get thread buffer");
          ret = OB_ERROR;
        }
      }
      return ret;
    }

    int OBRootWorker::up_freeze_mem(const common::ObServer& server, int64_t& frozen_mem_version)
    {
      if (TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_INFO)
      {
        char server_char[OB_IP_STR_BUFF];
        server.to_string(server_char, OB_IP_STR_BUFF);
        TBSYS_LOG(INFO, "up_freeze_mem(%s)", server_char);
      }
      static const int MY_VERSION = 1;
      int ret = OB_SUCCESS;
      ThreadSpecificBuffer::Buffer* my_buffer = my_thread_buffer.get_buffer();
      if (my_buffer != NULL)
      {
        my_buffer->reset();
        ObDataBuffer thread_buff(my_buffer->current(), my_buffer->remain());
        if (OB_SUCCESS == ret)
        {
          ret = client_manager.send_request(server, OB_FREEZE_MEM_TABLE, MY_VERSION, network_timeout_, thread_buff);
        }
        ObDataBuffer out_buffer(thread_buff.get_data(), thread_buff.get_position());
        if (ret == OB_SUCCESS) 
        {
          common::ObResultCode result_msg;
          ret = result_msg.deserialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
          if (ret == OB_SUCCESS)
          {
            ret = result_msg.result_code_;
            if (ret != OB_SUCCESS) 
            {
              TBSYS_LOG(INFO, "rpc return error code is %d msg is %s", result_msg.result_code_, result_msg.message_.ptr());
            }
          }
          if (ret == OB_SUCCESS)
          {
            ret = common::serialization::decode_vi64(out_buffer.get_data(), out_buffer.get_capacity(), 
                out_buffer.get_position(), &frozen_mem_version);
            if (ret != OB_SUCCESS) 
            {
              TBSYS_LOG(INFO, "deserialize frozen_mem_version error error_code is %d", ret);
            }
          }
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "can not get thread buffer");
        ret = OB_ERROR;
      }
      return ret;
    }
    bool OBRootWorker::is_master() const
    {
      return root_server_.is_master();
    }
    void OBRootWorker::dump_root_table() const
    {
      root_server_.dump_root_table();
      return;
    }
    void OBRootWorker::dump_available_server() const
    {
      root_server_.print_alive_server();
      return;
    }
    void OBRootWorker::drop_current_merge () 
    {
      root_server_.drop_current_build();
      return;
    }
    void OBRootWorker::use_new_schema()
    {
      root_server_.use_new_schema();
      return;
    }
    void OBRootWorker::reload_config()
    {
      root_server_.reload_config();
      return;
    }
    //bool OBRootWorker::start_report()
    //{
    //  //int64_t now = tbsys::CTimeUtil::getTime();
    //  bool ret = false;
    //  int retry = 0;
    //  while ((ret = root_server_.start_report(true)) != true) 
    //  {
    //    retry++;
    //    if (retry > 5) break;
    //    usleep(500);
    //  }
    //  return ret;
    //}
    bool OBRootWorker::start_merge()
    {
      //int64_t now = tbsys::CTimeUtil::getTime();
      bool ret = false;
      int retry = 0;
      while ((ret = root_server_.start_switch()) != true) 
      {
        retry++;
        if (retry > 1000) break;
        usleep(5000);
      }
      return ret;
    }
    int OBRootWorker::rt_get_update_server_info(const int32_t version, ObDataBuffer& in_buff, 
        tbnet::Connection* conn, const uint32_t channel_id, ObDataBuffer& out_buff,
        bool use_inner_port /* = false*/)
    {
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;

      //next two lines only for exmaples, actually this func did not need this
      char msg_buff[OB_MAX_RESULT_MESSAGE_LENGTH]; 
      result_msg.message_.assign_buffer(msg_buff, OB_MAX_RESULT_MESSAGE_LENGTH);

      if (version != MY_VERSION)
      {
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      UNUSED(in_buff); // rt_get_update_server_info() no input params
      common::ObServer found_server;
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        found_server = root_server_.get_update_server_info();

        if (use_inner_port)
        {
          found_server.set_port(root_server_.get_update_server_inner_port());
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "result_msg.serialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = found_server.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "found_server.serialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        send_response(OB_GET_UPDATE_SERVER_INFO_RES, MY_VERSION, out_buff, conn, channel_id);
      }
      return ret;
    }
    
    int OBRootWorker::rt_get_merge_delay_interval(const int32_t version, ObDataBuffer& in_buff, 
        tbnet::Connection* conn, const uint32_t channel_id, ObDataBuffer& out_buff)
    {
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;

      //next two lines only for exmaples, actually this func did not need this
      char msg_buff[OB_MAX_RESULT_MESSAGE_LENGTH]; 
      result_msg.message_.assign_buffer(msg_buff, OB_MAX_RESULT_MESSAGE_LENGTH);

      if (version != MY_VERSION)
      {
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      UNUSED(in_buff); // rt_get_update_server_info() no input params

      if (OB_SUCCESS == ret)
      {
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "result_msg.serialize error");
        }
      }

      if (OB_SUCCESS == ret)
      {
        int64_t interval = root_server_.get_merge_delay_interval();
        ret = serialization::encode_vi64(out_buff.get_data(),out_buff.get_capacity(),out_buff.get_position(),interval);
      }

      if (OB_SUCCESS == ret)
      {
        send_response(OB_GET_MERGE_DELAY_INTERVAL_RES, MY_VERSION, out_buff, conn, channel_id);
      }
      return ret;
    }


    int OBRootWorker::rt_scan(const int32_t version, ObDataBuffer& in_buff, 
        tbnet::Connection* conn, const uint32_t channel_id, ObDataBuffer& out_buff)
    {
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;

      char msg_buff[OB_MAX_RESULT_MESSAGE_LENGTH];
      result_msg.message_.assign_buffer(msg_buff, OB_MAX_RESULT_MESSAGE_LENGTH);

      if (version != MY_VERSION)
      {
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      ObScanParam scan_param_in;
      ObScanner scanner_out;

      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = scan_param_in.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "scan_param_in.deserialize error");
        }
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_) 
      {
        result_msg.result_code_ = scan(scan_param_in, scanner_out);
      }

      if (OB_SUCCESS == ret)
      {
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "result_msg.serialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = scanner_out.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "scanner_out.serialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        send_response(OB_SCAN_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
      }
      if (OB_SUCCESS == ret) 
      {
        stat_manager_.inc(ObRootStatManager::ROOT_TABLE_ID, ObRootStatManager::INDEX_SUCCESS_SCAN_COUNT);
      }
      else
      {
        stat_manager_.inc(ObRootStatManager::ROOT_TABLE_ID, ObRootStatManager::INDEX_FAIL_SCAN_COUNT);
      }
      return ret;
    }
    //ObResultCode rt_get(const ObGetParam& get_param, ObScanner& scanner);

    int OBRootWorker::rt_get(const int32_t version, ObDataBuffer& in_buff, 
        tbnet::Connection* conn, const uint32_t channel_id, ObDataBuffer& out_buff)
    {
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;

      char msg_buff[OB_MAX_RESULT_MESSAGE_LENGTH];
      result_msg.message_.assign_buffer(msg_buff, OB_MAX_RESULT_MESSAGE_LENGTH);

      if (version != MY_VERSION)
      {
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      ObGetParam get_param_in;
      ObScanner scanner_out;

      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = get_param_in.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "get_param_in.deserialize error");
        }
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_) 
      {
        result_msg.result_code_ = get(get_param_in, scanner_out);
      }

      if (OB_SUCCESS == ret)
      {
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "result_msg.serialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = scanner_out.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "scanner_out.serialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        send_response(OB_GET_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
      }
      if (OB_SUCCESS == ret) 
      {
        stat_manager_.inc(ObRootStatManager::ROOT_TABLE_ID, ObRootStatManager::INDEX_SUCCESS_GET_COUNT);
      }
      else
      {
        stat_manager_.inc(ObRootStatManager::ROOT_TABLE_ID, ObRootStatManager::INDEX_FAIL_GET_COUNT);
      }
      return ret;
    }
    int OBRootWorker::get(const common::ObGetParam& get_param, common::ObScanner& scanner)
    {
      int ret = OB_SUCCESS;
      const ObCellInfo* cell = get_param[0];
      if (cell == NULL)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        int8_t rt_type = 0;
        //cell->value_.get_extend_field(rt_type); 
        UNUSED(rt_type); // for now we ignore this; OP_RT_TABLE_TYPE or OP_RT_TABLE_INDEX_TYPE
        if (cell->table_id_ != 0 && cell->table_id_ != OB_INVALID_ID)
        {
          ret = root_server_.find_root_table_key(cell->table_id_, cell->row_key_, scanner);
        }
        else
        {
          ret = root_server_.find_root_table_key(cell->table_name_, cell->row_key_, scanner);
        }
      }
      return ret;

    }
    int OBRootWorker::scan(const common::ObScanParam& scan_param, common::ObScanner& scanner)
    {
      int ret = OB_SUCCESS;
      int8_t rt_type;  // we have no this infomation;
      UNUSED(rt_type); // for now we ignore this;
      ret = root_server_.find_root_table_range(scan_param.get_table_name(), *(scan_param.get_range()), scanner);
      return ret;

    }

    int OBRootWorker::rt_fetch_schema(const int32_t version, common::ObDataBuffer& in_buff, 
        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(in_buff);
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;

      if (version != MY_VERSION)
      {
        TBSYS_LOG(ERROR,"version not match,version:%d,MY_VERSION:%d",version,MY_VERSION);
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      //TBSYS_LOG(DEBUG, "inbuff %d %d", in_buff.get_capacity(), in_buff.get_position());

      //int64_t time_stamp = 0;
      ObSchemaManagerV2* schema = new(std::nothrow) ObSchemaManagerV2();
      if (schema == NULL)
      {
        TBSYS_LOG(ERROR, "error can not get mem for schema");
        ret = OB_ERROR;
      }

      //if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      //{
      //  ret = common::serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &time_stamp);
      //  if (ret != OB_SUCCESS)
      //  {
      //    TBSYS_LOG(ERROR, "time_stamp.deserialize error");
      //  }
      //}
      //TBSYS_LOG(DEBUG, "input time_stamp is %ld", time_stamp);

      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        if (!root_server_.get_schema(*schema))
        {
          TBSYS_LOG(ERROR,"get schema failed");
          result_msg.result_code_ = OB_ERROR;
        }
        else
        {
          TBSYS_LOG(INFO, "get schema, version=%ld", schema->get_version());
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "result_msg.serialize error");
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = schema->serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "schema.serialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        send_response(OB_FETCH_SCHEMA_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
      }
      if (NULL != schema)
      {
        delete schema;
        schema = NULL;
      }
      return ret;
    }
    int OBRootWorker::rt_fetch_schema_version(const int32_t version, common::ObDataBuffer& in_buff, 
        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(in_buff);
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      int64_t schema_version = 0;

      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        schema_version = root_server_.get_schema_version();
      }
      if (OB_SUCCESS == ret)
      {
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "result_msg.serialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position(), schema_version);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "schema version serialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        send_response(OB_FETCH_SCHEMA_VERSION_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
      }
      return ret;
    }
    int OBRootWorker::cs_start_merge(const ObServer& server, const int64_t frozen_mem_version, const int32_t init_flag)
    {
      if (TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_INFO)
      {
        char server_char[OB_IP_STR_BUFF];
        server.to_string(server_char, OB_IP_STR_BUFF);
        TBSYS_LOG(INFO, "cs_start_merge (%s, %ld)", server_char, frozen_mem_version);
      }
      static const int MY_VERSION = 1;
      int ret = OB_SUCCESS;
      ThreadSpecificBuffer::Buffer* my_buffer = my_thread_buffer.get_buffer();
      if (my_buffer != NULL)
      {
        my_buffer->reset();
        ObDataBuffer thread_buff(my_buffer->current(), my_buffer->remain());
        ret = common::serialization::encode_vi64(thread_buff.get_data(), 
            thread_buff.get_capacity(), thread_buff.get_position(), frozen_mem_version);
        if (OB_SUCCESS == ret)
        {
          ret = common::serialization::encode_vi32(thread_buff.get_data(), 
              thread_buff.get_capacity(), thread_buff.get_position(), init_flag);
        }

        common::ObServer tmp_server = server;
#ifdef PRESS_TEST
        tmp_server.reset_ipv4_10();
#endif
        if (OB_SUCCESS == ret)
        {
          ret = client_manager.send_request(tmp_server, OB_START_MERGE, MY_VERSION, network_timeout_, thread_buff);
        }
        ObDataBuffer out_buffer(thread_buff.get_data(), thread_buff.get_position());
        if (ret == OB_SUCCESS) 
        {
          common::ObResultCode result_msg;
          ret = result_msg.deserialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
          if (ret == OB_SUCCESS)
          {
            ret = result_msg.result_code_;
            if (ret != OB_SUCCESS) 
            {
              TBSYS_LOG(INFO, "rpc return error code is %d msg is %s", result_msg.result_code_, result_msg.message_.ptr());
            }
          }
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "can not get thread buffer");
        ret = OB_ERROR;
      }
      return ret;
    }

    //ObResultCode rt_report_tablets(const ObServer& server, const ObTabletReportInfoList& tablets, uint64_t time_stamp);
    int OBRootWorker::rt_report_tablets(const int32_t version, common::ObDataBuffer& in_buff, 
        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      ObServer server;
      ObTabletReportInfoList tablet_list;
      int64_t time_stamp = 0;

      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = server.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "server.deserialize error");
        }
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = tablet_list.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "tablet_list.deserialize error");
        }
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &time_stamp);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "time_stamp.deserialize error");
        }
      }

      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        result_msg.result_code_ = root_server_.report_tablets(server, tablet_list, time_stamp);
      }

      if (OB_SUCCESS == ret)
      {
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "result_msg.serialize error");
        }
      }

      if (OB_SUCCESS == ret)
      {
        send_response(OB_REPORT_TABLETS_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
      }

      return ret;
    }
    int OBRootWorker::rt_waiting_job_done(const int32_t version, common::ObDataBuffer& in_buff, 
        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      ObServer server;
      int64_t frozen_mem_version = 0;
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = server.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "server.deserialize error");
        }
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &frozen_mem_version);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "frozen_mem_version.deserialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        result_msg.result_code_ = root_server_.waiting_job_done(server, frozen_mem_version);
      }
      if (OB_SUCCESS == ret)
      {
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "result_msg.serialize error");
        }
      }

      if (OB_SUCCESS == ret)
      {
        send_response(OB_WAITING_JOB_DONE_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
      }
      return ret;

    }

    int OBRootWorker::rt_register(const int32_t version, common::ObDataBuffer& in_buff, 
        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        TBSYS_LOG(ERROR, "version:%d,MY_VERSION:%d",version,MY_VERSION);
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      ObServer server;
      bool is_merge_server = false;
      int32_t status = 0;
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = server.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "server.deserialize error");
        }
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = serialization::decode_bool(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &is_merge_server);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "time_stamp.deserialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(DEBUG,"receive server register,is_merge_server %d",is_merge_server ? 1 : 0);
        result_msg.result_code_ = root_server_.regist_server(server, is_merge_server, status);
      }
      if (OB_SUCCESS == ret)
      {
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "result_msg.serialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi32(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position(), status);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "schema.serialize error");
        }
      }

      if (OB_SUCCESS == ret)
      {
        send_response(OB_SERVER_REGISTER_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
      }
      return ret;

    }

    int OBRootWorker::rt_migrate_over(const int32_t version, common::ObDataBuffer& in_buff, 
        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      ObRange range;
      ObServer src_server;
      ObServer dest_server;
      bool keep_src = false;
      int64_t tablet_version = 0;
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = range.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "range.deserialize error");
        }
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = src_server.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "src_server.deserialize error");
        }
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = dest_server.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "dest_server.deserialize error");
        }
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = serialization::decode_bool(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &keep_src);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "keep_src.deserialize error");
        }
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &tablet_version);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "keep_src.deserialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        result_msg.result_code_ = root_server_.migrate_over(range, src_server, dest_server, keep_src, tablet_version);
      }
      if (OB_SUCCESS == ret)
      {
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "result_msg.serialize error");
        }
      }

      if (OB_SUCCESS == ret)
      {
        send_response(OB_MIGRATE_OVER_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
      }
      return ret;

    }
    int OBRootWorker::rt_report_capacity_info(const int32_t version, common::ObDataBuffer& in_buff, 
        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      ObServer server;
      int64_t capacity = 0;
      int64_t used = 0;

      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = server.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "server.deserialize error");
        }
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(),&capacity);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "capacity.deserialize error");
        }
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(),&used);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "used.deserialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        result_msg.result_code_ = root_server_.update_capacity_info(server, capacity, used);
      }
      if (OB_SUCCESS == ret)
      {
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "result_msg.serialize error");
        }
      }

      if (OB_SUCCESS == ret)
      {
        send_response(OB_REPORT_CAPACITY_INFO_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
      }
      return ret;

    }
    int OBRootWorker::rt_heartbeat(const int32_t version, common::ObDataBuffer& in_buff, 
        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      static const int MY_VERSION = 2;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      UNUSED(conn);
      UNUSED(channel_id);
      UNUSED(out_buff);
      int ret = OB_SUCCESS;
      //if (version != MY_VERSION)
      //{
      //  result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      //}
      ObServer server;
      ObRole role = OB_CHUNKSERVER;
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        ret = server.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "server.deserialize error");
        }
      }

      if (version == MY_VERSION)
      {
        ret = serialization::decode_vi32(in_buff.get_data(),in_buff.get_capacity(),in_buff.get_position(),reinterpret_cast<int32_t *>(&role));
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "decoe role error");
        }
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result_msg.result_code_)
      {
        result_msg.result_code_ = root_server_.receive_hb(server,role);
      }
      return ret;
    }
    int OBRootWorker::rt_dump_cs_info(const int32_t version, common::ObDataBuffer& in_buff, 
        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(in_buff);
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      ObChunkServerManager *out_server_manager = new ObChunkServerManager();
      if (out_server_manager == NULL)
      {
        TBSYS_LOG(ERROR, "can not new ObChunkServerManager");
        ret = OB_ERROR;
      }
      if (OB_SUCCESS == ret)
      {
        result_msg.result_code_ = root_server_.get_cs_info(out_server_manager);
      }
      if (OB_SUCCESS == ret)
      {
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "result_msg.serialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = out_server_manager->serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "schema.serialize error");
        }
      }
      if (out_server_manager != NULL)
      {
        delete out_server_manager;
        out_server_manager = NULL;
      }

      if (OB_SUCCESS == ret)
      {
        send_response(OB_DUMP_CS_INFO_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
      }
      return ret;

    }
    int OBRootWorker::rt_fetch_stats(const int32_t version, common::ObDataBuffer& in_buff, 
        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(in_buff);
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      if (OB_SUCCESS == ret)
      {
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "result_msg.serialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = stat_manager_.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "stat_manager_.serialize error");
        }
      }
      if (OB_SUCCESS == ret)
      {
        send_response(OB_FETCH_STATS_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
      }
      return ret;

    }
    int OBRootWorker::unload_old_table(const ObServer& server, const int64_t time_stamp)
    {
      if (TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_INFO)
      {
        char server_char[OB_IP_STR_BUFF];
        server.to_string(server_char, OB_IP_STR_BUFF);
        TBSYS_LOG(INFO, "unload_old_tablet (%s, %ld)", server_char, time_stamp);
      }
      static const int MY_VERSION = 1;
      int ret = OB_SUCCESS;
      ThreadSpecificBuffer::Buffer* my_buffer = my_thread_buffer.get_buffer();
      if (my_buffer != NULL)
      {
        my_buffer->reset();
        ObDataBuffer thread_buff(my_buffer->current(), my_buffer->remain());
        ret = common::serialization::encode_vi64(thread_buff.get_data(), 
            thread_buff.get_capacity(), thread_buff.get_position(), time_stamp);
        common::ObServer tmp_server = server;
#ifdef PRESS_TEST
        tmp_server.reset_ipv4_10();
#endif
        if (OB_SUCCESS == ret)
        {
          ret = client_manager.send_request(tmp_server, OB_DROP_OLD_TABLETS, MY_VERSION, network_timeout_, thread_buff);
        }
        ObDataBuffer out_buffer(thread_buff.get_data(), thread_buff.get_position());
        if (ret == OB_SUCCESS) 
        {
          common::ObResultCode result_msg;
          ret = result_msg.deserialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
          if (ret == OB_SUCCESS)
          {
            ret = result_msg.result_code_;
            if (ret != OB_SUCCESS) 
            {
              TBSYS_LOG(INFO, "rpc return error code is %d msg is %s", result_msg.result_code_, result_msg.message_.ptr());
            }
          }
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "can not get thread buffer");
        ret = OB_ERROR;
      }
      return ret;
    }
    int OBRootWorker::hb_to_cs(const common::ObServer& server, const int64_t lease_time,
                               const int64_t frozen_mem_version)
    {
      static const int MY_VERSION = 2;
      int ret = OB_SUCCESS;
      ThreadSpecificBuffer::Buffer* my_buffer = my_thread_buffer.get_buffer();
      if (my_buffer != NULL)
      {
        my_buffer->reset();
        ObDataBuffer thread_buff(my_buffer->current(), my_buffer->remain());
        ret = common::serialization::encode_vi64(thread_buff.get_data(), 
            thread_buff.get_capacity(), thread_buff.get_position(), lease_time);

        //TODO backward compatiblity
        if (OB_SUCCESS == ret)
        {
          ret = common::serialization::encode_vi64(thread_buff.get_data(),
              thread_buff.get_capacity(), thread_buff.get_position(), frozen_mem_version);
        }

        if (OB_SUCCESS == ret)
        {
          common::ObServer tmp_server = server;
#ifdef PRESS_TEST
          tmp_server.reset_ipv4_10();
#endif
          client_manager.post_request(tmp_server, OB_REQUIRE_HEARTBEAT, MY_VERSION, thread_buff);
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "can not get thread buffer");
        ret = OB_ERROR;
      }
      return ret;
    }

    int OBRootWorker::hb_to_ms(const common::ObServer& server, const int64_t lease_time,
                               const int64_t schema_version)
    {
      static const int MY_VERSION = 1;
      int ret = OB_SUCCESS;
      ThreadSpecificBuffer::Buffer* my_buffer = my_thread_buffer.get_buffer();
      if (NULL == my_buffer )
      {
        TBSYS_LOG(ERROR, "can not get thread buffer");
        ret = OB_ERROR;
      }
      else
      {
        my_buffer->reset();
        ObDataBuffer thread_buff(my_buffer->current(), my_buffer->remain());
        ret = common::serialization::encode_vi64(thread_buff.get_data(), 
            thread_buff.get_capacity(), thread_buff.get_position(), lease_time);

        if (OB_SUCCESS == ret)
        {
          ret = common::serialization::encode_vi64(thread_buff.get_data(),
              thread_buff.get_capacity(), thread_buff.get_position(), schema_version);
        }

        if (OB_SUCCESS == ret)
        {
          client_manager.post_request(server, OB_REQUIRE_HEARTBEAT, MY_VERSION, thread_buff);
        }
      }
      return ret;
    }

    int OBRootWorker::cs_migrate(const common::ObRange& range, 
        const common::ObServer& src_server, const common::ObServer& dest_server, bool keep_src)
    {
      static const int MY_VERSION = 1;
      int ret = OB_SUCCESS;
      ThreadSpecificBuffer::Buffer* my_buffer = my_thread_buffer.get_buffer();
      if (my_buffer != NULL)
      {
        my_buffer->reset();
        ObDataBuffer thread_buff(my_buffer->current(), my_buffer->remain());
        ret = range.serialize(thread_buff.get_data(), 
            thread_buff.get_capacity(), thread_buff.get_position());

        if (OB_SUCCESS == ret)
        {
          ret = dest_server.serialize(thread_buff.get_data(), 
              thread_buff.get_capacity(), thread_buff.get_position());
        }
        if (OB_SUCCESS == ret)
        {
          ret = common::serialization::encode_bool(thread_buff.get_data(), 
              thread_buff.get_capacity(), thread_buff.get_position(), keep_src);
        }
        if (OB_SUCCESS == ret)
        {
          ret = client_manager.send_request(src_server, OB_CS_MIGRATE, MY_VERSION, network_timeout_, thread_buff);
        }
        ObDataBuffer out_buffer(thread_buff.get_data(), thread_buff.get_position());
        if (ret == OB_SUCCESS) 
        {
          common::ObResultCode result_msg;
          ret = result_msg.deserialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
          if (ret == OB_SUCCESS)
          {
            ret = result_msg.result_code_;
            if (ret != OB_SUCCESS) 
            {
              TBSYS_LOG(INFO, "rpc return error code is %d msg is %s", result_msg.result_code_, result_msg.message_.ptr());
            }
          }
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "can not get thread buffer");
        ret = OB_ERROR;
      }
      return ret;

    }
    int OBRootWorker::cs_create_tablet(const common::ObServer& server, const common::ObRange& range, const int64_t mem_version)
    {
      static const int MY_VERSION = 1;
      int ret = OB_SUCCESS;
      ThreadSpecificBuffer::Buffer* my_buffer = my_thread_buffer.get_buffer();
      if (my_buffer != NULL)
      {
        my_buffer->reset();
        ObDataBuffer thread_buff(my_buffer->current(), my_buffer->remain());
        ret = range.serialize(thread_buff.get_data(), 
            thread_buff.get_capacity(), thread_buff.get_position());
        if (OB_SUCCESS == ret)
        {
          ret = common::serialization::encode_vi64(thread_buff.get_data(), 
              thread_buff.get_capacity(), thread_buff.get_position(), mem_version);
        }

        if (OB_SUCCESS == ret)
        {
          ret = client_manager.send_request(server, OB_CS_CREATE_TABLE, MY_VERSION, network_timeout_, thread_buff);
        }
        ObDataBuffer out_buffer(thread_buff.get_data(), thread_buff.get_position());
        if (ret == OB_SUCCESS) 
        {
          common::ObResultCode result_msg;
          ret = result_msg.deserialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
          if (ret == OB_SUCCESS)
          {
            ret = result_msg.result_code_;
            if (ret != OB_SUCCESS) 
            {
              TBSYS_LOG(INFO, "rpc return error code is %d msg is %s", result_msg.result_code_, result_msg.message_.ptr());
            }
          }
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "can not get thread buffer");
        ret = OB_ERROR;
      }
      return ret;
    }
    int OBRootWorker::ups_get_last_frozen_memtable_version(const common::ObServer& server, int64_t &last_version)
    {
      static const int MY_VERSION = 1;
      int ret = OB_SUCCESS;
      ThreadSpecificBuffer::Buffer* my_buffer = my_thread_buffer.get_buffer();
      if (my_buffer != NULL)
      {
        my_buffer->reset();
        ObDataBuffer thread_buff(my_buffer->current(), my_buffer->remain());

        if (OB_SUCCESS == ret)
        {
          ret = client_manager.send_request(server, OB_UPS_GET_LAST_FROZEN_VERSION, MY_VERSION, network_timeout_, thread_buff);
        }
        ObDataBuffer out_buffer(thread_buff.get_data(), thread_buff.get_position());
        if (ret == OB_SUCCESS) 
        {
          common::ObResultCode result_msg;
          ret = result_msg.deserialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
          if (ret == OB_SUCCESS)
          {
            ret = result_msg.result_code_;
            if (ret != OB_SUCCESS) 
            {
              TBSYS_LOG(INFO, "rpc return error code is %d msg is %s", result_msg.result_code_, result_msg.message_.ptr());
            }
          }
          if (OB_SUCCESS == ret)
          {
            ret = serialization::decode_vi64(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position(), &last_version);
            if (ret != OB_SUCCESS)
            {
              TBSYS_LOG(ERROR, "deserialize last_version failed:ret [%d]", ret);
            }
          }
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "can not get thread buffer");
        ret = OB_ERROR;
      }
      return ret;
    }

    ObRootLogManager* OBRootWorker::get_log_manager()
    {
      return &log_manager_;
    }

    ObRoleMgr* OBRootWorker::get_role_manager()
    {
      return &role_mgr_;
    }

    common::ThreadSpecificBuffer::Buffer* OBRootWorker::get_rpc_buffer() const
    {
      return my_thread_buffer.get_buffer();
    }

    int OBRootWorker::rt_ping(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(in_buff);
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;

      if (version != MY_VERSION)
      {
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else
      {
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "result message serialize failed, err: %d", ret);
        }
      }

      if (ret == OB_SUCCESS)
      {
        send_response(OB_PING_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
      }

      return ret;
    }

    int OBRootWorker::rt_slave_quit(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;

      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }

      ObServer rt_slave;
      if (ret == OB_SUCCESS)
      {
        ret = rt_slave.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "slave deserialize failed, err=%d", ret);
        }
      }

      if (ret == OB_SUCCESS)
      {
        ret = slave_mgr_.delete_server(rt_slave);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "ObSlaveMgr delete slave error, ret: %d", ret);
        }

        char addr_buf[ADDR_BUF_LEN];
        rt_slave.to_string(addr_buf, sizeof(addr_buf));
        addr_buf[ADDR_BUF_LEN - 1] = '\0';
        TBSYS_LOG(INFO, "slave quit, slave_addr=%s, err=%d", addr_buf, ret);
      }

      if (ret == OB_SUCCESS)
      {
        common::ObResultCode result_msg;
        result_msg.result_code_ = ret;

        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "result mssage serialize faild, err: %d", ret);
        }
      }

      if (ret == OB_SUCCESS)
      {
        send_response(OB_SLAVE_QUIT_RES, MY_VERSION, out_buff, conn, channel_id);
      }

      return ret;
    }
    
    int OBRootWorker::rt_update_server_report_freeze(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      int64_t frozen_version = 0;

      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }

      ObServer update_server;
      if (ret == OB_SUCCESS)
      {
        ret = update_server.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "deserialize failed, err=%d", ret);
        }
      }

      if (ret == OB_SUCCESS)
      {
        ret = serialization::decode_vi64(in_buff.get_data(),in_buff.get_capacity(),in_buff.get_position(),&frozen_version);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "decode frozen version error, ret: %d", ret);
        }
        else
        {
          TBSYS_LOG(INFO, "update report a new froze version : [%ld]",frozen_version);
        }
      }

      if (ret == OB_SUCCESS)
      {
        root_server_.report_frozen_memtable(frozen_version, false);
      }

      if (ret == OB_SUCCESS)
      {
        common::ObResultCode result_msg;
        result_msg.result_code_ = ret;

        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "result mssage serialize faild, err: %d", ret);
        }
      }

      if (ret == OB_SUCCESS)
      {
        send_response(OB_RESULT, MY_VERSION, out_buff, conn, channel_id);
      }

      return ret;
    }


    int OBRootWorker::rt_slave_register(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }

      uint64_t new_log_file_id = 0;
      ObServer rt_slave;
      if (ret == OB_SUCCESS)
      {
        ret = rt_slave.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "deserialize rt_slave failed, err=%d", ret);
        }
      }

      if (ret == OB_SUCCESS)
      {
        ret = log_manager_.add_slave(rt_slave, new_log_file_id);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "add_slave error, err=%d", ret);
        }
        else
        {
          char addr_buf[ADDR_BUF_LEN];
          rt_slave.to_string(addr_buf, sizeof(addr_buf));
          addr_buf[ADDR_BUF_LEN - 1] = '\0';
          TBSYS_LOG(INFO, "add slave, slave_addr=%s, new_log_file_id=%ld, ckpt_id=%lu, err=%d",
              addr_buf, new_log_file_id, log_manager_.get_check_point(), ret);
        }
      }

      if (ret == OB_SUCCESS)
      {
        int64_t lease_on = TBSYS_CONFIG.getInt(STR_ROOT_SECTION, STR_LEASE_ON, DEFAULT_LEASE_ON);
        if (LEASE_ON == lease_on)
        {
          ObLease lease;
          ret = slave_mgr_.extend_lease(rt_slave, lease);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "failed to extends lease, ret=%d", ret);
          }
        }
      }

      ObFetchParam fetch_param;
      if (ret == OB_SUCCESS)
      {
        fetch_param.fetch_log_ = true;
        fetch_param.min_log_id_ = log_manager_.get_replay_point();
        fetch_param.max_log_id_ = new_log_file_id - 1;

        if (log_manager_.get_check_point() > 0)
        {
          TBSYS_LOG(INFO, "master has check point, tell slave fetch check point files, id: %ld", log_manager_.get_check_point());
          fetch_param.fetch_ckpt_ = true;
          fetch_param.ckpt_id_ = log_manager_.get_check_point();
          fetch_param.min_log_id_ = fetch_param.ckpt_id_ + 1;
        }
        else
        {
          fetch_param.fetch_ckpt_ = false;
          fetch_param.ckpt_id_ = 0;
        }
      }

      common::ObResultCode result_msg;
      result_msg.result_code_ = ret;

      ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
      if (ret == OB_SUCCESS)
      {
        ret = fetch_param.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
      }

      if (ret == OB_SUCCESS)
      {
        send_response(OB_SLAVE_REG_RES, MY_VERSION, out_buff, conn, channel_id);
      }

      return ret;
    }

    int OBRootWorker::slave_register_(common::ObFetchParam& fetch_param)
    {
      int err = OB_SUCCESS;
      int64_t num_times = TBSYS_CONFIG.getInt(STR_ROOT_SECTION, STR_REGISTER_TIMES, DEFAULT_REGISTER_TIMES);
      int64_t timeout = TBSYS_CONFIG.getInt(STR_ROOT_SECTION, STR_REGISTER_TIMEOUT_US, DEFAULT_REGISTER_TIMEOUT_US);
      const ObServer& self_addr = self_addr_;

      err = OB_RESPONSE_TIME_OUT;
      for (int64_t i = 0; ObRoleMgr::STOP != role_mgr_.get_state()
          && OB_RESPONSE_TIME_OUT == err && i < num_times; ++i)
      {
        // slave register
        err = rt_rpc_stub_.slave_register(rt_master_, self_addr_, fetch_param, timeout);
        if (OB_RESPONSE_TIME_OUT == err)
        {
          TBSYS_LOG(INFO, "slave register timeout, i=%ld, err=%d", i, err);
        }
      }

      if (ObRoleMgr::STOP == role_mgr_.get_state())
      {
        TBSYS_LOG(INFO, "the slave is stopped manually.");
        err = OB_ERROR;
      }
      else if (OB_RESPONSE_TIME_OUT == err)
      {
        TBSYS_LOG(WARN, "slave register timeout, num_times=%ld, timeout=%ldus",
            num_times, timeout);
        err = OB_RESPONSE_TIME_OUT;
      }
      else if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "Error occurs when slave register, err=%d", err);
      }

      if (err == OB_SUCCESS)
      {
        int64_t renew_lease_timeout = 1000 * 1000L;
        check_thread_.set_renew_lease_timeout(renew_lease_timeout);
      }

      char addr_buf[ADDR_BUF_LEN];
      self_addr.to_string(addr_buf, sizeof(addr_buf));
      addr_buf[ADDR_BUF_LEN - 1] = '\0';
      TBSYS_LOG(INFO, "slave register, self=[%s], min_log_id=%ld, max_log_id=%ld, ckpt_id=%lu, err=%d",
          addr_buf, fetch_param.min_log_id_, fetch_param.max_log_id_, fetch_param.ckpt_id_, err);

      if (err == OB_SUCCESS)
      {
        is_registered_ = true;
      }

      return err;
    }

    int OBRootWorker::rt_renew_lease(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      static const int MY_VERSION = 1;
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }

      ObServer rt_slave;
      ObLease lease;
      if (ret == OB_SUCCESS)
      {
        ret = rt_slave.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "failed to deserialize root slave, ret=%d", ret);
        }
      }

      if (ret == OB_SUCCESS)
      {
        ret = slave_mgr_.extend_lease(rt_slave, lease);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "failed to extend lease, ret=%d", ret);
        }
      }

      common::ObResultCode result_msg;
      result_msg.result_code_ = ret;

      ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
      if (ret == OB_SUCCESS)
      {
        ret = lease.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "lease serialize failed, ret=%d", ret);
        }
      }

      if (ret == OB_SUCCESS)
      {
        send_response(OB_RENEW_LEASE_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
      }

      return ret;
    }

    int OBRootWorker::rt_grant_lease(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      static const int MY_VERSION = 1;
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }

      ObLease lease;
      if (ret == OB_SUCCESS)
      {
        ret = lease.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "failed to deserialize lease, ret=%d", ret);
        }
      }

      if (ret == OB_SUCCESS)
      {
        ret = check_thread_.renew_lease(lease);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "failed to extend lease, ret=%d", ret);
        }
      }

      common::ObResultCode result_msg;
      result_msg.result_code_ = ret;

      ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
      if (ret == OB_SUCCESS)
      {
        ret = lease.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "lease serialize failed, ret=%d", ret);
        }
      }

      if (ret == OB_SUCCESS)
      {
        send_response(OB_GRANT_LEASE_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
      }
      return ret;
    }

    int OBRootWorker::rt_slave_write_log(const int32_t version, common::ObDataBuffer& in_buffer, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buffer)
    {
      static const int MY_VERSION = 1;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      int64_t in_buff_begin = in_buffer.get_position();
      bool switch_log_flag = false;
      uint64_t log_id;
      static bool is_first_log = true;

      // send response to master ASAP
      ret = result_msg.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
      if (ret == OB_SUCCESS)
      {
        ret = send_response(OB_SEND_LOG_RES, MY_VERSION, out_buffer, conn, channel_id);
      }

      if (ret == OB_SUCCESS)
      {
        if (is_first_log)
        {
          is_first_log = false;

          ObLogEntry log_ent;
          ret = log_ent.deserialize(in_buffer.get_data(), in_buffer.get_limit(), in_buffer.get_position());
          if (ret != OB_SUCCESS)
          {
            common::hex_dump(in_buffer.get_data(), in_buffer.get_limit(), TBSYS_LOG_LEVEL_INFO);
            TBSYS_LOG(WARN, "ObLogEntry deserialize error, error code: %d, position: %ld, limit: %ld", ret, in_buffer.get_position(), in_buffer.get_limit());
          }
          else
          {
            if (OB_LOG_SWITCH_LOG != log_ent.cmd_)
            {
              TBSYS_LOG(WARN, "the first log of slave should be switch_log, cmd_=%d", log_ent.cmd_);
              ret = OB_ERROR;
            }
            else
            {
              ret = serialization::decode_i64(in_buffer.get_data(), in_buffer.get_limit(), in_buffer.get_position(), (int64_t*)&log_id);
              if (OB_SUCCESS != ret)
              {
                TBSYS_LOG(WARN, "decode_i64 log_id error, err=%d", ret);
              }
              else
              {
                ret = log_manager_.start_log(log_id);
                if (OB_SUCCESS != ret)
                {
                  TBSYS_LOG(WARN, "start_log error, log_id=%lu err=%d", log_id, ret);
                }
                else
                {
                  in_buffer.get_position() = in_buffer.get_limit();
                }
              }
            }
          }
        }
      } // end of first log

      if (ret == OB_SUCCESS)
      {
        int64_t data_begin = in_buffer.get_position();
        while (OB_SUCCESS == ret && in_buffer.get_position() < in_buffer.get_limit())
        {
          ObLogEntry log_ent;
          ret = log_ent.deserialize(in_buffer.get_data(), in_buffer.get_limit(), in_buffer.get_position());
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "ObLogEntry deserialize error, err=%d", ret);
            ret = OB_ERROR;
          }
          else
          {
            log_manager_.set_cur_log_seq(log_ent.seq_);

            // check switch_log
            if (OB_LOG_SWITCH_LOG == log_ent.cmd_)
            {
              if (data_begin != in_buff_begin
                  || ((in_buffer.get_position() + log_ent.get_log_data_len()) != in_buffer.get_limit()))
              {
                TBSYS_LOG(ERROR, "swith_log is not single, this should not happen, "
                    "in_buff.pos=%ld log_data_len=%d in_buff.limit=%ld",
                    in_buffer.get_position(), log_ent.get_log_data_len(), in_buffer.get_limit());
                ret = OB_ERROR;
              }
              else
              {
                ret = serialization::decode_i64(in_buffer.get_data(), in_buffer.get_limit(), in_buffer.get_position(), (int64_t*)&log_id);
                if (OB_SUCCESS != ret)
                {
                  TBSYS_LOG(WARN, "decode_i64 log_id error, err=%d", ret);
                }
                else
                {
                  switch_log_flag = true;
                }
              }
            }
            in_buffer.get_position() += log_ent.get_log_data_len();
          }
        }

        if (OB_SUCCESS == ret && in_buffer.get_limit() - data_begin > 0)
        {
          ret = log_manager_.store_log(in_buffer.get_data() + data_begin, in_buffer.get_limit() - data_begin);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "ObUpsLogMgr store_log error, err=%d", ret);
          }
        }

        if (switch_log_flag)
        {
          ret = log_manager_.switch_to_log_file(log_id + 1);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "switch_to_log_file error, log_id=%lu err=%d", log_id, ret);
          }
        }
      }

      return ret;
    }
  }
}


