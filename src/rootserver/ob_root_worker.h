/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_root_worker.h for ...
 *
 * Authors:
 *   daoan <daoan@taobao.com>
 *
 */

#ifndef OCEANBASE_ROOTSERVER_ROOT_WORKER_H_
#define OCEANBASE_ROOTSERVER_ROOT_WORKER_H_
#include "common/ob_define.h"
#include "common/ob_base_server.h"
#include "common/thread_buffer.h"
#include "common/ob_client_manager.h"
#include "common/ob_fetch_runnable.h"
#include "common/ob_role_mgr.h"
#include "common/ob_slave_mgr.h"
#include "common/ob_check_runnable.h"
#include "common/ob_packet_queue_thread.h"
#include "rootserver/ob_root_server2.h"
#include "rootserver/ob_root_rpc_stub.h"
#include "rootserver/ob_root_log_replay.h"
#include "rootserver/ob_root_log_manager.h"
#include "rootserver/ob_root_stat.h"
#include "rootserver/ob_root_fetch_thread.h"

namespace oceanbase 
{ 
  namespace common
  {
    class ObDataBuffer;
    class ObServer;
    class ObScanner;
    class ObTabletReportInfoList;
    class ObGetParam;
    class ObScanParam;
    class ObRange;
  }
  namespace rootserver
  {
    class OBRootWorker :public oceanbase::common::ObBaseServer, public tbnet::IPacketQueueHandler
    {
      public:
        OBRootWorker();
        ~OBRootWorker();
        tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Connection *connection, tbnet::Packet *packet);
        bool handleBatchPacket(tbnet::Connection *connection, tbnet::PacketQueue &packetQueue);
        /** packet queue handler */
        bool handlePacketQueue(tbnet::Packet *packet, void *args);
        int initialize();
        int start_service();
        void wait_for_queue();
        void destroy();
        //bool start_report();
        bool start_merge();
        int set_config_file_name(const char* conf_file_name);
        bool is_master() const;
        void dump_root_table() const;
        void dump_available_server() const;
        void use_new_schema();
        void reload_config();
        void drop_current_merge();

      public:
        virtual int up_switch_schema(const common::ObServer& server, common::ObSchemaManagerV2* schema_manager);
        virtual int up_freeze_mem(const common::ObServer& server, int64_t& frozen_mem_version);
        virtual int cs_start_merge(const common::ObServer& server, const int64_t frozen_mem_version, const int32_t init_flag);
        virtual int unload_old_table(const common::ObServer& server, const int64_t frozen_mem_version);
        virtual int hb_to_cs(const common::ObServer& server, const int64_t lease_time,
                             const int64_t frozen_mem_version);

        int hb_to_ms(const common::ObServer& server,const int64_t lease_time,const int64_t schema_version);

        virtual int cs_migrate(const common::ObRange& range, 
            const common::ObServer& src_server, const common::ObServer& dest_server, bool keep_src);
        virtual int cs_create_tablet(const common::ObServer& server, const common::ObRange& range, const int64_t version);
        virtual int ups_get_last_frozen_memtable_version(const common::ObServer& server, int64_t &last_version);
      private:
        // notice that in_buff can not be const.
        int rt_get_update_server_info(const int32_t version, common::ObDataBuffer& in_buff, 
            tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff,
            bool use_inner_port = false);
        
        int rt_get_merge_delay_interval(const int32_t version, common::ObDataBuffer& in_buff,
            tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

        int rt_get(const int32_t version, common::ObDataBuffer& in_buff, 
            tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

        int rt_scan(const int32_t version, common::ObDataBuffer& in_buff, 
            tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

        int rt_fetch_schema(const int32_t version, common::ObDataBuffer& in_buff, 
            tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

        int rt_fetch_schema_version(const int32_t version, common::ObDataBuffer& in_buff, 
            tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

        int rt_report_tablets(const int32_t version, common::ObDataBuffer& in_buff, 
            tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

        int rt_waiting_job_done(const int32_t version, common::ObDataBuffer& in_buff, 
            tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

        int rt_register(const int32_t version, common::ObDataBuffer& in_buff, 
            tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

        int rt_migrate_over(const int32_t version, common::ObDataBuffer& in_buff, 
            tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

        int rt_report_capacity_info(const int32_t version, common::ObDataBuffer& in_buff, 
            tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

        int rt_heartbeat(const int32_t version, common::ObDataBuffer& in_buff, 
            tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

        int rt_dump_cs_info(const int32_t version, common::ObDataBuffer& in_buff, 
            tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

        int rt_fetch_stats(const int32_t version, common::ObDataBuffer& in_buff, 
            tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

        int rt_ping(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

        int rt_slave_quit(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

        int rt_update_server_report_freeze(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

        int rt_slave_register(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_renew_lease(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
        int rt_grant_lease(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

      private:
        int get(const common::ObGetParam& get_param, common::ObScanner& scanner);
        int scan(const common::ObScanParam& scan_param, common::ObScanner& scanner);

      public:
        ObRootLogManager* get_log_manager();
        common::ObRoleMgr* get_role_manager();
        common::ThreadSpecificBuffer::Buffer* get_rpc_buffer() const;

      private:
        int start_as_master();
        int start_as_slave();
        int get_ups_frozen_memtable_version();
        int slave_register_(common::ObFetchParam& fetch_param);

      private:
        int rt_slave_write_log(const int32_t version, common::ObDataBuffer& in_buffer, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buffer);

      protected:
        bool is_registered_;
        char config_file_name_[common::OB_MAX_FILE_NAME_LENGTH];
        ObRootServer2 root_server_;
        common::ObPacketQueueThread read_thread_queue_;
        common::ObPacketQueueThread write_thread_queue_;
        common::ObPacketQueueThread log_thread_queue_;
        int task_read_queue_size_;
        int task_write_queue_size_;
        int task_log_queue_size_;
        int64_t network_timeout_;
        common::ThreadSpecificBuffer my_thread_buffer;
        common::ObClientManager client_manager;

        common::ObServer rt_master_;
        common::ObServer self_addr_;
        common::ObRoleMgr role_mgr_;
        common::ObSlaveMgr slave_mgr_;
        common::ObCheckRunnable check_thread_;
        ObRootFetchThread fetch_thread_;
        
        ObRootRpcStub rt_rpc_stub_;
        ObRootLogReplay log_replay_thread_;
        ObRootLogManager log_manager_;
        ObRootStatManager stat_manager_;
    };
  }
}

#endif


