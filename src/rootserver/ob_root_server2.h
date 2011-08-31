/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_root_server2.h for ...
 *
 * Authors:
 *   daoan <daoan@taobao.com>
 *
 */

#ifndef OCEANBASE_ROOTSERVER_OB_ROOT_SERVER2_H_
#define OCEANBASE_ROOTSERVER_OB_ROOT_SERVER2_H_
#include <tbsys.h>

#include "common/ob_define.h"
#include "common/ob_server.h"
#include "common/ob_string.h"
#include "rootserver/ob_chunk_server_manager.h"
#include "rootserver/ob_root_table2.h"
#include "rootserver/ob_batch_migrate_info.h"
#include "rootserver/ob_balance_candidate_server_manager2.h"
#include "rootserver/ob_root_log_worker.h"

namespace oceanbase 
{ 
  namespace common
  {
    class ObSchemaManagerV2;
    class ObRange;
    class ObTabletInfo;
    class ObTabletLocation;
    class ObScanner;
    class ObCellInfo;
    class ObTabletReportInfoList;
  }
  namespace rootserver 
  {
    class ObRootTable2;
    class ObRootServerTester;
    class OBRootWorker;
    class ObRootServer2
    {
      public:
        static const int STATUS_INIT         = 0;
        static const int STATUS_NEED_REPORT  = 1;
        static const int STATUS_NEED_BUILD   = 2;
        static const int STATUS_CHANGING     = 3;
        static const int STATUS_NEED_BALANCE = 4;
        static const int STATUS_BALANCING    = 5;
        static const int STATUS_SLEEP        = 6;
        static const int STATUS_INTERRUPT_BALANCING    = 7;
        static const char* ROOT_TABLE_EXT;
        static const char* CHUNKSERVER_LIST_EXT;
        enum 
        {
          BUILD_FOR_SWITCH = 0,
          BUILD_FOR_REPORT,
        };
        enum 
        {
          BUILD_SYNC_FLAG_NONE = 0,
          BUILD_SYNC_INIT_OK = 1,
          BUILD_SYNC_FLAG_FREEZE_MEM = 2,
          BUILD_SYNC_FLAG_CAN_ACCEPT_NEW_TABLE = 3,
          BUILD_SYNC_FLAG_CS_START_MERGE_OK = 4,
        };
        ObRootServer2();
        virtual ~ObRootServer2();

        bool init(const char* config_file_name, const int64_t now, OBRootWorker* worker);
        void start_threads();
        bool reload_config(const char* config_file_name);
        bool start_switch();
        void drop_current_build();
        /*
         * 从本地读取新schema, 判断兼容性
         */
        int switch_schema(const int64_t time_stamp);
        /*
         * 切换过程中, update server冻结内存表 或者chunk server 进行merge等耗时操作完成
         * 发送消息调用此函数
         */
        int waiting_job_done(const common::ObServer& server, const int64_t frozen_mem_version);

        /*
         * chunk serve和merege server注册
         * @param out status 0 do not start report 1 start report
         */
        int regist_server(const common::ObServer& server, bool is_merge_server, int32_t& status, int64_t time_stamp = -1);
        /*
         * chunk server更新自己的磁盘情况信息
         */
        int update_capacity_info(const common::ObServer& server, const int64_t capacity, const int64_t used);
        /*
         * 迁移完成操作
         */
        virtual int migrate_over(const common::ObRange& range, const common::ObServer& src_server, const common::ObServer& dest_server, const bool keep_src, const int64_t tablet_version);
        bool get_schema(common::ObSchemaManagerV2& out_schema) const;
        int64_t get_schema_version() const;

        int find_root_table_key(const uint64_t table_id, const common::ObString& table_name, const int32_t max_key_len, const common::ObString& key, common::ObScanner& scanner) const;
        int find_root_table_key(const common::ObString& table_name, const common::ObString& key, common::ObScanner& scanner) const;
        int find_root_table_key(const uint64_t table_id, const common::ObString& key, common::ObScanner& scanner) const;
        int find_root_table_range(const common::ObString& table_name, const common::ObRange& key_range, 
            common::ObScanner& scanner) const;

        //this will be called when a server echo rt_start_merge
        int echo_start_merge_received(const common::ObServer& server);
        int echo_update_server_freeze_mem();
        int echo_unload_received(const common::ObServer& server);
        virtual int report_tablets(const common::ObServer& server, const common::ObTabletReportInfoList& tablets, const int64_t time_stamp);

        int receive_hb(const common::ObServer& server,common::ObRole role);
        common::ObServer get_update_server_info() const;
        int32_t get_update_server_inner_port() const;
        int64_t get_merge_delay_interval() const;

        uint64_t get_table_info(const common::ObString& table_name, int32_t& max_row_key_length) const;
        int get_table_info(const uint64_t table_id, common::ObString& table_name, int32_t& max_row_key_length) const;

        int get_server_status() const;
        int64_t get_time_stamp_changing() const;
        int64_t get_lease() const;
        int get_server_index(const common::ObServer& server) const;

        int get_cs_info(ObChunkServerManager* out_server_manager) const;

        void print_alive_server() const;
        bool is_master() const;
        void dump_root_table() const;
        void reload_config();
        void use_new_schema();
        int do_check_point(const uint64_t ckpt_id); // dump current root table and chunkserver list into file
        int recover_from_check_point(const int server_status, const uint64_t ckpt_id); // recover root table and chunkserver list from file
      int report_frozen_memtable(const int64_t frozen_version, bool did_replay);
      // 用于slave启动过程中的同步
      void wait_init_finished();
      
        friend class ObRootServerTester;
        friend class ObRootLogWorker;
      private:
        /*
         * 收到汇报消息后调用
         */
        int got_reported(const common::ObTabletReportInfoList& tablets, const int server_index, const int64_t frozen_mem_version);
        
        int update_server_freeze_mem(int64_t& schema_time_stamp);
        int cs_start_merge(const int64_t frozen_mem_version, const int32_t init_flag);
        //balance happended when root table is ok, so balance only do some modify that never change the range order.
        void do_balance();
        //add_lost_tablet will add some tablet's copy by migrate
        void add_lost_tablet();
        /*
         * 系统初始化的时候, 处理汇报消息, 
         * 信息放到root table for build 结构中
         */
        int got_reported_for_build(const common::ObTabletInfo& tablet, 
            const int32_t server_index, const int64_t version);
        /*
         * 处理汇报消息, 直接写到当前的root table中
         * 如果发现汇报消息中有对当前root table的tablet的分裂或者合并
         * 要调用采用写拷贝机制的处理函数
         */
        int got_reported_for_query_table(const common::ObTabletReportInfoList& tablets, 
            const int32_t server_index, const int64_t frozen_mem_version);
        /*
         * 写拷贝机制的,处理汇报消息
         */
        int got_reported_with_copy(const common::ObTabletReportInfoList& tablets, 
            const int32_t server_index, const int64_t have_done_index);

        void create_new_table(common::ObSchemaManagerV2* schema);
        void create_new_table_in_init(common::ObSchemaManagerV2* schema,ObRootTable2* root_table_tmp);
        void create_new_table();
      int slave_create_new_table(const common::ObTabletInfo& tablet, const int32_t* t_server_index, const int32_t replicas_num, const int64_t mem_version);
        void get_available_servers_for_new_table(int* server_index, int32_t expected_num, int32_t &results_num);
        /*
         * 生成查询的输出cell
         */
        int make_out_cell(common::ObCellInfo& out_cell, ObRootTable2::const_iterator start, 
            ObRootTable2::const_iterator end, common::ObScanner& scanner, const int32_t max_row_count,
            const int32_t max_key_len) const;

      // @return 0 do not copy, 1 copy immediately, -1 delayed copy
      int need_copy(int32_t available_num, int32_t lost_num);
      int make_checkpointing();
      private:
        int init_root_table_by_report();
        bool start_report(bool init_flag = false);
        void execute_batch_migrate();
        /*
         * 当某tablet的备份数不足时候, 补足备份
         */
        int add_copy(const ObRootTable2* root_table, ObRootTable2::iterator& it, 
                     const int64_t monotonic_now,const int64_t last_version);
        /*
         * 需要从某server移出tablet以减轻其压力
         */
        int move_out_tablet(ObServerStatus* migrate_server, const int64_t monotonic_now);
        /*
         * 在一个tabelt的各份拷贝中, 寻找合适的备份替换掉
         */
        int write_new_info_to_root_table(
            const common::ObTabletInfo& tablet_info, const int64_t tablet_version, const int32_t server_index, 
            ObRootTable2::const_iterator& first, ObRootTable2::const_iterator& last, ObRootTable2 *p_root_table);

        bool all_tablet_is_the_last_frozen_version() const;

        DISALLOW_COPY_AND_ASSIGN(ObRootServer2);

      private:
        ObChunkServerManager server_manager_;
        mutable tbsys::CRWLock server_manager_rwlock_;

        ObServerStatus update_server_status_;
        int32_t ups_inner_port_;
        int64_t lease_duration_;
        common::ObSchemaManagerV2* schema_manager_; 
        //this one will protected schemaManager
        //actually schemaManager is accessed only in a schema change process
        mutable tbsys::CRWLock schema_manager_rwlock_;

        int64_t time_stamp_changing_;
        int64_t frozen_mem_version_;

        mutable tbsys::CThreadMutex root_table_build_mutex_; //any time only one thread can modify root_table
        //ObRootTable one for query 
        //another for receive reporting and build new one
        ObRootTable2* root_table_for_query_; 
        ObTabletInfoManager* tablet_manager_for_query_;
        mutable tbsys::CRWLock root_table_rwlock_; //every query root table should rlock this

        ObRootTable2* root_table_for_build_; 
        ObTabletInfoManager* tablet_manager_for_build_;

        char config_file_name_[common::OB_MAX_FILE_NAME_LENGTH];
        char schema_file_name_[common::OB_MAX_FILE_NAME_LENGTH];
        bool have_inited_;
        mutable bool new_table_created_;        

        int migrate_wait_seconds_;

        ObBatchMigrateInfoManager batch_migrate_helper_;

        int server_status_;
        mutable int build_sync_flag_;
        mutable tbsys::CThreadMutex status_mutex_; 
        ObBalancePrameter balance_prameter_;
        ObCandidateServerBySharedManager2 candidate_shared_helper_;
        ObCandidateServerByDiskManager candidate_disk_helper_;
        int64_t safe_lost_one_duration_;
        int64_t wait_init_time_;
        int64_t max_merge_duration_;
        int64_t cs_merge_command_interval_mseconds_;
        bool first_cs_had_registed_;
        bool receive_stop_;
        bool drop_this_build_;
        bool drop_last_cs;
        int32_t safe_copy_count_in_init_;
        int32_t safe_copy_count_in_merge_;
        int32_t create_table_in_init_;

      private:
        int64_t last_frozen_mem_version_;
        int64_t pre_frozen_mem_version_;
        int64_t last_frozen_time_;

      int32_t tablet_replicas_num_;
      static const int32_t DEFAULT_TABLET_REPLICAS_NUM = 3;

      private:
        OBRootWorker* worker_;//who does the net job
        ObRootLogWorker* log_worker_;

      protected:
        class rootTableModifier : public tbsys::CDefaultRunnable
      {
        public:
          explicit rootTableModifier(ObRootServer2* root_server);
          void run(tbsys::CThread *thread, void *arg);
        private:
          ObRootServer2* root_server_;
      };//report, switch schema and balance job
        rootTableModifier root_table_modifier_;
        //friend class rootTableModifier;  // g++ need't this
        class balanceWorker : public tbsys::CDefaultRunnable
      {
        public:
          explicit balanceWorker(ObRootServer2* root_server);
          void run(tbsys::CThread *thread, void *arg);
        private:
          ObRootServer2* root_server_;
      };
        balanceWorker balance_worker_;

        class heartbeatChecker : public tbsys::CDefaultRunnable
      {
        public:
          explicit heartbeatChecker(ObRootServer2* root_server);
          void run(tbsys::CThread *thread, void *arg);
        private:
          ObRootServer2* root_server_;
      };
        heartbeatChecker heart_beat_checker_;

    };

  }
}

#endif
