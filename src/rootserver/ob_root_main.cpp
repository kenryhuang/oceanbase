/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_root_main.cpp for ...
 *
 * Authors:
 *   zhuweng <zhuweng.yzf@taobao.com>
 *
 */

#include "rootserver/ob_root_main.h"
#include "rootserver/ob_root_signal.h"
namespace 
{
    const char* STR_ROOT_SECTION = "root_server";
    const char* STR_DEV_NAME = "dev_name";
    const char* STR_LISTEN_PORT = "port";
}

const char* svn_version();
const char* build_date();
const char* build_time();

namespace oceanbase 
{ 
  using common::OB_SUCCESS;
  using common::OB_ERROR;
  namespace rootserver
  { 
    ObRootMain::ObRootMain()
    {
    }
    common::BaseMain* ObRootMain::get_instance()
    {
      if (instance_ == NULL)
      {
        instance_ = new ObRootMain();
      }
      return instance_;
    }

void ObRootMain::print_version()
{
  fprintf(stderr, "rootserver (%s)\n", PACKAGE_STRING);
  fprintf(stderr, "SVN_VERSION: %s\n", svn_version());
  fprintf(stderr, "BUILD_TIME: %s %s\n\n", build_date(), build_time());
}

    int ObRootMain::do_work()
    {
      //add signal I want to catch
      //add_signal_catched(START_REPORT_SIG);
      add_signal_catched(START_MERGE_SIG);
      add_signal_catched(DUMP_ROOT_TABLE_TO_LOG);
      add_signal_catched(DUMP_AVAILABLE_SEVER_TO_LOG);
      add_signal_catched(SWITCH_SCHEMA);
      add_signal_catched(RELOAD_CONFIG);
      add_signal_catched(DO_CHECK_POINT);
      add_signal_catched(DROP_CURRENT_MERGE);
      int ret = OB_SUCCESS;
      int port = TBSYS_CONFIG.getInt(STR_ROOT_SECTION, STR_LISTEN_PORT, 0);
      ret = worker.set_listen_port(port);
      if (ret == OB_SUCCESS)
      {
        const char *dev_name = TBSYS_CONFIG.getString(STR_ROOT_SECTION, STR_DEV_NAME, NULL);
        ret = worker.set_dev_name(dev_name);
      }
      if (ret == OB_SUCCESS)
      {
        ret = worker.set_config_file_name(config_file_name_);
      }
      if (ret == OB_SUCCESS)
      {
        worker.set_packet_factory(&packet_factory_);
      }
      if (ret == OB_SUCCESS)
      {
        ret = worker.start();
      }
      return ret;
    }
    void ObRootMain::do_signal(const int sig)
    {
      bool ret = false;
      switch(sig)
      {
        case SIGTERM:
        case SIGINT:
          TBSYS_LOG(INFO, "stop signal received");
          worker.stop();
          break;
       // case START_REPORT_SIG:
       //   if (worker.is_master())
       //   {
       //     TBSYS_LOG(INFO, "start_report signal received");
       //     ret = worker.start_report(); 
       //     if (ret)
       //       TBSYS_LOG(INFO, "start_report signal ok");
       //     else
       //       TBSYS_LOG(INFO, "start_report signal fail");
       //   }
       //   else
       //   {
       //     TBSYS_LOG(INFO, "only master can take signal report");
       //   }

       //   break;
        case START_MERGE_SIG:
          if (worker.is_master())
          {
            TBSYS_LOG(INFO, "start_merge signal received");
            ret = worker.start_merge();
            if (ret)
              TBSYS_LOG(INFO, "start_merge signal ok");
            else
              TBSYS_LOG(INFO, "start_merge signal fail");
          }
          else
          {
            TBSYS_LOG(INFO, "only master can take signal merge");
          }
          break;
        case DROP_CURRENT_MERGE:
          if (worker.is_master())
          {
            TBSYS_LOG(INFO, "DROP_CURRENT_MERGE signal received");
            worker.drop_current_merge();
          }
          else
          {
            TBSYS_LOG(INFO, "only master can take signal DROP_CURRENT_MERGE");
          }
          break;
        case DUMP_ROOT_TABLE_TO_LOG:
          TBSYS_LOG(INFO, "DUMP_ROOT_TABLE_TO_LOG signal received");
          worker.dump_root_table();
          break;
        case DUMP_AVAILABLE_SEVER_TO_LOG:
          TBSYS_LOG(INFO, "DUMP_AVAILABLE_SEVER_TO_LOG signal received");
          worker.dump_available_server();
          break;
        case SWITCH_SCHEMA:
          TBSYS_LOG(INFO, "SWITCH_SCHEMA signal received");
          worker.use_new_schema();
          break;
        case RELOAD_CONFIG:
          TBSYS_LOG(INFO, "RELOAD_CONFIG signal received");
          worker.reload_config();
          break;
        case DO_CHECK_POINT:
          TBSYS_LOG(INFO, "DO_CHECK_POINT signal received");
          if (worker.is_master()) {
            int ret = worker.get_log_manager()->do_check_point();
            TBSYS_LOG(INFO, "do check point return: %d", ret);
          } else
          {
            TBSYS_LOG(INFO, "I am slave, operate rejected");
          }
          break;

        default:
          break;
      }
    }
  }
}

