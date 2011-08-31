/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * priority_packet_queue_thread.h for ...
 *
 * Authors:
 *   rizhao <rizhao.ych@taobao.com>
 *
 */
#ifndef OCEANBASE_COMMON_PRIORITY_PACKET_QUEUE_THREAD_H
#define OCEANBASE_COMMON_PRIORITY_PACKET_QUEUE_THREAD_H

#include "tbnet.h"
#include "ob_packet.h"
#include "ob_packet_queue.h"

namespace oceanbase {
namespace common {

class PriorityPacketQueueThread : public tbsys::CDefaultRunnable
{
public:
  enum QueuePriority {
    NORMAL_PRIV = 0,
    LOW_PRIV = 1,
  };

public:
    // 构造
  PriorityPacketQueueThread();

  // 构造
  PriorityPacketQueueThread(int threadCount, tbnet::IPacketQueueHandler *handler, void *args);

  // 析构
  ~PriorityPacketQueueThread();

  // 参数设置
  void setThreadParameter(int threadCount, tbnet::IPacketQueueHandler *handler, void *args);

  // stop
  void stop(bool waitFinish = false);

  // push
  bool push(ObPacket *packet, int maxQueueLen = 0, bool block = true, int priority = NORMAL_PRIV);

  // Runnable 接口
  void run(tbsys::CThread *thread, void *arg);

  // Sets wait time for normal and low priority queue.
  void setWaitTime(int normal_priv_wait_ms, int low_priv_wait_ms = 0);

  void getWaitTime(int& normal_priv_wait_ms, int& low_priv_wait_ms);

  tbnet::Packet* head(int priority)
  {
    return _queues[priority].head();
  }

  tbnet::Packet* tail(int priority)
  {
    return _queues[priority].tail();
  }

  size_t size(int priority)
  {
    return _queues[priority].size();
  }

  size_t size()
  {
    return _queues[NORMAL_PRIV].size() + _queues[LOW_PRIV].size();
  }
    
private:
  static const int64_t QUEUE_NUM = 2;
  static const int64_t DEF_WAIT_TIME_MS = 10;   // 10ms
  static const int64_t MAX_WAIT_TIME_MS = 1000; // 1000ms
  // The work thread fetches several(3, by default) normal tasks and then try to fetch a low priv task
  static const int64_t CONTINOUS_NORMAL_TASK_NUM = 3;

private:
  ObPacketQueue _queues[QUEUE_NUM];
  tbnet::IPacketQueueHandler *_handler;
  tbsys::CThreadCond _cond[QUEUE_NUM];
  tbsys::CThreadCond _pushcond[QUEUE_NUM];
  void *_args;
  bool _waitFinish;       // 等待完成

  bool _waiting[QUEUE_NUM];
  int _waitTime[QUEUE_NUM];
};

}
}

#endif


