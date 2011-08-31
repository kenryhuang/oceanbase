/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * priority_packet_queue_thread.cpp for ...
 *
 * Authors:
 *   rizhao <rizhao.ych@taobao.com>
 *
 */
#include "priority_packet_queue_thread.h"

using namespace tbnet;

namespace oceanbase {
namespace common {

// 构造
PriorityPacketQueueThread::PriorityPacketQueueThread() : tbsys::CDefaultRunnable()
{
  _stop = 0;
  _waitFinish = false;
  _handler = NULL;
  _args = NULL;
  _waitTime[NORMAL_PRIV] = DEF_WAIT_TIME_MS;
  _waitTime[LOW_PRIV] = 0;
  for (int64_t i = 0; i < QUEUE_NUM; ++i)
  {
    _waiting[i] = false;
    _queues[i].init();
  }
}

// 构造
PriorityPacketQueueThread::PriorityPacketQueueThread(int threadCount, IPacketQueueHandler *handler, void *args)
        : tbsys::CDefaultRunnable(threadCount)
{
  _stop = 0;
  _waitFinish = false;
  _handler = handler;
  _args = args;
  _waitTime[NORMAL_PRIV] = DEF_WAIT_TIME_MS;
  _waitTime[LOW_PRIV] = 0;
  for (int64_t i = 0; i < QUEUE_NUM; ++i)
  {
    _waiting[i] = false;
  }
}

// 析构
PriorityPacketQueueThread::~PriorityPacketQueueThread()
{
    stop();
}

// 线程参数设置
void PriorityPacketQueueThread::setThreadParameter(int threadCount, IPacketQueueHandler *handler, void *args)
{
    setThreadCount(threadCount);
    _handler = handler;
    _args = args;
}

// stop
void PriorityPacketQueueThread::stop(bool waitFinish)
{
    _waitFinish = waitFinish;
    _stop = true;
}

// push
// block==true, this thread can wait util _queue.size less than maxQueueLen
// otherwise, return false directly, client must be free this packet.
bool PriorityPacketQueueThread::push(ObPacket *packet, int maxQueueLen, bool block, int priority)
{
  // invalid param
  if (NULL == packet)
  {
    return true;
  }
  else if (priority != NORMAL_PRIV && priority != LOW_PRIV)
  {
    return true;
  }

  // 是停止就不允许放了
  if (_stop) {
    return true;
  }

  // 是否要限制push长度
  if (maxQueueLen>0 && _queues[priority].size() >= maxQueueLen)
  {
    _pushcond[priority].lock();
    _waiting[priority] = true;
    while (_stop == false && _queues[priority].size() >= maxQueueLen && block)
    {
      _pushcond[priority].wait(1000);
    }
    _waiting[priority] = false;
    if (_queues[priority].size() >= maxQueueLen && !block)
    {
      _pushcond[priority].unlock();
      return false;
    }
    _pushcond[priority].unlock();

    if (_stop)
    {
      return true;
    }
  }

  // 加锁写入队列
  _cond[priority].lock();
  _queues[priority].push(packet);
  _cond[priority].unlock();
  _cond[priority].signal();
  return true;
}

// Runnable 接口
void PriorityPacketQueueThread::run(tbsys::CThread *, void *)
{
  Packet *packet = NULL;
  int64_t priority = NORMAL_PRIV;
  int64_t continous_normal_task = 0;

  while (!_stop)
  {
    _cond[priority].lock();
    if (!_stop && _queues[priority].size() == 0 && _waitTime[priority] > 0)
    {
      _cond[priority].wait(_waitTime[priority]);
    }
    if (_stop)
    {
      _cond[priority].unlock();
      break;
    }

    // 取出packet
    packet = _queues[priority].pop();
    _cond[priority].unlock();

    // push 在等吗?
    if (_waiting)
    {
      _pushcond[priority].lock();
      _pushcond[priority].signal();
      _pushcond[priority].unlock();
    }

    bool has_task = true;
    // 空的packet?
    if (packet == NULL)
    {
      has_task = false;
    }
    else
    {
      bool ret = true;
      if (_handler)
      {
        ret = _handler->handlePacketQueue(packet, _args);
      }
      // 如果返回false, 不删除
      //if (ret) delete packet;
    }

    //TBSYS_LOG(DEBUG, "HANDLE task priority=%d has_task=%s", priority, has_task ? "true" : "false");
    // switch task queue if necessary
    if (LOW_PRIV == priority)
    {
      continous_normal_task = 0;
      priority = NORMAL_PRIV;
    }
    else
    {
      ++continous_normal_task;
      if (!has_task || continous_normal_task >= CONTINOUS_NORMAL_TASK_NUM)
      {
        priority = LOW_PRIV;
      }
    }
  }

  // 把queue中所有的task做完
  if (_waitFinish)
  {
    for (int64_t priority = NORMAL_PRIV; priority <= LOW_PRIV; ++priority)
    {
      bool ret = true;
      _cond[priority].lock();
      while (_queues[priority].size() > 0) {
        packet = _queues[priority].pop();
        _cond[priority].unlock();
        ret = true;
        if (_handler) {
          ret = _handler->handlePacketQueue(packet, _args);
        }
        //if (ret) delete packet;

        _cond[priority].lock();
      }
      _cond[priority].unlock();
    }
  }
  else
  {
    // 把queue中的free掉
    for (int64_t priority = NORMAL_PRIV; priority <= LOW_PRIV; ++priority)
    {
      _cond[priority].lock();
      while (_queues[priority].size() > 0) {
        _queues[priority].pop();
      }
      _cond[priority].unlock();
    }
  }
}

void PriorityPacketQueueThread::setWaitTime(int normal_priv_ms, int low_priv_ms)
{
  int tmp_normal_priv_ms = normal_priv_ms;
  int tmp_low_priv_ms = low_priv_ms;

  if (tmp_normal_priv_ms > 0 && tmp_low_priv_ms >= 0)
  {
    if (tmp_normal_priv_ms > MAX_WAIT_TIME_MS)
    {
      tmp_normal_priv_ms = MAX_WAIT_TIME_MS;
    }

    if (tmp_low_priv_ms > MAX_WAIT_TIME_MS)
    {
      tmp_low_priv_ms = MAX_WAIT_TIME_MS;
    }

    _waitTime[NORMAL_PRIV] = tmp_normal_priv_ms;
    _waitTime[LOW_PRIV] = tmp_low_priv_ms;
  }
}

void PriorityPacketQueueThread::getWaitTime(int& normal_priv_ms, int& low_priv_ms)
{
  normal_priv_ms = _waitTime[NORMAL_PRIV];
  low_priv_ms = _waitTime[LOW_PRIV];
}

}
}


