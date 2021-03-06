/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * mock_root_server.cpp for ...
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *
 */
#include "common/ob_result.h"
#include "common/ob_scanner.h"
#include "common/ob_read_common_data.h"
#include "common/ob_schema.h"
#include "mock_root_server.h"

using namespace oceanbase::common;
using namespace oceanbase::updateserver;

static const int32_t UPS_PORT = 2302;

int MockRootServer::initialize()
{
  set_listen_port(ROOT_SERVER_PORT);
  return MockServer::initialize();
}


ObPacket* MockRootServer::do_request(ObPacket* base_packet, bool &is_done)
{
  int ret = OB_SUCCESS;
  ObPacket* ob_packet = base_packet;
  int32_t packet_code = ob_packet->get_packet_code();
  ret = ob_packet->deserialize();
  if (OB_SUCCESS == ret)
  {   
    switch (packet_code)
    {
    case OB_WAITING_JOB_DONE:
      {
        ret = handle_waiting_job_done(ob_packet);
        break;
      }
    case OB_SERVER_REGISTER:
      {
        ret = handle_register_server(ob_packet);
        break;
      }
    case OB_FETCH_SCHEMA:
      {
        ret = handle_fetch_schema(ob_packet);
        break;
      }
    case OB_SCAN_REQUEST:
      {
        ret = handle_scan_root(ob_packet);
        break;
      }
    case OB_GET_UPDATE_SERVER_INFO:
      {
        ret = handle_get_updater(ob_packet);
        break;
      }
    default:
      {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "wrong packet code");
        break;
      }
    }
    //
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "check handle failed:ret[%d]", ret);
    }
  }
  return NULL;
}


int MockRootServer::handle_get_updater(ObPacket * ob_packet)
{
  int ret = OB_SUCCESS;
  ObDataBuffer* data = ob_packet->get_buffer();
  if (NULL == data)
  {
    ret = OB_ERROR;
  }
  
  tbnet::Connection* connection = ob_packet->get_connection();
  ThreadSpecificBuffer::Buffer* thread_buffer = response_packet_buffer_.get_buffer();
  if (NULL == thread_buffer)
  {
    ret = OB_ERROR;
  }
  else
  {
    thread_buffer->reset();
    ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());
    
    ObResultCode result_msg;
    result_msg.result_code_ = ret;
    ret = result_msg.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
    
    // new version
    int32_t channel_id = ob_packet->getChannelId();
    ObServer server;
    server.set_ipv4_addr("localhost", UPS_PORT); 
    ret = server.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
    ret = send_response(OB_REPORT_TABLETS_RESPONSE, 1, out_buffer, connection, channel_id);
  }
  return ret;
}

int MockRootServer::handle_waiting_job_done(ObPacket * ob_packet)
{
  int ret = OB_SUCCESS;
  ObDataBuffer* data = ob_packet->get_buffer();
  if (NULL == data)
  {
    ret = OB_ERROR;
  }

  ObServer ups_master;
  if (OB_SUCCESS == ret)
  {
    ret = ups_master.deserialize(data->get_data(), data->get_capacity(), data->get_position());
  }
  
  int64_t timestamp = 0;
  if (OB_SUCCESS == ret)
  {
    ret = serialization::decode_vi64(data->get_data(), data->get_capacity(), data->get_position(), &timestamp);
    TBSYS_LOG(DEBUG, "recv schema changed request, timestamp=%ld ret=%d", timestamp, ret);
  }

  tbnet::Connection* connection = ob_packet->get_connection();
  ThreadSpecificBuffer::Buffer* thread_buffer = response_packet_buffer_.get_buffer();
  if (NULL == thread_buffer)
  {
    ret = OB_ERROR;
  }
  else
  {
    thread_buffer->reset();
    ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());
    
    ObResultCode result_msg;
    result_msg.result_code_ = ret;
    ret = result_msg.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
    
    int32_t channel_id = ob_packet->getChannelId();
    ret = send_response(OB_WAITING_JOB_DONE_RESPONSE, 1, out_buffer, connection, channel_id);
  }
  return ret;
}

int MockRootServer::handle_fetch_schema(ObPacket * ob_packet)
{
  int ret = OB_SUCCESS;
  ObDataBuffer* data = ob_packet->get_buffer();
  if (NULL == data)
  {
    ret = OB_ERROR;
  }
  
  int64_t timestamp = 0;
  if (OB_SUCCESS == ret)
  {
    ret = serialization::decode_vi64(data->get_data(), data->get_capacity(), data->get_position(), &timestamp);
    TBSYS_LOG(DEBUG, "recv fetch schema request, timestamp=%ld ret=%d", timestamp, ret);
  }
  
  tbnet::Connection* connection = ob_packet->get_connection();
  ThreadSpecificBuffer::Buffer* thread_buffer = response_packet_buffer_.get_buffer();
  if (NULL == thread_buffer)
  {
    ret = OB_ERROR;
  }
  else
  {
    thread_buffer->reset();
    ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());
    
    ObResultCode result_msg;
    result_msg.result_code_ = ret;
    ret = result_msg.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
    
    ObSchemaManager schema(0 == timestamp ? 1 : timestamp);
    tbsys::CConfig config;
    ret = schema.parse_from_file("./test_schema.ini", config);
    int32_t channel_id = ob_packet->getChannelId();
    ret = schema.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
    ret = send_response(OB_REPORT_TABLETS_RESPONSE, 1, out_buffer, connection, channel_id);
  }
  return ret;
}

int MockRootServer::handle_register_server(ObPacket * ob_packet)
{
  int ret = OB_SUCCESS;
  ObDataBuffer* data = ob_packet->get_buffer();
  if (NULL == data)
  {
    ret = OB_ERROR;
  }

  if (OB_SUCCESS == ret)
  {
    ObServer server;
    ret = server.deserialize(data->get_data(), data->get_capacity(), data->get_position());
  }

  if (OB_SUCCESS == ret)
  {
    bool is_merger = false;
    ret = serialization::decode_bool(data->get_data(), data->get_capacity(), data->get_position(), &is_merger);
    if ((ret == OB_SUCCESS) && is_merger)
    {
      TBSYS_LOG(INFO, "merge server registered");
    }
  }
  
  // response
  tbnet::Connection* connection = ob_packet->get_connection();
  ThreadSpecificBuffer::Buffer* thread_buffer = response_packet_buffer_.get_buffer();
  if (NULL == thread_buffer)
  {
    ret = OB_ERROR;
  }
  else
  {
    thread_buffer->reset();
    ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());
    
    ObResultCode result_msg;
    result_msg.result_code_ = ret;
    result_msg.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
    
    int32_t channel_id = ob_packet->getChannelId();
    ret = send_response(OB_REPORT_TABLETS_RESPONSE, 1, out_buffer, connection, channel_id);
  }
  TBSYS_LOG(INFO, "handle register server result:ret[%d]", ret);
  return ret;
}


int MockRootServer::handle_scan_root(ObPacket * ob_packet)
{
  int ret = OB_SUCCESS;
  ObDataBuffer* data = ob_packet->get_buffer();
  if (NULL == data)
  {
    ret = OB_ERROR;
  }

  if (OB_SUCCESS == ret)
  {
    ObScanParam param;
    ret = param.deserialize(data->get_data(), data->get_capacity(), data->get_position());
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "check param failed");
    }
    else
    {
      //
      if (param.get_column_id_size() != 1)
      {
        TBSYS_LOG(ERROR, "check param failed:table[%lu], size[%lu]", param.get_table_id(), param.get_column_id_size());
        ret = OB_ERROR;
      }
    }
  }

  tbnet::Connection* connection = ob_packet->get_connection();
  ThreadSpecificBuffer::Buffer* thread_buffer = response_packet_buffer_.get_buffer();
  if (NULL == thread_buffer)
  {
    ret = OB_ERROR;
  }
  else
  {
    thread_buffer->reset();
    ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());
    
    ObResultCode result_msg;
    result_msg.result_code_ = ret;
    ret = result_msg.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());

    // fake cell
    ObCellInfo cell;
    cell.table_id_ = 0;
    cell.column_id_ = 1;
    cell.value_.set_int(246);
    
    ObScanner scanner;
    scanner.add_cell(cell);
    
    int32_t channel_id = ob_packet->getChannelId();
    ret = scanner.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
    //
    ret = send_response(OB_REPORT_TABLETS_RESPONSE, 1, out_buffer, connection, channel_id);
  }
  TBSYS_LOG(INFO, "handle scan root table result:ret[%d]", ret);
  return ret;
}

int main(int argc, char ** argv)
{
  ob_init_memory_pool();
  MockRootServer server;
  MockServerRunner root_server(server);
  tbsys::CThread root_server_thread;
  root_server_thread.start(&root_server, NULL);
  root_server_thread.join();
  return 0;
}



