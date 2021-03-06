/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * mock_mem_iterator.h for ...
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *
 */
#ifndef __OCEANBASE_CHUNKSERVER_MOCK_MEM_ITERATOR_H__
#define __OCEANBASE_CHUNKSERVER_MOCK_MEM_ITERATOR_H__

#include "common/ob_read_common_data.h"
#include "common/ob_string_buf.h"
#include "updateserver/ob_memtable.h"

using namespace oceanbase::common;
using namespace oceanbase::updateserver;

class MockMemIterator : public ObIterator
{
  public:
    MockMemIterator()
    {
      cell_num_ = 0;
      iter_idx_ = -1;
    }
    ~MockMemIterator()
    {
      cell_num_ = 0;
      iter_idx_ = -1;
    }

    // reset iter
    void reset()
    {
      iter_idx_ = -1;
    }

  public:
    int add_cell(const ObCellInfo& cell)
    {
      int err = OB_SUCCESS;

      if (cell_num_ >= MAX_MOCK_CELL_NUM)
      {
        TBSYS_LOG(WARN, "cell array full, cell_num=%ld", cell_num_);
        err = OB_ERROR;
      }
      else
      {
        cells_[cell_num_].table_id_ = cell.table_id_;
        cells_[cell_num_].column_id_ = cell.column_id_;

        // store row key
        err = str_buf_.write_string(cell.row_key_, &cells_[cell_num_].row_key_);
        if (OB_SUCCESS == err)
        {
          err = str_buf_.write_obj(cell.value_, &cells_[cell_num_].value_);
        }

        if (OB_SUCCESS == err)
        {
          ++cell_num_;
        }
      }

      return err;
    }

  public:
    int next_cell()
    {
      int err = OB_SUCCESS;
      ++iter_idx_;
      if (iter_idx_ >= cell_num_)
      {
        err = OB_ITER_END;
      }

      return err;
    }

    int get_cell(ObCellInfo** cell)
    {
      int err = OB_SUCCESS;
      if (NULL == cell)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        *cell = &cells_[iter_idx_];
      }

      return err;
    }

    int get_cell(ObCellInfo** cell, bool* is_row_changed)
    {
      int err = OB_SUCCESS;
      if (NULL == cell)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        *cell = &cells_[iter_idx_];
        if (NULL != is_row_changed)
        {
          if (0 == iter_idx_ ||
              cells_[iter_idx_].row_key_ != cells_[iter_idx_ - 1].row_key_)
          {
            *is_row_changed = true;
          }
          else
          {
            *is_row_changed = false;
          }
        }
      }

      return err;
    }

  public:
    static const int64_t MAX_MOCK_CELL_NUM = 1024;
  private:
    ObCellInfo cells_[MAX_MOCK_CELL_NUM];
    int64_t cell_num_;
    int64_t iter_idx_;
    ObStringBuf str_buf_;
};

#endif //__MOCK_MEM_ITERATOR_H__



