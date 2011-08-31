/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_common_param.cpp for ...
 *
 * Authors:
 *   qushan <qushan@taobao.com>
 *
 */
#include "ob_common_param.h"

namespace oceanbase
{
  namespace common
  {
    ObReadParam::ObReadParam()
    {
      reset();
    }
    
    void ObReadParam::reset()
    {
      is_result_cached_ = false;
      version_range_.start_version_ = 0;
      version_range_.end_version_ = 0;
    }

    ObReadParam::~ObReadParam()
    {
    }

    void ObReadParam::set_is_result_cached(const bool cached)
    {
      is_result_cached_ = cached;
    }

    bool ObReadParam::get_is_result_cached()const
    {
      return is_result_cached_;
    }

    void ObReadParam::set_version_range(const ObVersionRange & range)
    {
      version_range_ = range;
    }

    ObVersionRange ObReadParam::get_version_range(void) const
    {
      return version_range_;
    }
  } /* common */
} /* oceanbase */

