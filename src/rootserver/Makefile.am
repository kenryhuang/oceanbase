AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbnet \
			  -I${TBLIB_ROOT}/include/tbsys \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src 

LDADD = $(top_builddir)/src/common/libcommon.a \
		${TBLIB_ROOT}/lib/libtbnet.a \
		${TBLIB_ROOT}/lib/libtbsys.a

AM_LDFLAGS = -lpthread -lc -lm -lrt -lnuma

pub_source = ob_chunk_server_manager.cpp \
			 ob_root_server2.cpp \
			 ob_root_worker.cpp \
			 ob_server_balance_info.cpp \
			 ob_balance_candidate_server_manager2.cpp \
			 ob_batch_migrate_info.cpp \
       ob_root_log_worker.cpp \
			 ob_tablet_info_manager.cpp \
			 ob_root_meta2.cpp \
			 ob_root_table2.cpp \
       ob_root_log_replay.cpp \
       ob_root_log_manager.cpp \
       ob_root_rpc_stub.cpp \
       ob_root_fetch_thread.cpp

noinst_LIBRARIES = librootserver.a
#bin_PROGRAMS = rootserver schema_reader checkpoint2str str2checkpoint root_admin
bin_PROGRAMS = rootserver root_admin
noinst_PROGRAMS = schema_reader checkpoint2str str2checkpoint
librootserver_a_SOURCES = ${pub_source}
rootserver_SOURCES = ob_root_main.cpp ob_rt_main.cpp ${pub_source}
nodist_rootserver_SOURCES = ../../svn_version.cpp
schema_reader_SOURCES = ob_schema_reader.cpp ${pub_source}
checkpoint2str_SOURCES = ob_check_point2str.cpp ${pub_source}
str2checkpoint_SOURCES = ob_str2check_point.cpp ${pub_source}
root_admin_SOURCES = ob_root_admin.cpp


EXTRA_DIST = \
	     ob_balance_candidate_server_manager2.h \
	     ob_batch_migrate_info.h \
	     ob_chunk_server_manager.h \
	     ob_root_log_worker.h \
	     ob_root_main.h \
	     ob_root_meta2.h \
	     ob_root_server2.h \
	     ob_root_signal.h \
	     ob_root_table2.h \
	     ob_root_worker.h \
	     ob_server_balance_info.h \
	     ob_tablet_info_manager.h \
	     ob_root_log_manager.h \
	     ob_root_rpc_stub.h \
	     ob_root_log_replay.h \
	     ob_root_stat.h \
	     ob_root_fetch_thread.h \
	     rootserver.conf.template

pkgdata_data_DATA=rootserver.conf.template
pkgdata_datadir=${prefix}/etc

# rules to auto generate svn_version.cpp
include $(top_srcdir)/svn_version.mk
