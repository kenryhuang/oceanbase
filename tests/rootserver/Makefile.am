AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src \
			  -I${top_srcdir}/src/common \
			  -I${top_srcdir}/src/rootserver 

LDADD = $(top_builddir)/src/rootserver/librootserver.a \
		$(top_builddir)/src/common/libcommon.a \
		${TBLIB_ROOT}/lib/libtbnet.a \
		${TBLIB_ROOT}/lib/libtbsys.a

CXXFLAGS = -D__STDC_LIMIT_MACROS -g -Wall 
AM_LDFLAGS = -lnuma -lpthread -lc -lm  -lgtest  -lrt ${GCOV_LIB}

noinst_PROGRAMS = chunk_servers_manager_test \
			   base_main_test \
			   root_server_test \
				 balacne_candidate_test \
				 test_tablet_info_manager \
			   root_table_test2 \
				 mock_ups_4_root \
				 mock_cs_4_root \
				 rt_press_client \
				 rt_stats_reader


chunk_servers_manager_test_SOURCES = chunk_servers_manager_test.cpp
base_main_test_SOURCES = base_main_test.cpp test_main.cpp
root_server_test_SOURCES = root_server_test.cpp
balacne_candidate_test_SOURCES = balance_candidate_test.cpp
#root_server_log_test_SOURCES = root_server_log_test.cpp
test_tablet_info_manager_SOURCES = test_tablet_info_manager.cpp
root_table_test2_SOURCES = test_root_table2.cpp
mock_ups_4_root_SOURCES = mock_server.cpp mock_update_server.cpp test_update_server_main.cpp
mock_cs_4_root_SOURCES = mock_server.cpp mock_chunk_server.cpp test_chunk_server_main.cpp
rt_press_client_SOURCES = rt_press_client.cpp
rt_stats_reader_SOURCES = rt_stats_reader.cpp

#balance_info_test_SOURCES = balance_info_test.cpp

EXTRA_DIST = \
	root_server_tester.h \
	test_main.h

check_SCRIPTS = $(bin_PROGRAMS)
TESTS = $(check_SCRIPTS) 

CLEANFILES = $(check_SCRIPTS)
