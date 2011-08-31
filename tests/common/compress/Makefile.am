AM_CPPFLAGS = -I${top_srcdir}/src/common/compress \
			        -I${top_srcdir}/include

LDADD = $(top_builddir)/src/common/compress/libcomp.a 

AM_LDFLAGS=-lpthread -lc -lm -lgtest -ldl ${GCOV_LIB}
CXXFLAGS=-D__STDC_LIMIT_MACROS -g -Wall #-fprofile-arcs -ftest-coverage
noinst_PROGRAMS=test_libcomp

test_libcomp_SOURCES = test_libcomp.cpp

