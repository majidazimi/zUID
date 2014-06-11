#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])
AC_INIT([zUID], [0.1], [azimi.majid@yahoo.com])
AC_CONFIG_SRCDIR([config.h.in])
AC_CONFIG_HEADERS([config.h])
AM_INIT_AUTOMAKE

# Checks for programs.
AC_PROG_CC_C99
AC_PROG_CPP

# Use the C language and compiler for the following checks
AC_LANG([C])

# Checks for libraries.
PKG_CHECK_MODULES([ZMQ], [libzmq])
PKG_CHECK_MODULES([CZMQ], [libczmq])

# Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS([stdlib.h string.h sys/time.h czmq.h zmq.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_TYPE_SIZE_T

# Checks for library functions.
AC_FUNC_MALLOC
AC_CHECK_FUNCS([gettimeofday strndup strtol])

AC_CONFIG_FILES([Makefile
                 src/Makefile])
AC_OUTPUT
