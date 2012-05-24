dnl                                               -*- Autoconf -*-
dnl Process this file with autoconf to produce a configure script.

AC_PREREQ([2.65])
AC_INIT([Atomic], [0.9], [dventimi@gmail.com])
AM_INIT_AUTOMAKE
AC_CONFIG_SRCDIR([src/Atomic.java])
AC_CANONICAL_HOST
AS_CASE([$host_os], [cygwin], [CYGPATH_PW="cygpath -pw"], [CYGPATH_PW="echo"])
AC_SUBST([CYGPATH_PW])
AC_CHECK_CLASSPATH
AC_PROG_JAVAC
AC_PROG_JAVA
AC_PROG_JAR
AC_PROG_INSTALL
AC_CONFIG_FILES([Makefile])
AC_OUTPUT
