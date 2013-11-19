AC_PREREQ([2.62])
AC_INIT([IonGun], [0.9], [http://www.iongun.org])
AM_INIT_AUTOMAKE
AC_CONFIG_SRCDIR([src/org/iongun/util/Translator.java])
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
