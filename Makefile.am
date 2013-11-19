SRC_DIR = src
LIB_DIR = lib
BIN_DIR = bin
OBJ_DIR = classes

dist_pkgdata_DATA = @PACKAGE@.jar $(shell find $(srcdir)/$(LIB_DIR) -type f -iname '*.jar') 
dist_java_JAVA = $(shell find $(srcdir)/$(SRC_DIR) -type f -iname '*.java')
javadir = $(datadir)/java
JAVAROOT = $(OBJ_DIR)
CLASSPATH = $(shell find $(srcdir)/$(LIB_DIR) -name '*.jar' -printf '%p:')
CLASSPATH_ENV = export CLASSPATH=$(shell find $(srcdir)/$(LIB_DIR) -name '*.jar' -printf '%p:');
AM_JAVACFLAGS = -sourcepath $(srcdir)/$(SRC_DIR) -Xlint:unchecked -Xlint:deprecation
EXTRA_DIST = $(LIB_DIR) $(BIN_DIR)/@PACKAGE@.in
EXTRA_SCRIPTS = $(BIN_DIR)
CLEANFILES =  @PACKAGE@ @PACKAGE@.jar

$(shell mkdir -p $(OBJ_DIR))

@PACKAGE@.jar: classjava.stamp $(shell find $(srcdir)/$(SRC_DIR) -type f ! -iname '*.java')
	cd $(srcdir)/$(SRC_DIR) ; find . -type f ! -iname '*.java' -exec cp --parents -u {} ../$(OBJ_DIR) \;
	touch $@
	@JAR@ -uf $@ -C $(OBJ_DIR) .

clean-local:
	rm -rf $(OBJ_DIR)

# check: @PACKAGE@.jar
# 	@@JAVA@ -cp $(CLASSPATH):@PACKAGE@.jar org.junit.runner.JUnitCore \
# 	org.pjdbc.tests.CatDriverTest \
# 	org.pjdbc.tests.DriverCompositionTest \
# 	org.pjdbc.tests.FilteringDriverTest \
# 	org.pjdbc.tests.LoggingDriverTest \
# 	org.pjdbc.tests.PoolingDriverTest \
# 	org.pjdbc.tests.SinkDriverTest \
# 	org.pjdbc.tests.TeeDriverTest
