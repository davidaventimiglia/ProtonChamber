################################################################################
# CONFIGURABLE PART
################################################################################

JARFILE = salt.jar
SRCDIR = src
LIBDIR = lib
CLASSESDIR = classes
BINFILES = salt $(JARFILE)
JAVAC_FLAGS = -g -w

################################################################################
# NON-CONFIGURABLE PART
################################################################################

# Automake `primary' says to compile *.java files and put results in `classesdir'
javadir = $(top_builddir)/$(CLASSESDIR)
JAVAROOT = $(javadir)
dist_java_JAVA = $(shell find $(SRCDIR) -name '*.java') src/TParser.java src/TLexer.java

# Debug flags, no warnings, for javac and gcj
AM_JAVACFLAGS = $(JAVAC_FLAGS)

# Automake variable with supplemental items to clean
CLEANFILES = *.class $(JARFILE) salt src/TParser.java src/TLexer.java src/T.tokens

# Automake variables needed by java and javac
JARS = $(shell find $(LIBDIR) -name '*.jar' -printf '%p:')
CLASSPATH = "$(shell $(CYGPATH_PW) $(JARS)$(top_builddir)/$(CLASSESDIR))"
CLASSPATH_ENV = export CLASSPATH=$(CLASSPATH);

# Automake `primary' says to put these `scripts' into the ../bin directory
dist_bin_SCRIPTS = $(BINFILES)

# We need this for substitution in sed commands
saltdir := $(pkgdatadir)

src/TParser.java: T.g
	@JAVA@ -cp $(CLASSPATH) org.antlr.Tool -o src $<

src/TLexer.java: T.g
	@JAVA@ -cp $(CLASSPATH) org.antlr.Tool -o src $<

src/T.tokens: T.g
	@JAVA@ -cp $(CLASSPATH) org.antlr.Tool -o src $<

# Build the main jar file.
$(JARFILE): classdist_java.stamp $(dist_data_DATA)
	if ! test -d scratch ; then \
	  mkdir scratch ; \
	  cd scratch ; \
	  find ../lib/*.jar -exec jar -xf {} \; ; \
	  find . -iname manifest.mf -exec rm {} + ; fi
	if ! test -f $(JARFILE) ; then \
	  touch $(JARFILE) ; \
	  touch ./scratch/ZIPCREATED ; \
	  @JAR@ -uf $(JARFILE) -C ./scratch . ; fi
	@JAR@ -uf $(JARFILE) -C $(javadir) .

salt: salt.in
	sed 's|PKGDATADIR|$(pkgdatadir)|' salt.in > salt

# Supplemental cleaning rule
clean-local:
	rm -rf scratch
	rm -rf $(javadir)

install-data-local:
	mkdir -p $(pkgdatadir)
	cp -rf $(srcdir)/templates $(pkgdatadir)

# Phony targets have no dependencies.
.PHONY: distclean-local clean-local all
