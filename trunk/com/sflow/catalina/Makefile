JAVAC=javac
JARC=jar
OPT=-g -target 1.5 -source 1.5
 
CLASSPATH='.:/usr/share/apache-tomcat-7.0.11/lib/catalina.jar:/usr/share/apache-tomcat-7.0.11/lib/servlet-api.jar:/usr/share/apache-tomcat-7.0.11/lib/tomcat-coyote.jar'
JFLAGS= -classpath $(CLASSPATH)
 
.SUFFIXES: .java .class
 
CLASSES= \
        SFlowValve.class
 
all: build-classes build-jar
 
build-classes: $(CLASSES)

build-jar:
	cd ../../../; $(JARC) cf com/sflow/catalina/sflowvalve.jar com/sflow/catalina/*.class

install:
	cp sflowvalve.jar /usr/share/apache-tomcat-7.0.11/lib/

clean:
	rm -f *.class
	rm -f *.jar
 
#-----------------------------------------------------------
#generic rules
 
.java.class:
	$(JAVAC) $(JFLAGS) $(OPT) $*.java

