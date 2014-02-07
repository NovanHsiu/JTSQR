HADOOP_HOME := /opt/hadoop
MTJ := /opt/hadoop/lib/mtj-0.9.9.jar
JLAPACK_JBLAS := /home/hadoop
all: TSQR
	javac -classpath $(HADOOP_HOME)/hadoop-core-1.0.3.jar:$(MTJ):$(HADOOP_HOME)/lib/slf4j-api-1.4.3.jar -d TSQR jtsqr/nthu/scopelab/tsqr/*.java jtsqr/nthu/scopelab/tsqr/matrix/*.java jtsqr/nthu/scopelab/tsqr/math/*.java jtsqr/nthu/scopelab/tsqr/ssvd/*.java jtsqr/nthu/scopelab/tsqr/rs/*.java
	jar -cvf TSQR.jar -C TSQR/ .
TSQR:
	mkdir TSQR
clean:
	rm -r TSQR
	rm TSQR.jar
