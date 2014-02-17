HADOOP_HOME := /opt/hadoop
MTJ := /opt/hadoop/lib/mtj-0.9.9.jar
JLAPACK_JBLAS := /home/hadoop
COMPILE := $(shell ./compile $(HADOOP_HOME) $(MTJ) $(JLAPACK_JBLAS))
all: TSQR
	$(COMPILE)
	jar -cvf TSQR.jar -C TSQR/ .
TSQR:
	mkdir TSQR
clean:
	rm -r TSQR
	rm TSQR.jar
