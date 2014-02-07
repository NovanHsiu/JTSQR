JTSQR: Hadoop MapReduce Tall-and-Skinny QR Factorization written in Java
Version 1.0
Distributed in 2014/02/07
===

### Hsiu-Cheng Yu, Che-Rung Lee


# Introduction
----------------
The function provided by this package is the computation of QR factorizations for Tall-and-Skinny matrices (TSQR) using MapReduce.  
A 'Tall-and-Skinny matrix' has the property that its number of rows is much bigger than its number of columns. 
The TSQR function can be used to solve many problems, such as stochastic SVD (SSVD).

In this implementation, we provide three functions:

 * TSQR: Compute Q and R matrices of a given tall-and-skinny matrix.
 * Q^T*B: Apply the Q factor to a matrix B.
 * SSVD: Compute the stochastic SVD. This is an application of TSQR. 

This code is written in Java and uses JLAPACK and JBLAS library for matrix computation.

# Environment Setup
--------
This code is for Linux systems.  It is built based on several packages. Make sure those packages are properly installed before using this package.

 * Hadoop 1.x version (available on http://hadoop.apache.org/)
 * matrix-toolkits-java (available on https://github.com/fommil/matrix-toolkits-java)
 * JLAPACK and JBLAS (available on http://www.netlib.org/java/f2j/)

# Compiling 
--------------
There are three paths to setup in Makefile: “HADOOP_HOME”, “MTJ” and “JLAPACK_JBLAS”.

 - HADOOP_HOME
   Path where Hadoop is installed.
 - MTJ_HOME 
  Path where matrix-toolkits-java is installed.
 - JLAPACK_JBLAS_HOME
  Path where JLAPACK and JBLAS are installed.

The Makefile have two commands.
 (1) $ make
     Compiling and packaging all class files into a JAR file
 (2) $ make clean
     Remove JAR file and directory of Class file


# Usage Example and Command Line Arguments
----------------------------------------------------

 $(MTJ_PATH)
  Path of matrix-toolkits-java
  In this example JLAPACK, JBLAS and matrix-toolkits-java are all in one package: It is called mtj-version.jar.
 $(HADOOP_HOME)
  Path where Hadoop is installed
 $(TSQR_PACKAGE)
  Path where TSQR package is installed
  
## Upload our example
 $ hadoop fs -mkdir tsqr
 $ hadoop fs -copyFromLocal testdata/100x5

In the input file of example, one line means one row in a matrix, and numbers are separated by a space. 
If you want to use other matrices for this code, the format of matrices must follow above rules.

## Example: Run TSQR
### Do QR Factorization only
 ${HADOOP_HOME}/bin/hadoop jar ${TSQR_PACKAGE}/TSQR.jar nthu.scopelab.tsqr.TSQRunner \
 -input mat/100x5 \
 -output tsqrOutput \
 -subRowSize 10 \
 -reduceSchedule 4,1 \
 -mis 64 \
 -type 0
 

### Compute Q^T*A
 ${HADOOP_HOME}/bin/hadoop jar ${TSQR_PACKAGE}/TSQR.jar nthu.scopelab.tsqr.TSQRunner \
 -input mat/100x5 \
 -output tsqrOutput \
 -inputB mat/100x5 \
 -outputQ true \
 -subRowSize 10 \
 -reduceSchedule 4,1 \
 -mis 64 \
 -type 1

#### Arguments for Run TSQR
The argument has star mark * means that it must be given a value by user and other arguments without star mark have default value.

 -input *
  Input file, including its directory.
 -inputB *
  The "inputB" argument is necessary if the "type" argument set to 1. 
  The matrix used to compute Q^T*B. Notice that the number of rows of B matrix must equal to that of A matrix(the original matrix).
 -output *
  Output directory
 -outputQ (default: false)
  This argument only used in compute Q^T*B. If it is true, QMultiplyJob outputs the Q matrix to a file.  False outputs R matrix only.
 -subRowSize (default: equal to number of columns)
  It is the number of rows of each submatrix. These submatrices are split from input matrix. “subRowSize” must be bigger than the number of columns, and smaller than the number of rows of the original matrix.
 -reduceSchedule (default: 1) 
  This argument is used in QRFirstJob (refer JTSQR_Introduction.pdf). The input can be a sequence of numbers, which are separated by commas.  The ith number in the sequence represents the number of Reducers in the ith MapReduce task.
  Finally, last number need to set to one in order to calculate the final R.
  For example, 4,1 means that it has two MapReduce and first MapReduce has four Reducers and second has one reducer.
 -mis (default: 64)
  Max Input Split size: the maximum size of each input data.  For example, mis=64 means the maximum size of each split file is 64MB. 
 -type *
  0 means that do QR factorization only and 1 is to compute Q^T*B

## Example: Run SSVD
### Run SSVD
 ${HADOOP_HOME}/bin/hadoop jar ${TSQR_PACKAGE}/TSQR.jar nthu.scopelab.tsqr.ssvd.SSVDRunner \
 -input mat/100kx500 \
 -output tsqrOutput \
 -subRowSize 4000 \
 -reduceSchedule 4,1 \
 -mis 64 \
 -rank 10 \
 -reduceTasks 8

#### Arguments for Run SSVD
 - input, output, ....
  The same as they are in the TSQR example
 - rank *
  “rank” is Stochastic number that would simply m*n matrix to m*k matrix. (k is smaller than n)
 - reduceTasks *
  Number of Reduce tasks of BtJob, UJob and VJob

## Example: Turn the matrix from Hadoop sequencefile into text file and then print out 
 ${HADOOP_HOME}/bin/hadoop jar ${TSQR_PACKAGE}/TSQR.jar nthu.scopelab.tsqr.rs.readSFMat \
 –input tsqrOutput/part-00000

# Tuning Suggestion of Arguments
--------------------------------
If you want improve performance for JTSQR by tuning argument. We have some tips for following three arguments.

-subRowSize
This argument does not affect performance of application in this package, but it has a point need to know. 
If the argument is too large which cause the data size of sub matrix is bigger than Map task input split size, 
that will dramatically reduce the performance. In order to avoid above situation, it would automatically reduce 
the “subRowSize” in JTSQR if that value is too large.

-mis
In most cases, reducing the “mis” would lightly improve the performance. That means that you increase the number 
of Map tasks. If you lunch too many Map tasks (too many JVM processes) that cause memory requirement exceed the free 
memory of machine and then the Hadoop file system corrupted. In order to avoid above situation, it would automatically 
check the value of “mis” in JTSQR.

-reduceSchedule
The default value of this argument was zero (0). If you encounter the out of memory problem of JVM or performance bottleneck
that caused by bigger data, you could reference section 3.3 of document“JTSQR_Introduction" to tune this argument.

# Overview
--------
 * javac/nthu/scopelab/tsqr/TSQRunner.java - driver code for TSQR
 * javac/nthu/scopelab/tsqr/SequencefileMatrixMaker.java - driver code for MapReduce code, which turns the matrix from text file to Hadoop sequencefile 
 * javac/nthu/scopelab/tsqr/ssvd/SSVDRunner.java - driver code for SSVD


# References
--------------
 * Direct QR factorizations for tall-and-skinny matrices in MapReduce architectures [[pdf](http://arxiv.org/abs/1301.1071)]
 * Tall and skinny QR factorizations in MapReduce architectures [[pdf](http://www.cs.purdue.edu/homes/dgleich/publications/Constantine%202011%20-%20TSQR.pdf)]
 * [MAHOUT-376] Implement Map-reduce version of stochastic SVD [https://issues.apache.org/jira/browse/MAHOUT-376]

Contact
--------
 * For any questions, suggestions, and bug reports, email Hsiu-Cheng Yu by s411051@gmail.com please.
 * This code can be reached at: https://github.com/NovanHsiu/JTSQR
