hadoop_home=$1
mtj=$2
jlablas=$3
hadoopcore=$(ls $hadoop_home/hadoop-*-core.jar)
slf4j=$(ls $hadoop_home/lib/slf4j-api-*.jar)
javac -classpath $hadoopcore:$mtj:$slf4j -d TSQR jtsqr/nthu/scopelab/tsqr/*.java jtsqr/nthu/scopelab/tsqr/matrix/*.java jtsqr/nthu/scopelab/tsqr/math/*.java jtsqr/nthu/scopelab/tsqr/ssvd/*.java jtsqr/nthu/scopelab/tsqr/rs/*.java
