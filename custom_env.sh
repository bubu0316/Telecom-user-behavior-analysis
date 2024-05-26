#! /bin/bash
# java_home
export JAVA_HOME=/bigdata/server/jdk1.8
export PATH=$JAVA_HOME/bin:$PATH
# hadoop_home
export HADOOP_HOME=/bigdata/server/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
#FLUME_HOME
export FLUME_HOME=/bigdata/server/flume
export PATH=$PATH:$FLUME_HOME/bin
## hive
export HIVE_HOME=/bigdata/server/hive
export PATH=$PATH:$HIVE_HOME/bin
# spark
export SPARK_HOME=/bigdata/server/spark
export PATH=$PATH:$SPARK_HOME/bin
