all: hdfs_test

hdfs_test.o:hdfs_test.c
	gcc -c hdfs_test.c -I$(HADOOP_HOME)/include

hdfs_test: hdfs_test.o
	gcc hdfs_test.o -I$(HADOOP_HOME)/include	\
	-L$(HADOOP_HOME)/lib/native -o hdfs_test -lhdfs 

