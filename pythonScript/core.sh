#! /bin/bash

if [ $# != 4  ];then
	echo "参数有误!"
    echo "---------参数一：zzdpJoid  参数二：checkSQL  参数三：recoveryOffset 参数四：执行任务用户"
    echo "---------举例：【sh core.sh 1 submit nosavepoint zdp】"
    exit
fi

export HADOOP_USER_NAME=$4

python FlinkScheduler.py -zzdpJobId $1 -checkSQL $2 -recoveryOffset $3
