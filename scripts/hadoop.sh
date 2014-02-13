#!/bin/sh

if [ $# -lt 2 ]; then
	echo "Usage: hadoop.sh <social_graph_file> <id_screen_name_pair_file>"
	exit 1
fi

social_graph=$1
screen_names=$2
num_edges=`wc -l $social_graph| awk '{print $1}'`
sampled_edges_num=$((${num_edges}/10))
social_graph_basename=`basename $1`
screen_names_basename=`basename $2`
hadoop fs -rm /user/group42/$social_graph_basename
hadoop fs -rm /user/group42/$screen_names_basename
hadoop fs -copyFromLocal $social_graph /user/group42
hadoop fs -copyFromLocal $screen_names /user/group42
hadoop jar ../mapreduceJobs/target/mapreduceJobs-0.0.1-SNAPSHOT-job.jar /user/group42/$social_graph_basename /user/group42/$screen_names_basename /user/group42/${social_graph_basename}_sampled $num_edges $sampled_edges_num
mkdir -p ../data
rm ../data/${social_graph_basename}_sampled
hadoop fs -copyToLocal /user/group42/${social_graph_basename}_sampled/part-r-00000 ../data/${social_graph_basename}_sampled
