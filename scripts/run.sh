#!/bin/sh
data_dir=../data

social_graph=$data_dir/social_graph_mini
screen_names=$data_dir/screen_names_mini

if ! [ -f $social_graph ]; then
   echo "File $social_graph not found." >&3
   exit 1
fi
if ! [ -f $screen_names ]; then
   echo "File $screen_names not found." >&3
   exit 1
fi

./hadoop.sh $social_graph $screen_names
./crawler.sh ${social_graph}_sampled $data_dir/tweet.log $data_dir/retweet.log
./lucene.sh $data_dir/tweet.log $data_dir/luceneIdx