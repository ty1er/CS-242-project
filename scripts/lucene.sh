#!/bin/sh

if [ $# -lt 2 ]; then
	echo "Usage: lucene.sh <tweet_log> <index_dir>"
	exit 1
fi

if ! [ -f $1 ]; then
   echo "File $1 not found." >&3
   exit 1
fi

java -Xms1G -cp ../tweetCrawler/target/tweetCrawler-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.ucr.cs242.tweetCrawler.CreateLuceneIndex $1 $2 