#!/bin/sh

if [ $# -lt 3 ]; then
	echo "Usage: crawler.sh <user_followers_file> <tweet_output_file> <rewteet_output_file>"
	exit 1
fi
if ! [ -f $1 ]; then
   echo "File $1 not found." >&3
   exit 1
fi

#number of crawlers, running on different machines
crawlerNum=1

java -Xms1G -cp ../tweetCrawler/target/tweetCrawler-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.ucr.cs242.tweetCrawler.Crawler $1 $2 $3 0 $crawlerNum
