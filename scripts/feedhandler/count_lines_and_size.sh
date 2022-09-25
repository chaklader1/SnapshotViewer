#!/bin/bash

source_path=${1%/}

lines=$(cat $source_path/MarketDataFeedHandler.log*| wc -l)
size=$(du -hsc $source_path/MarketDataFeedHandler.log* | awk 'END { print $1 }')
echo "lines=$lines"
echo "size=$size"
