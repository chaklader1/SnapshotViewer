#!/bin/bash

source_path=${1%/}

awk -F ',' '
	{
		if (FILENAME ~ "csv") {
			gsub("\r", "")
			print $1 "," (($1 in metas) ? metas[$1] : 0)
		} else for (f=8; f<NF; f+=13) metas[$f]++
	} ENDFILE {
		#print FILENAME
		#for (meta in metas) print meta, metas[meta]
	}' $source_path/{MarketDataFeedEngine*,data.csv} > out.csv