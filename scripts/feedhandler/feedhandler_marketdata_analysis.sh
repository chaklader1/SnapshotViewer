#!/bin/bash

source_dir=${1%/}
full_output="$source_dir/marketData_full.csv"
zero_response_output="$source_dir/marketData_zero_response.csv"

[[ -f $full_output ]] && rm $full_output
[[ -f $zero_response_output ]] && rm $zero_response_output

awk -F ',' '
	{
		k = $2
		sub("\r", "")
		if (FILENAME ~ "csv$") {
			#print k, kc[k]
			h = (k in kh) ? kh[k] : $3
			if (!(k in kc)) print k "," h >> "'"$zero_response_output"'"
			printf "%s,%s,%d\n", k, h, kc[k]
		} else {
			sub("^[0-9]+ *", "", k)
			#print "count ^" k "^"
			kc[k]++
			kh[k] = $3
		}
	}' $source_dir/{MarketDataFeedHandler.log*,data.csv} > $full_output
