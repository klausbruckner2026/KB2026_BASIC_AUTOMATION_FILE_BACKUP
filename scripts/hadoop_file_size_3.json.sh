#!/bin/bash

#====================================================================
# Script: hdfs_file_size_json.sh
# Description: Outputs file sizes in JSON format for API integration
#====================================================================

HDFS_PATH=${1:-"/apps/hive/warehouse"}

echo "{"
echo "  \"scan_path\": \"$HDFS_PATH\","
echo "  \"timestamp\": \"$(date -Iseconds)\","
echo "  \"files\": ["

hdfs fsck "$HDFS_PATH" -files 2>/dev/null | \
    grep "$HDFS_PATH" | \
    grep -v "<dir>" | \
    awk '{
        for(i=1;i<=NF;i++) {
            if($i ~ /^[0-9]+\(bytes\)$/) {
                size=gensub(/\(bytes\)/, "", "g", $i)
                path=$(i-1)
                printf "    {\"size\": %d, \"path\": \"%s\"},\n", size, path
            }
        }
    }' | sed '$ s/,$//'  # Remove trailing comma from last line

echo "  ]"
echo "}"