#!/bin/bash

#====================================================================
# Script: hive_table_size_report.sh
# Description: Summarizes file sizes by Hive table/partition
#====================================================================

HIVE_WAREHOUSE="/apps/hive/warehouse"

echo "========================================================="
echo " Hive Table Size Summary"
echo "========================================================="

# Process each database/table
hdfs dfs -ls "$HIVE_WAREHOUSE" 2>/dev/null | grep "^d" | awk '{print $8}' | while read -r db_path; do
    db_name=$(basename "$db_path")
    
    hdfs dfs -ls "$db_path" 2>/dev/null | grep "^d" | awk '{print $8}' | while read -r table_path; do
        table_name=$(basename "$table_path")
        
        # Calculate total size for the table
        table_size=$(hdfs dfs -du -s "$table_path" 2>/dev/null | awk '{print $1}')
        
        if [ -n "$table_size" ] && [ "$table_size" -gt 0 ]; then
            # Convert to human readable
            if [ "$table_size" -ge 1073741824 ]; then
                size_str=$(echo "scale=2; $table_size/1073741824" | bc)" GB"
            elif [ "$table_size" -ge 1048576 ]; then
                size_str=$(echo "scale=2; $table_size/1048576" | bc)" MB"
            else
                size_str="$table_size B"
            fi
            
            printf "%-30s %10s\n" "$db_name.$table_name" "$size_str"
        fi
    done
done

echo "========================================================="
echo "Finished!!!"