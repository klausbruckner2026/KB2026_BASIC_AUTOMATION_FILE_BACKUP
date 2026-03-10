#!/bin/bash

#====================================================================
# Script: mysql_to_hive_sqoop.sh
# Description: Uses Sqoop for efficient MySQL to Hive transfer
#====================================================================

# Configuration
TARGET_DATE="${1:-$(date -d 'yesterday' +%Y-%m-%d)}"
MYSQL_TABLE="${2:-table_name}"
MYSQL_DB="${3:-db_name}"
HIVE_TABLE="mysql_${MYSQL_TABLE}"

# MySQL Connection
MYSQL_CONN="jdbc:mysql://localhost:3306/${MYSQL_DB}"
MYSQL_USER="root"
MYSQL_PASS="root"

# Hive Configuration
HIVE_PARTITION="dt=${TARGET_DATE}"
HIVE_TARGET_DIR="/user/hive/warehouse/${HIVE_TABLE}/${HIVE_PARTITION}"

echo "Starting Sqoop import for date: $TARGET_DATE"

# Sqoop import command
sqoop import \
    --connect "$MYSQL_CONN" \
    --username "$MYSQL_USER" \
    --password "$MYSQL_PASS" \
    --table "$MYSQL_TABLE" \
    --where "DATE(datetime_column) = '$TARGET_DATE'" \
    --target-dir "$HIVE_TARGET_DIR" \
    --hive-import \
    --hive-table "$HIVE_TABLE" \
    --hive-partition-key "dt" \
    --hive-partition-value "$TARGET_DATE" \
    --create-hive-table \
    --fields-terminated-by '\t' \
    --lines-terminated-by '\n' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --compress \
    --compression-codec snappy \
    --as-parquetfile 2>&1 | tee -a sqoop_import_${TARGET_DATE}.log

# Check status
if [ ${PIPESTATUS[0]} -eq 0 ]; then
    echo "Sqoop import completed successfully!"
    
    # Run analyze table
    hive -e "ANALYZE TABLE ${HIVE_TABLE} PARTITION(dt='${TARGET_DATE}') COMPUTE STATISTICS;"
else
    echo "Sqoop import failed!"
    exit 1
fi