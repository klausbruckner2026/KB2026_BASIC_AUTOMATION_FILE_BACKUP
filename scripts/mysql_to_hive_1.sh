#!/bin/bash

#====================================================================
# Script: mysql_to_hive_daily_pull.sh
# Description: Daily incremental data transfer from MySQL to Hive external table
# Author: Data Engineering Team
# Usage: ./mysql_to_hive_daily_pull.sh [date] [table_name] [database]
# Example: ./mysql_to_hive_daily_pull.sh 2024-01-15 sales mydb
#====================================================================

#==================== CONFIGURATION ====================
# MySQL Configuration
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-root}"
MYSQL_HOST="${MYSQL_HOST:-localhost}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_DATABASE="${1:-db_name}"  # Can be passed as argument
MYSQL_TABLE="${2:-table_name}"   # Can be passed as argument

# Hive Configuration
HIVE_WAREHOUSE_DIR="/user/hive/warehouse"
HIVE_DATABASE="default"
HIVE_EXTERNAL_TABLE="${3:-mysql_import_table}"
HIVE_PARTITION_COLUMN="dt"  # Date partition column

# Date Configuration
TARGET_DATE="${4:-$(date -d 'yesterday' +%Y-%m-%d)}"  # Default to yesterday
YEAR=$(date -d "$TARGET_DATE" +%Y)
MONTH=$(date -d "$TARGET_DATE" +%m)
DAY=$(date -d "$TARGET_DATE" +%d)

# File Paths
BASE_DIR="/data/mysql_imports"
OUTPUT_DIR="${BASE_DIR}/${MYSQL_DATABASE}/${MYSQL_TABLE}/${YEAR}/${MONTH}"
OUTPUT_FILE="${OUTPUT_DIR}/data_${TARGET_DATE}.tsv"
HIVE_TARGET_PATH="${HIVE_WAREHOUSE_DIR}/${HIVE_EXTERNAL_TABLE}/dt=${TARGET_DATE}"

# Logging
LOG_DIR="/var/log/mysql_hive_import"
LOG_FILE="${LOG_DIR}/import_${MYSQL_DATABASE}_${MYSQL_TABLE}_${TARGET_DATE}.log"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

# Email Alerts (optional)
ALERT_EMAIL="data-team@example.com"

#==================== FUNCTIONS ====================

log_message() {
    local level=$1
    local message=$2
    echo "[$TIMESTAMP] [$level] $message" | tee -a "$LOG_FILE"
}

send_alert() {
    local subject=$1
    local body=$2
    if [ -n "$ALERT_EMAIL" ]; then
        echo "$body" | mail -s "$subject" "$ALERT_EMAIL"
    fi
}

check_prerequisites() {
    log_message "INFO" "Checking prerequisites..."
    
    # Check if mysql client is installed
    if ! command -v mysql &> /dev/null; then
        log_message "ERROR" "MySQL client is not installed"
        exit 1
    fi
    
    # Check if hive command is available
    if ! command -v hive &> /dev/null; then
        log_message "ERROR" "Hive command not available"
        exit 1
    fi
    
    # Check if hdfs command is available
    if ! command -v hdfs &> /dev/null; then
        log_message "ERROR" "HDFS command not available"
        exit 1
    fi
    
    log_message "INFO" "All prerequisites satisfied"
}

create_output_directory() {
    log_message "INFO" "Creating output directory: $OUTPUT_DIR"
    mkdir -p "$OUTPUT_DIR"
    if [ $? -ne 0 ]; then
        log_message "ERROR" "Failed to create output directory"
        exit 1
    fi
}

test_mysql_connection() {
    log_message "INFO" "Testing MySQL connection..."
    
    mysql -u"$MYSQL_USER" \
          -p"$MYSQL_PASSWORD" \
          -h"$MYSQL_HOST" \
          -P"$MYSQL_PORT" \
          -D"$MYSQL_DATABASE" \
          -e "SELECT 1" > /dev/null 2>&1
    
    if [ $? -ne 0 ]; then
        log_message "ERROR" "Cannot connect to MySQL database"
        send_alert "MySQL Connection Failed" "Cannot connect to $MYSQL_HOST/$MYSQL_DATABASE"
        exit 1
    fi
    
    log_message "INFO" "MySQL connection successful"
}

extract_mysql_data() {
    log_message "INFO" "Extracting data from MySQL table $MYSQL_TABLE for date $TARGET_DATE"
    
    # Build the query
    local query="SELECT * FROM $MYSQL_TABLE WHERE DATE(datetime_column) = '$TARGET_DATE'"
    
    # Count records first
    local count_query="SELECT COUNT(*) FROM $MYSQL_TABLE WHERE DATE(datetime_column) = '$TARGET_DATE'"
    local record_count=$(mysql -u"$MYSQL_USER" \
                              -p"$MYSQL_PASSWORD" \
                              -h"$MYSQL_HOST" \
                              -P"$MYSQL_PORT" \
                              -D"$MYSQL_DATABASE" \
                              -N -B -e "$count_query" | tr -d ' ')
    
    log_message "INFO" "Records to be extracted: $record_count"
    
    if [ "$record_count" -eq 0 ]; then
        log_message "WARNING" "No records found for date $TARGET_DATE"
        # Continue execution but with warning
    fi
    
    # Extract data with proper formatting
    mysql -u"$MYSQL_USER" \
          -p"$MYSQL_PASSWORD" \
          -h"$MYSQL_HOST" \
          -P"$MYSQL_PORT" \
          -D"$MYSQL_DATABASE" \
          --silent \
          --raw \
          -e "$query" > "$OUTPUT_FILE"
    
    if [ $? -ne 0 ]; then
        log_message "ERROR" "Failed to extract data from MySQL"
        send_alert "MySQL Extraction Failed" "Failed to extract data from $MYSQL_TABLE for $TARGET_DATE"
        exit 1
    fi
    
    # Check if file was created and has content
    if [ ! -s "$OUTPUT_FILE" ] && [ "$record_count" -gt 0 ]; then
        log_message "ERROR" "Output file is empty but records were expected"
        exit 1
    fi
    
    log_message "INFO" "Data extraction completed. Output file: $OUTPUT_FILE"
    log_message "INFO" "File size: $(du -h "$OUTPUT_FILE" | cut -f1)"
}

generate_hive_ddl() {
    log_message "INFO" "Generating Hive DDL statements"
    
    # Get MySQL table schema
    local schema_query="SHOW COLUMNS FROM $MYSQL_TABLE"
    local schema=$(mysql -u"$MYSQL_USER" \
                        -p"$MYSQL_PASSWORD" \
                        -h"$MYSQL_HOST" \
                        -P"$MYSQL_PORT" \
                        -D"$MYSQL_DATABASE" \
                        -N -B -e "$schema_query")
    
    # Build Hive columns string
    HIVE_COLUMNS=""
    while IFS=$'\t' read -r col_name col_type rest; do
        # Map MySQL types to Hive types (basic mapping)
        case $col_type in
            *int*|*INT*) hive_type="INT" ;;
            *varchar*|*VARCHAR*|*text*|*TEXT*) hive_type="STRING" ;;
            *date*|*DATE*) hive_type="DATE" ;;
            *datetime*|*DATETIME*|*timestamp*|*TIMESTAMP*) hive_type="TIMESTAMP" ;;
            *decimal*|*DECIMAL*|*float*|*FLOAT*|*double*|*DOUBLE*) hive_type="DOUBLE" ;;
            *) hive_type="STRING" ;;
        esac
        
        HIVE_COLUMNS="${HIVE_COLUMNS}${col_name} ${hive_type}, "
    done <<< "$schema"
    
    # Remove trailing comma and space
    HIVE_COLUMNS=${HIVE_COLUMNS%, }
    
    log_message "INFO" "Hive schema generated"
}

create_hive_table() {
    log_message "INFO" "Creating Hive external table if not exists"
    
    # Generate Hive DDL
    generate_hive_ddl
    
    # Hive DDL
    local hive_ddl="
    CREATE EXTERNAL TABLE IF NOT EXISTS ${HIVE_DATABASE}.${HIVE_EXTERNAL_TABLE} (
        ${HIVE_COLUMNS}
    )
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n'
    STORED AS TEXTFILE
    LOCATION '${HIVE_WAREHOUSE_DIR}/${HIVE_EXTERNAL_TABLE}';"
    
    # Execute Hive DDL
    echo "$hive_ddl" | hive > /dev/null 2>&1
    
    if [ $? -ne 0 ]; then
        log_message "ERROR" "Failed to create Hive table"
        exit 1
    fi
    
    log_message "INFO" "Hive table created/verified successfully"
}

load_to_hdfs() {
    log_message "INFO" "Loading data to HDFS: $HIVE_TARGET_PATH"
    
    # Create HDFS directory
    hdfs dfs -mkdir -p "$HIVE_TARGET_PATH" 2>&1 | tee -a "$LOG_FILE"
    
    if [ $? -ne 0 ]; then
        log_message "ERROR" "Failed to create HDFS directory"
        exit 1
    fi
    
    # Copy file to HDFS
    hdfs dfs -put -f "$OUTPUT_FILE" "$HIVE_TARGET_PATH/" 2>&1 | tee -a "$LOG_FILE"
    
    if [ $? -ne 0 ]; then
        log_message "ERROR" "Failed to copy file to HDFS"
        exit 1
    fi
    
    log_message "INFO" "File successfully copied to HDFS"
    
    # Verify HDFS copy
    local hdfs_file_size=$(hdfs dfs -du "$HIVE_TARGET_PATH/data_${TARGET_DATE}.tsv" | awk '{print $1}')
    local local_file_size=$(stat -c%s "$OUTPUT_FILE")
    
    if [ "$hdfs_file_size" -ne "$local_file_size" ]; then
        log_message "ERROR" "File size mismatch between local and HDFS"
        exit 1
    fi
    
    log_message "INFO" "HDFS file verification successful"
}

add_hive_partition() {
    log_message "INFO" "Adding Hive partition for date $TARGET_DATE"
    
    # Check if partition already exists
    local partition_check=$(hive -e "SHOW PARTITIONS ${HIVE_DATABASE}.${HIVE_EXTERNAL_TABLE} PARTITION(dt='${TARGET_DATE}')" 2>/dev/null)
    
    if [ -n "$partition_check" ]; then
        log_message "WARNING" "Partition already exists. Repairing table..."
        hive -e "MSCK REPAIR TABLE ${HIVE_DATABASE}.${HIVE_EXTERNAL_TABLE}" > /dev/null 2>&1
    else
        # Add partition
        local add_partition="
        ALTER TABLE ${HIVE_DATABASE}.${HIVE_EXTERNAL_TABLE} 
        ADD IF NOT EXISTS PARTITION (dt='${TARGET_DATE}') 
        LOCATION '${HIVE_TARGET_PATH}';"
        
        echo "$add_partition" | hive > /dev/null 2>&1
        
        if [ $? -ne 0 ]; then
            log_message "ERROR" "Failed to add partition to Hive table"
            exit 1
        fi
    fi
    
    log_message "INFO" "Partition added/verified successfully"
}

validate_data() {
    log_message "INFO" "Validating data in Hive"
    
    # Count records in Hive
    local hive_count=$(hive -S -e "SELECT COUNT(*) FROM ${HIVE_DATABASE}.${HIVE_EXTERNAL_TABLE} WHERE dt='${TARGET_DATE}'" 2>/dev/null | tr -d ' ')
    
    # Get original count from MySQL
    local mysql_count_query="SELECT COUNT(*) FROM $MYSQL_TABLE WHERE DATE(datetime_column) = '$TARGET_DATE'"
    local mysql_count=$(mysql -u"$MYSQL_USER" \
                              -p"$MYSQL_PASSWORD" \
                              -h"$MYSQL_HOST" \
                              -P"$MYSQL_PORT" \
                              -D"$MYSQL_DATABASE" \
                              -N -B -e "$mysql_count_query" | tr -d ' ')
    
    log_message "INFO" "MySQL record count: $mysql_count"
    log_message "INFO" "Hive record count: $hive_count"
    
    if [ "$mysql_count" -ne "$hive_count" ]; then
        log_message "ERROR" "Data validation failed! Count mismatch"
        send_alert "Data Validation Failed" "Count mismatch for $TARGET_DATE: MySQL=$mysql_count, Hive=$hive_count"
        exit 1
    fi
    
    log_message "INFO" "Data validation successful"
}

cleanup_old_files() {
    log_message "INFO" "Cleaning up old local files"
    
    # Remove files older than 7 days
    find "$BASE_DIR" -type f -name "*.tsv" -mtime +7 -delete 2>/dev/null
    
    if [ $? -eq 0 ]; then
        log_message "INFO" "Old files cleaned up"
    else
        log_message "WARNING" "Failed to clean up some old files"
    fi
}

send_success_notification() {
    local subject="MySQL to Hive Import Successful - ${MYSQL_TABLE} - ${TARGET_DATE}"
    local body="
    Import Details:
    ----------------
    Database: $MYSQL_DATABASE
    Table: $MYSQL_TABLE
    Date: $TARGET_DATE
    Records Imported: $(hive -S -e "SELECT COUNT(*) FROM ${HIVE_DATABASE}.${HIVE_EXTERNAL_TABLE} WHERE dt='${TARGET_DATE}'" 2>/dev/null)
    Hive Table: ${HIVE_DATABASE}.${HIVE_EXTERNAL_TABLE}
    HDFS Location: $HIVE_TARGET_PATH
    Log File: $LOG_FILE
    "
    
    if [ -n "$ALERT_EMAIL" ]; then
        echo "$body" | mail -s "$subject" "$ALERT_EMAIL"
    fi
    
    log_message "INFO" "Import completed successfully"
}

#==================== MAIN EXECUTION ====================

main() {
    # Create log directory
    mkdir -p "$LOG_DIR"
    
    log_message "INFO" "========== MySQL to Hive Import Started =========="
    log_message "INFO" "Target Date: $TARGET_DATE"
    log_message "INFO" "MySQL Database: $MYSQL_DATABASE"
    log_message "INFO" "MySQL Table: $MYSQL_TABLE"
    log_message "INFO" "Hive Table: ${HIVE_DATABASE}.${HIVE_EXTERNAL_TABLE}"
    
    # Execute steps
    check_prerequisites
    test_mysql_connection
    create_output_directory
    extract_mysql_data
    create_hive_table
    load_to_hdfs
    add_hive_partition
    validate_data
    cleanup_old_files
    send_success_notification
    
    log_message "INFO" "========== MySQL to Hive Import Completed =========="
}

# Trap errors
trap 'log_message "ERROR" "Script interrupted"; exit 1' INT TERM

# Run main function
main

# Exit with success
exit 0