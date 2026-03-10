#!/bin/bash
#===============================================================================
# Script: sqlserver_to_hdfs_sqoop.sh
# Description: Transfer data from SQL Server to HDFS using Sqoop
# Author: SRavindranath (Enhanced Version)
# Created: $(date +'%Y-%m-%d')
# Version: 2.0
#===============================================================================

#===============================================================================
# CONFIGURATION SECTION - MODIFY THESE VALUES
#===============================================================================

# Source Database Configuration
DB_SERVER="sqlserver_name"
DB_PORT="1433"
DB_NAME="mydatabasename"
DB_USER="user_name"
DB_PASSWORD="your_password"  # Consider using password file or vault

# Target Configuration
HDFS_BASE_PATH="/complete_path_name"
HDFS_TARGET_DIR="${HDFS_BASE_PATH}/Actions"
LOCAL_BASE_PATH="/home/username/complete_path_name/DQ"
LOCAL_TARGET_DIR="${LOCAL_BASE_PATH}/DQData/Actions"
LOG_BASE_PATH="/home/username/DQ/DQLogs"

# Query Configuration
TABLE_NAME="table_name"
QUERY_CONDITION="id < 3"  # Base condition without AND $CONDITIONS
LIMIT_ROWS="100"

# Script Configuration
TIMESTAMP=$(date +'%Y%m%d_%H%M%S')
LOG_FILE="${LOG_BASE_PATH}/sqoop_import_${TIMESTAMP}.log"
ERROR_LOG="${LOG_BASE_PATH}/sqoop_import_${TIMESTAMP}.error"
SCRIPT_NAME=$(basename "$0")
EXECUTION_ID="IMP_${TIMESTAMP}"

#===============================================================================
# COLOR CODES FOR OUTPUT
#===============================================================================

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

#===============================================================================
# FUNCTION DEFINITIONS
#===============================================================================

#-------------------------------------------------------------------------------
# Logging function
#-------------------------------------------------------------------------------
log_message() {
    local level=$1
    local message=$2
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    local log_entry="[${timestamp}] [${level}] ${message}"
    
    # Write to log file
    echo "${log_entry}" >> "${LOG_FILE}"
    
    # Write to console with color
    case ${level} in
        "ERROR")   echo -e "${RED}${log_entry}${NC}" ;;
        "WARNING") echo -e "${YELLOW}${log_entry}${NC}" ;;
        "SUCCESS") echo -e "${GREEN}${log_entry}${NC}" ;;
        "INFO")    echo -e "${CYAN}${log_entry}${NC}" ;;
        *)         echo "${log_entry}" ;;
    esac
}

#-------------------------------------------------------------------------------
# Error handling function
#-------------------------------------------------------------------------------
handle_error() {
    local exit_code=$1
    local error_message=$2
    local line_number=$3
    
    log_message "ERROR" "Error at line ${line_number}: ${error_message} (Exit code: ${exit_code})"
    
    # Write to error log
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Error: ${error_message}" >> "${ERROR_LOG}"
    
    # Cleanup on error
    cleanup_on_error
    
    exit ${exit_code}
}

#-------------------------------------------------------------------------------
# Cleanup function for error scenarios
#-------------------------------------------------------------------------------
cleanup_on_error() {
    log_message "INFO" "Performing cleanup due to error..."
    
    # Remove partial HDFS directory if it exists
    if hdfs dfs -test -d "${HDFS_TARGET_DIR}" 2>/dev/null; then
        log_message "INFO" "Removing incomplete HDFS directory: ${HDFS_TARGET_DIR}"
        hdfs dfs -rm -r -skipTrash "${HDFS_TARGET_DIR}" >> "${LOG_FILE}" 2>&1
    fi
    
    # Remove partial local file if it exists
    if [ -f "${LOCAL_TARGET_DIR}/part-m-00000" ]; then
        log_message "INFO" "Removing incomplete local file"
        rm -f "${LOCAL_TARGET_DIR}/part-m-00000"
    fi
}

#-------------------------------------------------------------------------------
# Trap for unexpected errors
#-------------------------------------------------------------------------------
trap 'handle_error $? "Unexpected error occurred" $LINENO' ERR

#-------------------------------------------------------------------------------
# Function to check prerequisites
#-------------------------------------------------------------------------------
check_prerequisites() {
    log_message "INFO" "Checking prerequisites..."
    
    # Check if hadoop command is available
    if ! command -v hadoop &> /dev/null; then
        handle_error 1 "Hadoop command not found. Please check Hadoop installation." $LINENO
    fi
    
    # Check if sqoop command is available
    if ! command -v sqoop &> /dev/null; then
        handle_error 1 "Sqoop command not found. Please check Sqoop installation." $LINENO
    fi
    
    # Check if sqlcmd is available (optional, for connection test)
    # if ! command -v sqlcmd &> /dev/null; then
    #     log_message "WARNING" "sqlcmd not found. Skipping connection test."
    # fi
    
    log_message "SUCCESS" "All prerequisites satisfied"
}

#-------------------------------------------------------------------------------
# Function to test database connectivity
#-------------------------------------------------------------------------------
test_db_connectivity() {
    log_message "INFO" "Testing SQL Server connectivity..."
    
    # Simple query to test connection
    sqoop eval \
        --connect "jdbc:sqlserver://${DB_SERVER}:${DB_PORT};database=${DB_NAME}" \
        --username "${DB_USER}" \
        --password "${DB_PASSWORD}" \
        --query "SELECT 1 as connection_test" \
        >> "${LOG_FILE}" 2>&1
    
    if [ $? -ne 0 ]; then
        handle_error 1 "Failed to connect to SQL Server. Please check database configuration." $LINENO
    fi
    
    log_message "SUCCESS" "Database connectivity test passed"
}

#-------------------------------------------------------------------------------
# Function to verify table exists and has data
#-------------------------------------------------------------------------------
verify_table() {
    log_message "INFO" "Verifying table ${TABLE_NAME}..."
    
    # Check if table exists and get row count
    local row_count=$(sqoop eval \
        --connect "jdbc:sqlserver://${DB_SERVER}:${DB_PORT};database=${DB_NAME}" \
        --username "${DB_USER}" \
        --password "${DB_PASSWORD}" \
        --query "SELECT COUNT(*) as count FROM ${TABLE_NAME} WHERE ${QUERY_CONDITION}" \
        2>/dev/null | grep -oP '[0-9]+' | tail -1)
    
    if [ -z "${row_count}" ] || [ "${row_count}" -eq 0 ]; then
        log_message "WARNING" "Table ${TABLE_NAME} has no records matching condition: ${QUERY_CONDITION}"
        # Continue anyway, but with warning
    else
        log_message "INFO" "Table verified. Found ${row_count} records matching condition"
    fi
}

#-------------------------------------------------------------------------------
# Function to create required directories
#-------------------------------------------------------------------------------
create_directories() {
    log_message "INFO" "Creating required directories..."
    
    # Create log directory if it doesn't exist
    if [ ! -d "${LOG_BASE_PATH}" ]; then
        mkdir -p "${LOG_BASE_PATH}"
        log_message "INFO" "Created log directory: ${LOG_BASE_PATH}"
    fi
    
    # Create local target directory if it doesn't exist
    if [ ! -d "${LOCAL_TARGET_DIR}" ]; then
        mkdir -p "${LOCAL_TARGET_DIR}"
        log_message "INFO" "Created local directory: ${LOCAL_TARGET_DIR}"
    fi
    
    # Set permissions on local directory
    chmod -R 755 "${LOCAL_BASE_PATH}"
    log_message "INFO" "Set permissions on local directory"
}

#-------------------------------------------------------------------------------
# Function to clean HDFS directory if exists
#-------------------------------------------------------------------------------
clean_hdfs_directory() {
    log_message "INFO" "Checking HDFS directory: ${HDFS_TARGET_DIR}"
    
    if hdfs dfs -test -d "${HDFS_TARGET_DIR}" 2>/dev/null; then
        log_message "WARNING" "HDFS directory already exists. Removing it..."
        hdfs dfs -rm -r -skipTrash "${HDFS_TARGET_DIR}" >> "${LOG_FILE}" 2>&1
        
        if [ $? -ne 0 ]; then
            handle_error 1 "Failed to remove existing HDFS directory" $LINENO
        fi
        log_message "SUCCESS" "Removed existing HDFS directory"
    else
        log_message "INFO" "HDFS directory is clean"
    fi
}

#-------------------------------------------------------------------------------
# Function to run Sqoop import
#-------------------------------------------------------------------------------
run_sqoop_import() {
    log_message "INFO" "Starting Sqoop import..."
    
    # Build the query
    local full_query="SELECT TOP ${LIMIT_ROWS} id, description FROM ${TABLE_NAME} WHERE ${QUERY_CONDITION} AND \$CONDITIONS"
    
    log_message "INFO" "Query: ${full_query}"
    log_message "INFO" "Target HDFS directory: ${HDFS_TARGET_DIR}"
    
    # Run Sqoop import
    sqoop import \
        --connect "jdbc:sqlserver://${DB_SERVER}:${DB_PORT};database=${DB_NAME}" \
        --username "${DB_USER}" \
        --password "${DB_PASSWORD}" \
        --query "${full_query}" \
        -m 1 \
        --null-string '\\N' \
        --null-non-string '\\N' \
        --hive-drop-import-delims \
        --target-dir "${HDFS_TARGET_DIR}" \
        --delete-target-dir \
        2>&1 | tee -a "${LOG_FILE}"
    
    local sqoop_exit_code=${PIPESTATUS[0]}
    
    if [ ${sqoop_exit_code} -ne 0 ]; then
        handle_error ${sqoop_exit_code} "Sqoop import failed with exit code ${sqoop_exit_code}" $LINENO
    fi
    
    log_message "SUCCESS" "Sqoop import completed successfully"
}

#-------------------------------------------------------------------------------
# Function to verify HDFS import
#-------------------------------------------------------------------------------
verify_hdfs_import() {
    log_message "INFO" "Verifying HDFS import..."
    
    # Check if directory exists
    if ! hdfs dfs -test -d "${HDFS_TARGET_DIR}" 2>/dev/null; then
        handle_error 1 "HDFS directory not found after import" $LINENO
    fi
    
    # Check if part file exists
    local part_file_count=$(hdfs dfs -ls "${HDFS_TARGET_DIR}" | grep -c "part-m-00000" || true)
    
    if [ ${part_file_count} -eq 0 ]; then
        handle_error 1 "Part file not found in HDFS directory" $LINENO
    fi
    
    # Get file size and record count
    local file_size=$(hdfs dfs -du -s "${HDFS_TARGET_DIR}" | awk '{print $1}')
    local record_count=$(hdfs dfs -cat "${HDFS_TARGET_DIR}/part-m-00000" 2>/dev/null | wc -l)
    
    if [ ${file_size} -eq 0 ]; then
        handle_error 1 "Imported file is empty" $LINENO
    fi
    
    log_message "SUCCESS" "HDFS import verified"
    log_message "INFO" "File size: ${file_size} bytes, Records: ${record_count}"
}

#-------------------------------------------------------------------------------
# Function to copy from HDFS to local
#-------------------------------------------------------------------------------
copy_to_local() {
    log_message "INFO" "Copying data from HDFS to local..."
    
    # Clean existing local file if it exists
    if [ -f "${LOCAL_TARGET_DIR}/part-m-00000" ]; then
        log_message "INFO" "Removing existing local file"
        rm -f "${LOCAL_TARGET_DIR}/part-m-00000"
    fi
    
    # Copy from HDFS to local
    hadoop fs -copyToLocal "${HDFS_TARGET_DIR}/part-m-00000" "${LOCAL_TARGET_DIR}/"
    
    if [ $? -ne 0 ]; then
        handle_error 1 "Failed to copy file from HDFS to local" $LINENO
    fi
    
    # Set permissions
    chmod 644 "${LOCAL_TARGET_DIR}/part-m-00000"
    
    log_message "SUCCESS" "File copied to local: ${LOCAL_TARGET_DIR}/part-m-00000"
}

#-------------------------------------------------------------------------------
# Function to verify local file
#-------------------------------------------------------------------------------
verify_local_file() {
    log_message "INFO" "Verifying local file..."
    
    local local_file="${LOCAL_TARGET_DIR}/part-m-00000"
    
    # Check if file exists
    if [ ! -f "${local_file}" ]; then
        handle_error 1 "Local file not found" $LINENO
    fi
    
    # Check if file is not empty
    if [ ! -s "${local_file}" ]; then
        handle_error 1 "Local file is empty" $LINENO
    fi
    
    # Get file info
    local file_size=$(stat -c%s "${local_file}" 2>/dev/null || stat -f%z "${local_file}" 2>/dev/null)
    local line_count=$(wc -l < "${local_file}")
    
    log_message "SUCCESS" "Local file verified"
    log_message "INFO" "Local file size: ${file_size} bytes, Lines: ${line_count}"
}

#-------------------------------------------------------------------------------
# Function to generate summary report
#-------------------------------------------------------------------------------
generate_summary() {
    local summary_file="${LOG_BASE_PATH}/summary_${TIMESTAMP}.txt"
    local hdfs_size=$(hdfs dfs -du -s "${HDFS_TARGET_DIR}" | awk '{print $1}')
    local hdfs_size_hr=$(hdfs dfs -du -s -h "${HDFS_TARGET_DIR}" | awk '{print $1}')
    local local_size=$(stat -c%s "${LOCAL_TARGET_DIR}/part-m-00000" 2>/dev/null || stat -f%z "${LOCAL_TARGET_DIR}/part-m-00000" 2>/dev/null)
    local record_count=$(wc -l < "${LOCAL_TARGET_DIR}/part-m-00000")
    
    cat > "${summary_file}" << EOF
===============================================================================
                      SQOOP IMPORT SUMMARY REPORT
===============================================================================
Execution ID        : ${EXECUTION_ID}
Script              : ${SCRIPT_NAME}
Execution Time      : $(date '+%Y-%m-%d %H:%M:%S')

SOURCE INFORMATION:
------------------
Database Server     : ${DB_SERVER}:${DB_PORT}
Database Name       : ${DB_NAME}
Table Name          : ${TABLE_NAME}
Query Condition     : ${QUERY_CONDITION}
Limit Rows          : ${LIMIT_ROWS}

TARGET INFORMATION:
------------------
HDFS Directory      : ${HDFS_TARGET_DIR}
HDFS File Size      : ${hdfs_size} bytes (${hdfs_size_hr})
Local Directory     : ${LOCAL_TARGET_DIR}
Local File          : ${LOCAL_TARGET_DIR}/part-m-00000
Local File Size     : ${local_size} bytes
Records Extracted   : ${record_count}

LOG FILES:
----------
Main Log            : ${LOG_FILE}
Error Log           : ${ERROR_LOG}
Summary File        : ${summary_file}

STATUS: SUCCESS
===============================================================================
EOF
    
    log_message "SUCCESS" "Summary report generated: ${summary_file}"
}

#-------------------------------------------------------------------------------
# Function to cleanup old files
#-------------------------------------------------------------------------------
cleanup_old_files() {
    log_message "INFO" "Cleaning up old files..."
    
    # Remove log files older than 30 days
    find "${LOG_BASE_PATH}" -name "*.log" -type f -mtime +30 -delete 2>/dev/null
    find "${LOG_BASE_PATH}" -name "*.error" -type f -mtime +30 -delete 2>/dev/null
    find "${LOG_BASE_PATH}" -name "summary_*.txt" -type f -mtime +7 -delete 2>/dev/null
    
    log_message "INFO" "Cleanup completed"
}

#===============================================================================
# MAIN EXECUTION
#===============================================================================

# Clear screen for better presentation
clear

# Display header
echo -e "${BLUE}"
echo "================================================================================"
echo "                    SQL SERVER TO HDFS DATA TRANSFER"
echo "================================================================================"
echo -e "${NC}"

# Start script execution
log_message "INFO" "Script started with Execution ID: ${EXECUTION_ID}"
log_message "INFO" "Script version: 2.0"

# Execute main functions with timing
START_TIME=$(date +%s)

# Step 1: Check prerequisites
check_prerequisites

# Step 2: Test database connectivity
test_db_connectivity

# Step 3: Verify table
verify_table

# Step 4: Create directories
create_directories

# Step 5: Clean HDFS directory
clean_hdfs_directory

# Step 6: Run Sqoop import
run_sqoop_import

# Step 7: Verify HDFS import
verify_hdfs_import

# Step 8: Copy to local
copy_to_local

# Step 9: Verify local file
verify_local_file

# Step 10: Generate summary
generate_summary

# Step 11: Cleanup old files
cleanup_old_files

# Calculate execution time
END_TIME=$(date +%s)
EXECUTION_TIME=$((END_TIME - START_TIME))

# Final success message
echo -e "${GREEN}"
echo "================================================================================"
echo "                      ✓ PROCESS COMPLETED SUCCESSFULLY"
echo "================================================================================"
echo -e "${NC}"
log_message "SUCCESS" "Total execution time: ${EXECUTION_TIME} seconds"
log_message "SUCCESS" "Log file: ${LOG_FILE}"
log_message "SUCCESS" "Summary report: ${LOG_BASE_PATH}/summary_${TIMESTAMP}.txt"
log_message "INFO" "Script finished"

exit 0