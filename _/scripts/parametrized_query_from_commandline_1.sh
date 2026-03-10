#!/bin/bash

#====================================================================
# Script: hive_partitioned_data_extract.sh
# Description: Extract data from Hive partitioned tables with date range
# Author: Data Engineering Team
# Version: 2.0
#====================================================================

#==================== CONFIGURATION ====================
# Default values (can be overridden via command line)
DEFAULT_DATABASE="yourdatabase_name"
DEFAULT_TABLE="table_name"
DEFAULT_OUTPUT_DIR="/home/kapru/output"
DATE_FORMAT="YYYYMMDD"

# Color codes for better UI
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Log file
LOG_DIR="/tmp/hive_extract_logs"
mkdir -p "$LOG_DIR"
LOG_FILE="${LOG_DIR}/extract_$(date '+%Y%m%d_%H%M%S').log"

#==================== FUNCTIONS ====================

# Logging function
log_message() {
    local level=$1
    local message=$2
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo -e "[$timestamp] [$level] $message" | tee -a "$LOG_FILE"
}

# Print colored output
print_colored() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

# Usage information
show_usage() {
    print_colored "$CYAN" "
====================================================================
                    HIVE DATA EXTRACTOR USAGE
====================================================================
Usage: $0 [OPTIONS]

Required Parameters:
  -s, --startdate     Start date in YYYYMMDD format
  -e, --enddate       End date in YYYYMMDD format
  -r, --ref           Reference number

Optional Parameters:
  -d, --database      Database name (default: yourdatabase_name)
  -t, --table         Table name (default: table_name)
  -o, --output        Output directory (default: /home/kapru/output)
  -f, --format        Output format (tsv|csv|json) (default: tsv)
  -c, --compress      Compress output (gzip)
  -l, --limit         Limit number of rows
  -p, --partitioned   Use partition pruning (yes/no) (default: yes)
  -h, --help          Show this help message

Examples:
  $0 -s 20230101 -e 20230131 -r 12345
  $0 --startdate=20230101 --enddate=20230131 --ref=12345 --format=json
  $0 -s 20230101 -e 20230131 -r 12345 -d mydb -t mytable -o /custom/path

====================================================================
"
}

# Validate date format
validate_date() {
    local date=$1
    local field_name=$2
    
    # Check if date is 8 digits
    if ! [[ $date =~ ^[0-9]{8}$ ]]; then
        log_message "ERROR" "$field_name must be 8 digits in YYYYMMDD format"
        return 1
    fi
    
    # Extract components
    local year=${date:0:4}
    local month=${date:4:2}
    local day=${date:6:2}
    
    # Check if valid date
    if ! date -d "$year-$month-$day" >/dev/null 2>&1; then
        log_message "ERROR" "$field_name is not a valid date: $date"
        return 1
    fi
    
    return 0
}

# Validate reference number
validate_ref() {
    local ref=$1
    
    # Check if reference number is provided and numeric
    if [[ -z "$ref" ]]; then
        log_message "ERROR" "Reference number cannot be empty"
        return 1
    fi
    
    # Optional: Check if reference is numeric
    if ! [[ "$ref" =~ ^[0-9]+$ ]]; then
        log_message "WARNING" "Reference number contains non-numeric characters"
        # Continue but warn
    fi
    
    return 0
}

# Check Hive connectivity
check_hive_connectivity() {
    log_message "INFO" "Checking Hive connectivity..."
    
    if ! command -v hive &> /dev/null; then
        log_message "ERROR" "Hive command not found. Is Hive installed?"
        return 1
    fi
    
    # Test Hive connection
    if ! hive -e "show databases;" >/dev/null 2>&1; then
        log_message "ERROR" "Cannot connect to Hive. Please check your Hive configuration."
        return 1
    fi
    
    log_message "INFO" "Hive connectivity OK"
    return 0
}

# Check if table exists
check_table_exists() {
    local database=$1
    local table=$2
    
    log_message "INFO" "Checking if table $database.$table exists..."
    
    local table_check=$(hive -S -e "USE $database; SHOW TABLES LIKE '$table';" 2>/dev/null | wc -l)
    
    if [ "$table_check" -eq 0 ]; then
        log_message "ERROR" "Table $database.$table does not exist"
        return 1
    fi
    
    log_message "INFO" "Table exists"
    return 0
}

# Estimate row count
estimate_row_count() {
    local database=$1
    local table=$2
    local start_date=$3
    local end_date=$4
    local ref=$5
    
    log_message "INFO" "Estimating row count..."
    
    local count_query="USE $database; 
        SELECT COUNT(*) as estimated_count 
        FROM $table 
        WHERE startdate = $start_date 
        AND enddate = $end_date 
        AND referencenumber = $ref"
    
    local count=$(hive -S -e "$count_query" 2>/dev/null | head -1 | tr -d ' ')
    
    if [[ -z "$count" || "$count" == "NULL" ]]; then
        log_message "WARNING" "Could not estimate row count"
        echo "Unknown"
    else
        log_message "INFO" "Estimated rows: $count"
        echo "$count"
    fi
}

# Generate partition pruning query
generate_partitioned_query() {
    local database=$1
    local table=$2
    local start_date=$3
    local end_date=$4
    local ref=$5
    local limit=$6
    
    # Extract year, month, day from dates
    local start_year=${start_date:0:4}
    local start_month=${start_date:4:2}
    local start_day=${start_date:6:2}
    
    local end_year=${end_date:0:4}
    local end_month=${end_date:4:2}
    local end_day=${end_date:6:2}
    
    # Build partition-aware query
    local query="USE $database; 
        SELECT * FROM $table 
        WHERE startdate = $start_date 
        AND enddate = $end_date 
        AND referencenumber = $ref"
    
    # Add limit if specified
    if [[ -n "$limit" && "$limit" -gt 0 ]]; then
        query="$query LIMIT $limit"
    fi
    
    echo "$query"
}

# Execute Hive query
execute_hive_query() {
    local query=$1
    local output_file=$2
    local format=$3
    
    log_message "INFO" "Executing Hive query..."
    log_message "DEBUG" "Query: $query"
    
    # Set Hive output format
    local hive_opts=""
    case $format in
        csv)
            hive_opts="--outputformat=csv2"
            ;;
        json)
            hive_opts="--outputformat=json"
            ;;
        tsv|*)
            hive_opts="--outputformat=tsv2"
            ;;
    esac
    
    # Execute query
    hive -S $hive_opts -e "$query" 2>>"$LOG_FILE" > "$output_file"
    
    local exit_code=$?
    
    if [ $exit_code -ne 0 ]; then
        log_message "ERROR" "Hive query failed with exit code $exit_code"
        return 1
    fi
    
    log_message "INFO" "Query executed successfully"
    return 0
}

# Generate summary report
generate_summary() {
    local output_file=$1
    local row_count=$2
    local file_size=$3
    local execution_time=$4
    
    local summary_file="${output_file}.summary"
    
    cat > "$summary_file" << EOF
====================================================================
                    EXTRACTION SUMMARY REPORT
====================================================================
Generated: $(date '+%Y-%m-%d %H:%M:%S')
Script: $0

PARAMETERS:
  Start Date: $START_DATE
  End Date: $END_DATE
  Reference Number: $REF_NUMBER
  Database: $DATABASE
  Table: $TABLE
  Output Format: $OUTPUT_FORMAT

RESULTS:
  Output File: $output_file
  Total Rows: $row_count
  File Size: $file_size
  Execution Time: $execution_time seconds

STATUS: SUCCESS
====================================================================
EOF
    
    log_message "INFO" "Summary report generated: $summary_file"
}

#==================== MAIN SCRIPT ====================

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -s|--startdate)
            START_DATE="$2"
            shift 2
            ;;
        -e|--enddate)
            END_DATE="$2"
            shift 2
            ;;
        -r|--ref)
            REF_NUMBER="$2"
            shift 2
            ;;
        -d|--database)
            DATABASE="$2"
            shift 2
            ;;
        -t|--table)
            TABLE="$2"
            shift 2
            ;;
        -o|--output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -f|--format)
            OUTPUT_FORMAT="$2"
            shift 2
            ;;
        -c|--compress)
            COMPRESS="yes"
            shift
            ;;
        -l|--limit)
            LIMIT_ROWS="$2"
            shift 2
            ;;
        -p|--partitioned)
            USE_PARTITIONS="$2"
            shift 2
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            print_colored "$RED" "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Clear screen for better presentation
clear

# Display header
print_colored "$BLUE" "
====================================================================
                    HIVE DATA EXTRACTION TOOL
====================================================================
"

# Set default values if not provided
DATABASE="${DATABASE:-$DEFAULT_DATABASE}"
TABLE="${TABLE:-$DEFAULT_TABLE}"
OUTPUT_DIR="${OUTPUT_DIR:-$DEFAULT_OUTPUT_DIR}"
OUTPUT_FORMAT="${OUTPUT_FORMAT:-tsv}"
COMPRESS="${COMPRESS:-no}"
USE_PARTITIONS="${USE_PARTITIONS:-yes}"

# Validate required parameters
missing_params=0

if [[ -z "$START_DATE" ]]; then
    print_colored "$RED" "✗ Missing start date parameter"
    missing_params=1
fi

if [[ -z "$END_DATE" ]]; then
    print_colored "$RED" "✗ Missing end date parameter"
    missing_params=1
fi

if [[ -z "$REF_NUMBER" ]]; then
    print_colored "$RED" "✗ Missing reference number parameter"
    missing_params=1
fi

if [ $missing_params -eq 1 ]; then
    show_usage
    exit 1
fi

# Validate inputs
print_colored "$YELLOW" "Validating inputs..."
log_message "INFO" "Starting validation"

# Validate dates
if ! validate_date "$START_DATE" "Start date"; then
    exit 1
fi

if ! validate_date "$END_DATE" "End date"; then
    exit 1
fi

# Validate reference
if ! validate_ref "$REF_NUMBER"; then
    exit 1
fi

# Check if start date <= end date
if [[ "$START_DATE" > "$END_DATE" ]]; then
    log_message "ERROR" "Start date ($START_DATE) is after end date ($END_DATE)"
    print_colored "$RED" "✗ Start date must be before or equal to end date"
    exit 1
fi

print_colored "$GREEN" "✓ Input validation passed"
log_message "INFO" "Input validation passed"

# Check Hive connectivity
print_colored "$YELLOW" "Checking Hive connectivity..."
if ! check_hive_connectivity; then
    exit 1
fi
print_colored "$GREEN" "✓ Hive connectivity OK"

# Check if table exists
print_colored "$YELLOW" "Checking table existence..."
if ! check_table_exists "$DATABASE" "$TABLE"; then
    exit 1
fi

# Estimate rows
print_colored "$YELLOW" "Estimating data volume..."
ROW_ESTIMATE=$(estimate_row_count "$DATABASE" "$TABLE" "$START_DATE" "$END_DATE" "$REF_NUMBER")
print_colored "$CYAN" "  Estimated rows: $ROW_ESTIMATE"

# Create output directory
mkdir -p "$OUTPUT_DIR"
log_message "INFO" "Output directory: $OUTPUT_DIR"

# Generate output filename
TIMESTAMP=$(date '+%Y%m%d_%H%M%S')
BASE_FILENAME="${DATABASE}_${TABLE}_${START_DATE}_${END_DATE}_${REF_NUMBER}_${TIMESTAMP}"
OUTPUT_FILE="${OUTPUT_DIR}/${BASE_FILENAME}.${OUTPUT_FORMAT}"

if [[ "$COMPRESS" == "yes" ]]; then
    OUTPUT_FILE="${OUTPUT_FILE}.gz"
fi

# Display execution plan
print_colored "$PURPLE" "
====================================================================
                      EXECUTION PLAN
====================================================================
Start Date      : $START_DATE
End Date        : $END_DATE
Reference       : $REF_NUMBER
Database        : $DATABASE
Table           : $TABLE
Output File     : $OUTPUT_FILE
Output Format   : $OUTPUT_FORMAT
Compression     : $COMPRESS
Row Limit       : ${LIMIT_ROWS:-No limit}
====================================================================
"

# Confirm execution
read -p "Proceed with extraction? (y/n): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    print_colored "$YELLOW" "Extraction cancelled by user"
    exit 0
fi

# Start timing
START_TIME=$(date +%s)

# Generate and execute query
print_colored "$YELLOW" "Generating Hive query..."
QUERY=$(generate_partitioned_query "$DATABASE" "$TABLE" "$START_DATE" "$END_DATE" "$REF_NUMBER" "$LIMIT_ROWS")

print_colored "$YELLOW" "Executing Hive query (this may take a while)..."

if [[ "$COMPRESS" == "yes" ]]; then
    # Execute and compress on the fly
    execute_hive_query "$QUERY" "/dev/stdout" "$OUTPUT_FORMAT" | gzip > "$OUTPUT_FILE"
    EXIT_CODE=${PIPESTATUS[0]}
else
    execute_hive_query "$QUERY" "$OUTPUT_FILE" "$OUTPUT_FORMAT"
    EXIT_CODE=$?
fi

# Check execution status
if [ $EXIT_CODE -ne 0 ]; then
    print_colored "$RED" "✗ Extraction failed! Check log file: $LOG_FILE"
    exit 1
fi

# End timing
END_TIME=$(date +%s)
EXECUTION_TIME=$((END_TIME - START_TIME))

# Get file info
if [[ -f "$OUTPUT_FILE" ]]; then
    ROW_COUNT=$(wc -l < "$OUTPUT_FILE" 2>/dev/null || echo "0")
    if [[ "$COMPRESS" == "yes" ]]; then
        FILE_SIZE=$(du -h "$OUTPUT_FILE" | cut -f1)
        ACTUAL_ROWS=$(zcat "$OUTPUT_FILE" | wc -l)
    else
        FILE_SIZE=$(du -h "$OUTPUT_FILE" | cut -f1)
        ACTUAL_ROWS=$ROW_COUNT
    fi
    
    # Generate summary
    generate_summary "$OUTPUT_FILE" "$ACTUAL_ROWS" "$FILE_SIZE" "$EXECUTION_TIME"
    
    # Success message
    print_colored "$GREEN" "
====================================================================
                    ✓ EXTRACTION COMPLETED SUCCESSFULLY
====================================================================
Output File     : $OUTPUT_FILE
Total Rows      : $ACTUAL_ROWS
File Size       : $FILE_SIZE
Execution Time  : ${EXECUTION_TIME} seconds
Log File        : $LOG_FILE
Summary File    : ${OUTPUT_FILE}.summary
====================================================================
"
    
    # Show sample of data
    print_colored "$CYAN" "Sample of extracted data (first 5 rows):"
    echo "--------------------------------------------------------------------"
    if [[ "$COMPRESS" == "yes" ]]; then
        zcat "$OUTPUT_FILE" | head -5 | column -t -s $'\t'
    else
        head -5 "$OUTPUT_FILE" | column -t -s $'\t'
    fi
    echo "--------------------------------------------------------------------"
    
else
    print_colored "$RED" "✗ Output file not created!"
    exit 1
fi

# Optional: Send notification
# send_notification "$OUTPUT_FILE" "$ACTUAL_ROWS" "$EXECUTION_TIME"

log_message "INFO" "Script completed successfully"
print_colored "$GREEN" "Finished!!!"