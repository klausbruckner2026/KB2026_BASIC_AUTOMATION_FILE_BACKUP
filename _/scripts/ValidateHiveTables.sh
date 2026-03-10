#!/bin/bash
#===============================================================================
# Script: hive_table_validator.sh
# Description: Advanced Hive table validation tool for comparing tables across databases
# Author: Enhanced Version
# Version: 3.0
#===============================================================================

#===============================================================================
# CONFIGURATION SECTION
#===============================================================================

# Default database names
DEFAULT_ORIG_DB="hiveSourceOfTruthDatabase"
DEFAULT_RESULT_DB="hiveDatabasetoValidate"

# Base directory for validation outputs
BASE_DIR="/tmp/validation"

# Hive settings
HIVE_QUEUE="default"
HIVE_TEZ_QUEUE="default"

# Performance settings
SPLIT_NUM_LINES=301024  # Lines per chunk for large tables
PARALLEL_JOBS=4         # Number of parallel jobs
SORT_MEMORY="30%"       # Memory for sorting

# Output formats
REPORT_FORMAT="text"    # text, html, json, email
COMPRESS_OUTPUT=true    # Compress output files

# Email settings (if REPORT_FORMAT=email)
SMTP_SERVER="localhost"
SMTP_PORT="25"
EMAIL_RECIPIENT="data-team@example.com"
EMAIL_SENDER="hive-validator@example.com"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
NC='\033[0m' # No Color

#===============================================================================
# FUNCTION DEFINITIONS
#===============================================================================

#-------------------------------------------------------------------------------
# Usage information
#-------------------------------------------------------------------------------
show_usage() {
    cat << EOF
${BLUE}================================================================================
                    HIVE TABLE VALIDATION TOOL - USAGE
================================================================================${NC}

Usage: $0 [OPTIONS] TABLE1[:EXCLUDE_COLUMNS] TABLE2[:EXCLUDE_COLUMNS] ...

OPTIONS:
    -o, --orig-db DB        Original database name (default: ${DEFAULT_ORIG_DB})
    -r, --result-db DB      Result database name (default: ${DEFAULT_RESULT_DB})
    -b, --base-dir DIR      Base directory for outputs (default: /tmp/validation)
    -q, --queue QUEUE       Hive queue name (default: default)
    -s, --split-lines N     Lines per split chunk (default: 301024)
    -p, --parallel N        Parallel jobs count (default: 4)
    -f, --format FORMAT     Report format: text|html|json|email (default: text)
    -e, --email RECIPIENT   Email recipient for email format
    --no-compress          Disable output compression
    --skip-count-check     Skip row count validation
    --skip-schema-check    Skip schema validation
    --quick-mode           Quick validation (sample data only)
    --help                 Show this help message

EXAMPLES:
    # Basic validation
    $0 customers orders products
    
    # Validate with excluded columns
    $0 customers:col1,col2 orders:timestamp_field
    
    # Specify custom databases
    $0 -o prod_db -r test_db customers
    
    # Generate HTML report
    $0 -f html customers orders
    
    # Quick validation with sampling
    $0 --quick-mode -f email -e team@company.com customers

EOF
}

#-------------------------------------------------------------------------------
# Logging function
#-------------------------------------------------------------------------------
log_message() {
    local level=$1
    local message=$2
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    case $level in
        "ERROR")   echo -e "${RED}[$timestamp] ERROR: $message${NC}" ;;
        "WARNING") echo -e "${YELLOW}[$timestamp] WARNING: $message${NC}" ;;
        "SUCCESS") echo -e "${GREEN}[$timestamp] SUCCESS: $message${NC}" ;;
        "INFO")    echo -e "${CYAN}[$timestamp] INFO: $message${NC}" ;;
        "DEBUG")   echo -e "${PURPLE}[$timestamp] DEBUG: $message${NC}" ;;
        *)         echo "[$timestamp] $message" ;;
    esac
    
    # Also write to log file
    echo "[$timestamp] [$level] $message" >> "${GLOBAL_REPORT}"
}

#-------------------------------------------------------------------------------
# Error handling function
#-------------------------------------------------------------------------------
handle_error() {
    local exit_code=$1
    local error_message=$2
    local line_number=$3
    
    log_message "ERROR" "Error at line ${line_number}: ${error_message} (Exit code: ${exit_code})"
    
    # Cleanup on error
    cleanup_temp_files
    
    exit ${exit_code}
}

#-------------------------------------------------------------------------------
# Trap for errors
#-------------------------------------------------------------------------------
trap 'handle_error $? "Unexpected error" $LINENO' ERR

#-------------------------------------------------------------------------------
# Check prerequisites
#-------------------------------------------------------------------------------
check_prerequisites() {
    log_message "INFO" "Checking prerequisites..."
    
    # Check Hive availability
    if ! command -v hive &> /dev/null; then
        handle_error 1 "Hive command not found" $LINENO
    fi
    
    # Check Hadoop availability
    if ! command -v hadoop &> /dev/null; then
        handle_error 1 "Hadoop command not found" $LINENO
    fi
    
    # Test Hive connection
    if ! hive -e "show databases;" &> /dev/null; then
        handle_error 1 "Cannot connect to Hive" $LINENO
    fi
    
    log_message "SUCCESS" "Prerequisites satisfied"
}

#-------------------------------------------------------------------------------
# Validate databases exist
#-------------------------------------------------------------------------------
validate_databases() {
    log_message "INFO" "Validating databases: $ORIG_DB, $RESULT_DB"
    
    local databases=$(hive -S -e "show databases;" 2>/dev/null)
    
    if ! echo "$databases" | grep -qw "$ORIG_DB"; then
        handle_error 1 "Original database '$ORIG_DB' does not exist" $LINENO
    fi
    
    if ! echo "$databases" | grep -qw "$RESULT_DB"; then
        handle_error 1 "Result database '$RESULT_DB' does not exist" $LINENO
    fi
    
    log_message "SUCCESS" "Databases validated"
}

#-------------------------------------------------------------------------------
# Validate tables exist
#-------------------------------------------------------------------------------
validate_tables() {
    local table=$1
    local db=$2
    
    log_message "DEBUG" "Checking if table $db.$table exists"
    
    local tables=$(hive -S -e "use $db; show tables like '$table';" 2>/dev/null)
    
    if [ -z "$tables" ]; then
        return 1
    fi
    return 0
}

#-------------------------------------------------------------------------------
# Compare table schemas
#-------------------------------------------------------------------------------
compare_schemas() {
    local table=$1
    local orig_schema_file="$BASE_DIR/$ORIG_DB/${table}_schema.txt"
    local result_schema_file="$BASE_DIR/$RESULT_DB/${table}_schema.txt"
    
    log_message "INFO" "Comparing schemas for table $table"
    
    # Extract schemas
    hive -S -e "use $ORIG_DB; describe $table;" > "$orig_schema_file" 2>/dev/null
    hive -S -e "use $RESULT_DB; describe $table;" > "$result_schema_file" 2>/dev/null
    
    # Compare schemas
    if ! diff -q "$orig_schema_file" "$result_schema_file" &>/dev/null; then
        log_message "WARNING" "Schema mismatch detected for table $table"
        return 1
    fi
    
    log_message "SUCCESS" "Schemas match for table $table"
    return 0
}

#-------------------------------------------------------------------------------
# Get table statistics
#-------------------------------------------------------------------------------
get_table_stats() {
    local db=$1
    local table=$2
    
    log_message "DEBUG" "Getting statistics for $db.$table"
    
    # Try to get stats from Hive metastore
    local stats=$(hive -S -e "
        use $db;
        analyze table $table compute statistics;
        describe formatted $table;
    " 2>/dev/null | grep -E "numRows|totalSize|rawDataSize" | awk '{print $2}' | tr -d ' ')
    
    if [ -z "$stats" ]; then
        # Fallback to counting
        local count=$(hive -S -e "use $db; select count(*) from $table;" 2>/dev/null | head -1)
        echo "$count|unknown|unknown"
    else
        echo "$stats"
    fi
}

#-------------------------------------------------------------------------------
# Generate Hive query for data extraction
#-------------------------------------------------------------------------------
generate_hive_query() {
    local db=$1
    local table=$2
    local exclude_cols=$3
    local output_dir=$4
    local is_sample=$5
    
    local query=""
    local select_clause=""
    
    # Add sampling if quick mode
    if [ "$QUICK_MODE" = true ] && [ "$is_sample" = true ]; then
        query="set hive.limit.pushdown.memory usage=0.5; "
    fi
    
    query="${query}INSERT OVERWRITE LOCAL DIRECTORY '$output_dir' "
    query="${query}ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' "
    
    # Build select clause
    if [ -n "$exclude_cols" ]; then
        # Handle excluded columns
        local exclude_pattern=$(echo "$exclude_cols" | sed 's/,/|/g')
        query="${query}SELECT \`($exclude_pattern)?+.+\` "
    else
        query="${query}SELECT * "
    fi
    
    query="${query}FROM $db.$table"
    
    # Add sampling if quick mode
    if [ "$QUICK_MODE" = true ] && [ "$is_sample" = true ]; then
        query="${query} TABLESAMPLE(10 PERCENT)"
    fi
    
    echo "$query"
}

#-------------------------------------------------------------------------------
# Extract table data
#-------------------------------------------------------------------------------
extract_table_data() {
    local db=$1
    local table=$2
    local exclude_cols=$3
    local output_dir="$CURRENT_BASE_DIR/$db/$table"
    local temp_dir="$CURRENT_BASE_DIR/tmp/${db}_${table}"
    
    mkdir -p "$output_dir" "$temp_dir"
    
    log_message "INFO" "Extracting data from $db.$table"
    
    # Generate and execute query
    local query_file="$temp_dir/query.hql"
    generate_hive_query "$db" "$table" "$exclude_cols" "$output_dir" true > "$query_file"
    
    # Execute Hive query
    hive --hiveconf tez.queue.name=$HIVE_TEZ_QUEUE \
         --hiveconf mapreduce.job.queuename=$HIVE_QUEUE \
         -f "$query_file" &> "$temp_dir/hive.log"
    
    if [ $? -ne 0 ]; then
        log_message "ERROR" "Failed to extract data from $db.$table"
        return 1
    fi
    
    # Check if extraction produced files
    if [ ! -d "$output_dir" ] || [ -z "$(ls -A $output_dir 2>/dev/null)" ]; then
        log_message "ERROR" "No data extracted from $db.$table"
        return 1
    fi
    
    log_message "SUCCESS" "Data extracted from $db.$table"
    return 0
}

#-------------------------------------------------------------------------------
# Process and sort extracted data
#-------------------------------------------------------------------------------
process_table_data() {
    local db=$1
    local table=$2
    local data_dir="$CURRENT_BASE_DIR/$db/$table"
    local temp_dir="$CURRENT_BASE_DIR/tmp/${db}_${table}"
    
    log_message "INFO" "Processing data for $db.$table"
    
    cd "$data_dir" || return 1
    
    # Concatenate all part files
    cat 0* > all_data.tmp 2>/dev/null
    
    if [ ! -s all_data.tmp ]; then
        log_message "ERROR" "No data to process for $db.$table"
        return 1
    fi
    
    # Count lines
    local line_count=$(wc -l < all_data.tmp)
    log_message "INFO" "Rows extracted from $db.$table: $line_count"
    
    # Sort data
    log_message "INFO" "Sorting data for $db.$table"
    sort -S $SORT_MEMORY --temporary-directory="$temp_dir" \
         --numeric-sort all_data.tmp -o sorted.tmp
    
    # Split if needed
    if [ $line_count -gt $SPLIT_NUM_LINES ]; then
        log_message "INFO" "Splitting large dataset into chunks"
        split --suffix-length=3 --lines=$SPLIT_NUM_LINES sorted.tmp sorted-
        rm -f sorted.tmp
    else
        mv sorted.tmp sorted
    fi
    
    # Clean up part files
    rm -f 0* all_data.tmp
    
    # Compress if enabled
    if [ "$COMPRESS_OUTPUT" = true ]; then
        log_message "INFO" "Compressing output files"
        gzip -f sorted* 2>/dev/null
    fi
    
    cd - >/dev/null
    
    echo "$line_count"
}

#-------------------------------------------------------------------------------
# Compare two datasets
#-------------------------------------------------------------------------------
compare_datasets() {
    local table=$1
    local orig_count=$2
    local result_count=$3
    local orig_dir="$CURRENT_BASE_DIR/$ORIG_DB/$table"
    local result_dir="$CURRENT_BASE_DIR/$RESULT_DB/$table"
    
    log_message "INFO" "Comparing datasets for table $table"
    
    local num_diff=0
    local chunks_with_errors=""
    local mismatch_details=""
    
    # Check row count
    if [ "$SKIP_COUNT_CHECK" = false ] && [ "$orig_count" -ne "$result_count" ]; then
        local error_msg="Row count mismatch: Original=$orig_count, Result=$result_count"
        log_message "ERROR" "$error_msg"
        mismatch_details="$error_msg\n"
        NUM_ERRORS=$((NUM_ERRORS + 1))
    fi
    
    # Compare data
    if [ -d "$orig_dir" ] && [ -d "$result_dir" ]; then
        cd "$orig_dir"
        
        if [ -f "sorted" ] || [ -f "sorted.gz" ]; then
            # Unsplitted dataset
            local orig_file="sorted"
            local result_file="$result_dir/sorted"
            
            if [ "$COMPRESS_OUTPUT" = true ]; then
                orig_file="sorted.gz"
                result_file="$result_dir/sorted.gz"
            fi
            
            if [ -f "$result_file" ]; then
                if [ "$COMPRESS_OUTPUT" = true ]; then
                    num_diff=$(diff <(zcat "$orig_file") <(zcat "$result_file") | grep -c '^[<>]' || true)
                else
                    num_diff=$(diff "$orig_file" "$result_file" | grep -c '^[<>]' || true)
                fi
            fi
        else
            # Splitted dataset
            for file_chunk in sorted-*; do
                local chunk_basename=$(basename "$file_chunk")
                local result_chunk="$result_dir/$chunk_basename"
                
                if [ ! -e "$result_chunk" ]; then
                    continue
                fi
                
                if [ "$COMPRESS_OUTPUT" = true ]; then
                    local chunk_diff=$(diff <(zcat "$file_chunk") <(zcat "$result_chunk") | grep -c '^[<>]' || true)
                else
                    local chunk_diff=$(diff "$file_chunk" "$result_chunk" | grep -c '^[<>]' || true)
                fi
                
                if [ $chunk_diff -gt 0 ]; then
                    num_diff=$((num_diff + chunk_diff))
                    chunks_with_errors="${chunks_with_errors}${chunk_diff}\t\t${chunk_basename}\n"
                fi
            done
        fi
        
        cd - >/dev/null
    fi
    
    # Store results
    TABLE_RESULTS["$table,orig_count"]=$orig_count
    TABLE_RESULTS["$table,result_count"]=$result_count
    TABLE_RESULTS["$table,num_diff"]=$num_diff
    TABLE_RESULTS["$table,chunks"]=$(echo -e "$chunks_with_errors" | base64 -w 0 2>/dev/null || echo "")
    
    if [ $num_diff -gt 0 ]; then
        NUM_ERRORS=$((NUM_ERRORS + 1))
        log_message "WARNING" "Found $num_diff differences in table $table"
    else
        log_message "SUCCESS" "Table $table validated successfully"
    fi
}

#-------------------------------------------------------------------------------
# Generate HTML report
#-------------------------------------------------------------------------------
generate_html_report() {
    local report_file="$GLOBAL_REPORT.html"
    
    cat > "$report_file" << EOF
<!DOCTYPE html>
<html>
<head>
    <title>Hive Table Validation Report</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        h1 { color: #333; }
        h2 { color: #666; }
        table { border-collapse: collapse; width: 100%; margin-bottom: 20px; }
        th { background-color: #4CAF50; color: white; padding: 10px; }
        td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        tr:nth-child(even) { background-color: #f2f2f2; }
        .error { color: red; font-weight: bold; }
        .warning { color: orange; }
        .success { color: green; }
        .summary { background-color: #e7f3fe; padding: 15px; border-radius: 5px; }
    </style>
</head>
<body>
    <h1>Hive Table Validation Report</h1>
    <div class="summary">
        <p><strong>Generated:</strong> $(date)</p>
        <p><strong>Original Database:</strong> $ORIG_DB</p>
        <p><strong>Result Database:</strong> $RESULT_DB</p>
        <p><strong>Tables Validated:</strong> ${#TABLES[@]}</p>
        <p><strong>Errors Found:</strong> <span class="${NUM_ERRORS -gt 0 ? 'error' : 'success'}">$NUM_ERRORS</span></p>
    </div>
    
    <h2>Validation Results</h2>
    <table>
        <tr>
            <th>Table Name</th>
            <th>Original Rows</th>
            <th>Result Rows</th>
            <th>Differences</th>
            <th>Status</th>
        </tr>
EOF
    
    for table in "${TABLES[@]}"; do
        local orig_count=${TABLE_RESULTS["$table,orig_count"]}
        local result_count=${TABLE_RESULTS["$table,result_count"]}
        local num_diff=${TABLE_RESULTS["$table,num_diff"]}
        local status="success"
        local status_class="success"
        
        if [ "$num_diff" -gt 0 ] || [ "$orig_count" != "$result_count" ]; then
            status="error"
            status_class="error"
        fi
        
        cat >> "$report_file" << EOF
        <tr>
            <td>$table</td>
            <td>$orig_count</td>
            <td>$result_count</td>
            <td>$num_diff</td>
            <td class="$status_class">$status</td>
        </tr>
EOF
    done
    
    cat >> "$report_file" << EOF
    </table>
    
    <h2>Detailed Differences</h2>
EOF
    
    for table in "${TABLES[@]}"; do
        local num_diff=${TABLE_RESULTS["$table,num_diff"]}
        if [ "$num_diff" -gt 0 ]; then
            local chunks_base64=${TABLE_RESULTS["$table,chunks"]}
            local chunks=$(echo "$chunks_base64" | base64 -d 2>/dev/null)
            
            cat >> "$report_file" << EOF
    <h3>$table</h3>
    <pre class="error">$chunks</pre>
EOF
        fi
    done
    
    cat >> "$report_file" << EOF
</body>
</html>
EOF
    
    log_message "SUCCESS" "HTML report generated: $report_file"
    echo "$report_file"
}

#-------------------------------------------------------------------------------
# Generate JSON report
#-------------------------------------------------------------------------------
generate_json_report() {
    local report_file="$GLOBAL_REPORT.json"
    
    cat > "$report_file" << EOF
{
    "report": {
        "generated": "$(date -Iseconds)",
        "original_database": "$ORIG_DB",
        "result_database": "$RESULT_DB",
        "tables_validated": ${#TABLES[@]},
        "errors_found": $NUM_ERRORS,
        "results": [
EOF
    
    local first=true
    for table in "${TABLES[@]}"; do
        if [ "$first" = true ]; then
            first=false
        else
            echo "," >> "$report_file"
        fi
        
        cat >> "$report_file" << EOF
            {
                "table": "$table",
                "original_rows": ${TABLE_RESULTS["$table,orig_count"]},
                "result_rows": ${TABLE_RESULTS["$table,result_count"]},
                "differences": ${TABLE_RESULTS["$table,num_diff"]}
            }
EOF
    done
    
    cat >> "$report_file" << EOF
        ]
    }
}
EOF
    
    log_message "SUCCESS" "JSON report generated: $report_file"
    echo "$report_file"
}

#-------------------------------------------------------------------------------
# Send email report
#-------------------------------------------------------------------------------
send_email_report() {
    local report_file=$1
    local subject="Hive Table Validation Report - $(date +%Y-%m-%d)"
    
    if [ ! -f "$report_file" ]; then
        log_message "ERROR" "Report file not found: $report_file"
        return 1
    fi
    
    # Create email content
    local email_body=""
    if [[ "$report_file" == *.html ]]; then
        email_body=$(cat "$report_file")
        local mime_type="text/html"
    else
        email_body=$(cat "$report_file")
        local mime_type="text/plain"
    fi
    
    # Send email (using mail command or sendmail)
    if command -v mail &>/dev/null; then
        echo "$email_body" | mail -s "$subject" -a "Content-Type: $mime_type" "$EMAIL_RECIPIENT"
    else
        # Fallback to sendmail
        (
            echo "To: $EMAIL_RECIPIENT"
            echo "From: $EMAIL_SENDER"
            echo "Subject: $subject"
            echo "Content-Type: $mime_type"
            echo ""
            echo "$email_body"
        ) | sendmail -t
    fi
    
    log_message "SUCCESS" "Email report sent to $EMAIL_RECIPIENT"
}

#-------------------------------------------------------------------------------
# Cleanup temporary files
#-------------------------------------------------------------------------------
cleanup_temp_files() {
    if [ "$KEEP_TEMP_FILES" != true ]; then
        log_message "INFO" "Cleaning up temporary files"
        rm -rf "$CURRENT_BASE_DIR/tmp" 2>/dev/null
    fi
}

#-------------------------------------------------------------------------------
# Main validation function for a table
#-------------------------------------------------------------------------------
validate_table() {
    local table_config=$1
    local table=$(echo "$table_config" | cut -d: -f1)
    local exclude_cols=$(echo "$table_config" | cut -s -d: -f2 | sed 's/,/|/g')
    
    log_message "INFO" "========================================"
    log_message "INFO" "Validating table: $table"
    if [ -n "$exclude_cols" ]; then
        log_message "INFO" "Excluded columns: $exclude_cols"
    fi
    log_message "INFO" "========================================"
    
    # Check if tables exist
    if ! validate_tables "$table" "$ORIG_DB"; then
        log_message "ERROR" "Table $ORIG_DB.$table does not exist"
        return 1
    fi
    
    if ! validate_tables "$table" "$RESULT_DB"; then
        log_message "ERROR" "Table $RESULT_DB.$table does not exist"
        return 1
    fi
    
    # Compare schemas if not skipped
    if [ "$SKIP_SCHEMA_CHECK" = false ]; then
        if ! compare_schemas "$table"; then
            log_message "WARNING" "Schema validation failed for table $table"
        fi
    fi
    
    # Extract data from both databases in parallel
    log_message "INFO" "Starting parallel data extraction"
    
    # Create process IDs array
    declare -a pids=()
    
    # Extract original data
    extract_table_data "$ORIG_DB" "$table" "$exclude_cols" &
    pids+=($!)
    
    # Extract result data
    extract_table_data "$RESULT_DB" "$table" "$exclude_cols" &
    pids+=($!)
    
    # Wait for both extractions to complete
    for pid in "${pids[@]}"; do
        wait $pid
    done
    
    # Process data
    orig_count=$(process_table_data "$ORIG_DB" "$table")
    result_count=$(process_table_data "$RESULT_DB" "$table")
    
    # Compare datasets
    compare_datasets "$table" "$orig_count" "$result_count"
}

#===============================================================================
# MAIN SCRIPT EXECUTION
#===============================================================================

# Parse command line arguments
TABLES=()
ORIG_DB="$DEFAULT_ORIG_DB"
RESULT_DB="$DEFAULT_RESULT_DB"
REPORT_FORMAT="text"
SKIP_COUNT_CHECK=false
SKIP_SCHEMA_CHECK=false
QUICK_MODE=false
KEEP_TEMP_FILES=false
EMAIL_RECIPIENT=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -o|--orig-db)
            ORIG_DB="$2"
            shift 2
            ;;
        -r|--result-db)
            RESULT_DB="$2"
            shift 2
            ;;
        -b|--base-dir)
            BASE_DIR="$2"
            shift 2
            ;;
        -q|--queue)
            HIVE_QUEUE="$2"
            HIVE_TEZ_QUEUE="$2"
            shift 2
            ;;
        -s|--split-lines)
            SPLIT_NUM_LINES="$2"
            shift 2
            ;;
        -p|--parallel)
            PARALLEL_JOBS="$2"
            shift 2
            ;;
        -f|--format)
            REPORT_FORMAT="$2"
            shift 2
            ;;
        -e|--email)
            EMAIL_RECIPIENT="$2"
            REPORT_FORMAT="email"
            shift 2
            ;;
        --no-compress)
            COMPRESS_OUTPUT=false
            shift
            ;;
        --skip-count-check)
            SKIP_COUNT_CHECK=true
            shift
            ;;
        --skip-schema-check)
            SKIP_SCHEMA_CHECK=true
            shift
            ;;
        --quick-mode)
            QUICK_MODE=true
            shift
            ;;
        --keep-temp)
            KEEP_TEMP_FILES=true
            shift
            ;;
        --help)
            show_usage
            exit 0
            ;;
        -*)
            log_message "ERROR" "Unknown option: $1"
            show_usage
            exit 1
            ;;
        *)
            TABLES+=("$1")
            shift
            ;;
    esac
done

# Check if tables were provided
if [ ${#TABLES[@]} -eq 0 ]; then
    log_message "ERROR" "No tables specified for validation"
    show_usage
    exit 1
fi

# Create base directory
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
CURRENT_BASE_DIR="$BASE_DIR/${ORIG_DB}_${RESULT_DB}_${TIMESTAMP}"
mkdir -p "$CURRENT_BASE_DIR/tmp"

# Global report file
GLOBAL_REPORT="$CURRENT_BASE_DIR/validation_report.txt"
NUM_ERRORS=0

# Declare associative array for results
declare -A TABLE_RESULTS

# Clear screen
clear

# Display header
echo -e "${BLUE}"
echo "================================================================================"
echo "                    HIVE TABLE VALIDATION TOOL v3.0"
echo "================================================================================"
echo -e "${NC}"

# Start main execution
log_message "INFO" "Starting Hive table validation"
log_message "INFO" "Original Database: $ORIG_DB"
log_message "INFO" "Result Database: $RESULT_DB"
log_message "INFO" "Tables to validate: ${TABLES[*]}"
log_message "INFO" "Output directory: $CURRENT_BASE_DIR"

# Check prerequisites
check_prerequisites

# Validate databases
validate_databases

# Process each table
for table_config in "${TABLES[@]}"; do
    validate_table "$table_config"
done

# Generate reports
log_message "INFO" "Generating validation reports"

case $REPORT_FORMAT in
    html)
        report_file=$(generate_html_report)
        ;;
    json)
        report_file=$(generate_json_report)
        ;;
    email)
        if [ -n "$EMAIL_RECIPIENT" ]; then
            report_file=$(generate_html_report)
            send_email_report "$report_file"
        else
            log_message "ERROR" "Email recipient not specified for email format"
            generate_summary_report
        fi
        ;;
    *)
        generate_summary_report
        report_file="$GLOBAL_REPORT"
        ;;
esac

# Cleanup
cleanup_temp_files

# Final summary
echo -e "${GREEN}"
echo "================================================================================"
echo "                    VALIDATION COMPLETED"
echo "================================================================================"
echo -e "${NC}"
log_message "SUCCESS" "Validation completed with $NUM_ERRORS errors"
log_message "SUCCESS" "Report saved to: $report_file"
log_message "SUCCESS" "All outputs saved to: $CURRENT_BASE_DIR"

exit $NUM_ERRORS