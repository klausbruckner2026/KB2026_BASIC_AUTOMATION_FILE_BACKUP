#!/bin/bash

#====================================================================
# Script: hive_data_extract_interactive.sh
# Description: Interactive version with menu-driven interface
#====================================================================

# Color definitions
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Function to draw a line
draw_line() {
    echo "================================================================"
}

# Header
clear
draw_line
echo -e "${BLUE}                    HIVE DATA EXTRACTION TOOL${NC}"
draw_line
echo ""

# Get inputs with validation
while true; do
    read -p "$(echo -e ${YELLOW}"Enter start date (YYYYMMDD): "${NC}) " START_DATE
    if [[ $START_DATE =~ ^[0-9]{8}$ ]]; then
        break
    else
        echo -e "${RED}Invalid format. Please use YYYYMMDD${NC}"
    fi
done

while true; do
    read -p "$(echo -e ${YELLOW}"Enter end date (YYYYMMDD): "${NC}) " END_DATE
    if [[ $END_DATE =~ ^[0-9]{8}$ ]]; then
        break
    else
        echo -e "${RED}Invalid format. Please use YYYYMMDD${NC}"
    fi
done

read -p "$(echo -e ${YELLOW}"Enter reference number: "${NC}) " REF_NUMBER

# Database and table options
echo ""
echo "Available databases:"
hive -e "show databases;" 2>/dev/null | grep -v "default" | cat -n

read -p "$(echo -e ${YELLOW}"Enter database name [default]: "${NC}) " DATABASE
DATABASE=${DATABASE:-default}

echo ""
echo "Available tables in $DATABASE:"
hive -e "use $DATABASE; show tables;" 2>/dev/null | cat -n

read -p "$(echo -e ${YELLOW}"Enter table name: "${NC}) " TABLE

# Output format selection
echo ""
echo "Select output format:"
echo "1) TSV (Tab-separated)"
echo "2) CSV (Comma-separated)"
echo "3) JSON"
read -p "Choice [1]: " FORMAT_CHOICE

case $FORMAT_CHOICE in
    2) FORMAT="csv" ;;
    3) FORMAT="json" ;;
    *) FORMAT="tsv" ;;
esac

# Confirm details
echo ""
draw_line
echo -e "${BLUE}EXTRACTION DETAILS${NC}"
draw_line
echo "Start Date    : $START_DATE"
echo "End Date      : $END_DATE"
echo "Reference     : $REF_NUMBER"
echo "Database      : $DATABASE"
echo "Table         : $TABLE"
echo "Output Format : $FORMAT"
draw_line

read -p "Proceed with extraction? (y/n): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}Extraction cancelled${NC}"
    exit 0
fi

# Create output directory
OUTPUT_DIR="/home/kapru/output"
mkdir -p "$OUTPUT_DIR"
OUTPUT_FILE="${OUTPUT_DIR}/${DATABASE}_${TABLE}_${START_DATE}_${END_DATE}_${REF_NUMBER}.${FORMAT}"

# Build query based on format
case $FORMAT in
    csv)
        HIVE_OPTS="--outputformat=csv2"
        ;;
    json)
        HIVE_OPTS="--outputformat=json"
        ;;
    *)
        HIVE_OPTS="--outputformat=tsv2"
        ;;
esac

# Execute query
echo -e "${YELLOW}Executing Hive query...${NC}"
hive -S $HIVE_OPTS -e "
    USE $DATABASE;
    SELECT * FROM $TABLE 
    WHERE startdate = '$START_DATE' 
    AND enddate = '$END_DATE' 
    AND referencenumber = '$REF_NUMBER';" > "$OUTPUT_FILE"

# Check result
if [ $? -eq 0 ] && [ -s "$OUTPUT_FILE" ]; then
    ROWS=$(wc -l < "$OUTPUT_FILE")
    echo -e "${GREEN}✓ Extraction completed successfully${NC}"
    echo "Output file: $OUTPUT_FILE"
    echo "Total rows: $ROWS"
    
    # Show sample
    echo ""
    echo "Sample data (first 3 rows):"
    head -3 "$OUTPUT_FILE" | column -t -s $'\t'
else
    echo -e "${RED}✗ Extraction failed or returned no data${NC}"
fi

echo -e "${GREEN}Finished!!!${NC}"