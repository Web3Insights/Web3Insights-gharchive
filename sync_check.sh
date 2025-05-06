#!/bin/bash

# Cross-platform date handling
date_cmd() {
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS (BSD date)
        if [[ "$1" == "validate" ]]; then
            # Validate date format
            if date -j -f "%Y-%m-%d" "$2" >/dev/null 2>&1; then
                return 0
            else
                return 1
            fi
        elif [[ "$1" == "timestamp" ]]; then
            # Get timestamp from date
            date -j -f "%Y-%m-%d" "$2" "+%s" 2>/dev/null
        elif [[ "$1" == "next_day" ]]; then
            # Get next day
            next_ts=$(($(date -j -f "%Y-%m-%d" "$2" "+%s" 2>/dev/null) + 86400))
            date -r $next_ts "+%Y-%m-%d" 2>/dev/null
        elif [[ "$1" == "format" ]]; then
            # Format date components
            date -j -f "%Y-%m-%d" "$2" "+$3" 2>/dev/null
        fi
    else
        # Linux (GNU date)
        if [[ "$1" == "validate" ]]; then
            if date -d "$2" >/dev/null 2>&1; then
                return 0
            else
                return 1
            fi
        elif [[ "$1" == "timestamp" ]]; then
            date -d "$2" "+%s"
        elif [[ "$1" == "next_day" ]]; then
            date -d "$2 + 1 day" "+%Y-%m-%d"
        elif [[ "$1" == "format" ]]; then
            date -d "$2" "+$3"
        fi
    fi
}

# Set start date to January 1, 2020
start_date="2020-01-01"
# Set end date, default is current date
end_date="2025-05-01"
# Get current date
current_date=$(date "+%Y-%m-%d")
# Download directory
download_dir="./data"

# Validate end date format
if ! date_cmd validate "$end_date"; then
    echo "Error: Invalid end date '$end_date'. Please use YYYY-MM-DD format"
    exit 1
fi

# Ensure end date doesn't exceed current date
end_timestamp=$(date_cmd timestamp "$end_date")
current_timestamp=$(date_cmd timestamp "$current_date")
if [ $end_timestamp -gt $current_timestamp ]; then
    echo "Warning: End date exceeds current date. Using $current_date instead"
    end_date=$current_date
    end_timestamp=$current_timestamp
fi

echo "Downloading data from $start_date to $end_date"

# Create download directory if it doesn't exist
mkdir -p "$download_dir"

# Verify gzip file integrity
verify_gzip() {
    local file="$1"
    gzip -t "$file" 2>/dev/null
    return $?
}

# Parallel verification of all files for a single day
parallel_verify_day() {
    local year="$1"
    local month="$2"
    local day="$3"
    local year_month_dir="$4"
    local all_files_ok=true
    local temp_file=$(mktemp)

    # Use GNU Parallel to verify files concurrently
    seq 0 23 | parallel --will-cite -j$(nproc) "
        output_file=\"$year_month_dir/$year-$month-$day-{}.json.gz\"
        if [ -f \"\$output_file\" ]; then
            if ! gzip -t \"\$output_file\" 2>/dev/null; then
                echo \"⚠️ Verification failed: \$output_file is corrupted\" >> $temp_file
                echo false >> $temp_file
            fi
        else
            echo \"⚠️ File not found: \$output_file\" >> $temp_file
            echo false >> $temp_file
        fi
    "

    # Check verification results
    if grep -q "false" $temp_file; then
        all_files_ok=false
    fi

    # Output verification results
    cat $temp_file

    # Clean up temp file
    rm -f $temp_file

    echo $all_files_ok
}

# Convert start date to Unix timestamp
start_timestamp=$(date_cmd timestamp "$start_date")

# Ensure GNU Parallel is installed
if ! command -v parallel &>/dev/null; then
    echo "GNU Parallel not installed. Please install before running this script."
    echo "You can install using 'sudo apt-get install parallel' or equivalent package manager command."
    exit 1
fi

# Determine if we should use nproc or sysctl for CPU count
get_cpu_count() {
    if command -v nproc &>/dev/null; then
        nproc
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        sysctl -n hw.ncpu
    else
        echo 4  # Default fallback
    fi
}
CPU_COUNT=$(get_cpu_count)

# Iterate through each day from start date to end date
d="$start_date"
while [ "$(date_cmd timestamp "$d")" -le "$end_timestamp" ]; do
    echo "Processing data for $d..."

    # Extract date components
    year=$(date_cmd format "$d" "%Y")
    month=$(date_cmd format "$d" "%m")
    day=$(date_cmd format "$d" "%d")

    # Create directory by year/month
    year_month_dir="$download_dir/$year/$month"
    mkdir -p "$year_month_dir"

    # Create aria2c input file for the current day
    input_file="$download_dir/aria2c_input_${year}${month}${day}.txt"
    >"$input_file"

    # Flag indicating if any files need downloading
    need_download=false

    # Parallel file integrity check
    echo "Checking file integrity for $d in parallel..."
    temp_verify_file=$(mktemp)

    seq 0 23 | parallel --will-cite -j$CPU_COUNT "
        hour={}
        output_file=\"$year_month_dir/$year-$month-$day-\$hour.json.gz\"
        url=\"https://data.gharchive.org/$year-$month-$day-\$hour.json.gz\"

        if [ -f \"\$output_file\" ]; then
            if gzip -t \"\$output_file\" 2>/dev/null; then
                echo \"✅ \$output_file exists and is valid, skipping download\"
            else
                echo \"⚠️ \$output_file exists but is corrupted, will re-download\"
                rm -f \"\$output_file\"
                echo \"\$url\" >> $temp_verify_file
                echo \"  out=\$output_file\" >> $temp_verify_file
            fi
        else
            echo \"\$url\" >> $temp_verify_file
            echo \"  out=\$output_file\" >> $temp_verify_file
        fi
    "

    # If temp file has content, files need downloading
    if [ -s "$temp_verify_file" ]; then
        cat "$temp_verify_file" >"$input_file"
        need_download=true
    fi

    rm -f "$temp_verify_file"

    # Execute download if needed
    if [ "$need_download" = true ]; then
        echo "Starting download for $d..."
        # Use aria2c with 24 concurrent downloads
        aria2c --input-file="$input_file" \
            --max-concurrent-downloads=24 \
            --max-connection-per-server=8 \
            --split=8 \
            --min-split-size=1M \
            --continue=true \
            --auto-file-renaming=false

        # Check download status
        if [ $? -eq 0 ]; then
            echo "✅ Data for $d downloaded successfully"
        else
            echo "❌ Download failed for $d, check errors and retry"
        fi
    else
        echo "✅ All files for $d already exist and are valid, skipping download"
    fi

    # Move to next day
    d=$(date_cmd next_day "$d")
done

echo "Processing complete! Data from $start_date to $end_date has been processed."
