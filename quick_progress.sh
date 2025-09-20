#!/bin/bash
# Quick Progress Check - One-liner style
echo "🎵 Quick Progress Check [$(date '+%H:%M:%S')]"
echo "=========================================="

# Get latest progress from all log files
total=0
active=0

for i in {0..5}; do
    # Check if process is running
    if [[ -f "speed_${i}.pid" ]] && kill -0 $(cat "speed_${i}.pid" 2>/dev/null) 2>/dev/null; then
        status="🟢"
        ((active++))
    else
        status="🔴"
    fi
    
    # Get latest progress
    progress=0
    if [[ -f "speed_${i}.log" ]]; then
        progress=$(tail -10 "speed_${i}.log" | grep "Progress:" | tail -1 | grep -o '[0-9]\+' | head -1)
        progress=${progress:-0}
    fi
    
    # Add to total
    total=$((total + progress))
    
    # Show process status
    printf "P%d: %6s songs %s\n" $i $progress "$status"
done

echo "=========================================="
printf "Total: %s songs | Active: %d/6\n" $(printf "%'d" $total) $active

# Calculate percentage (bash doesn't handle floats well, so approximate)
if [ $total -gt 0 ]; then
    percentage=$(( total * 100 / 630000 ))
    echo "Progress: ${percentage}% of ~630K total"
fi