# Adjusted Timestamp (AT) Periodic Cleaning

## Overview
With the AT (Adjusted Timestamp) implementation, the sliding window requires **periodic cleaning** to maintain accuracy and memory efficiency.

## Why Periodic Cleaning is Necessary

The AT method uses modular arithmetic to compress timestamps:
- Timestamps are stored as `vAT = t % (2*N)` 
- This creates ambiguity after `2*N` time units
- Outdated entries can appear "valid" if not cleaned regularly

## Cleaning Strategy

### 1. **periodicClean() Method**
Added to `SlidingKMVSketch` class:
```java
public void periodicClean(long currentTime)
```

**What it does:**
- Iterates through all buckets
- Calls `clean(T)` on each entry's AdjustedTimestamp
- Resets outdated entries to empty state
- Updates head pointers
- Updates bucket lock status

**When to call:**
- Every N time units (window size)
- Before critical query operations
- At regular intervals (e.g., every N/2 or N/4 for better accuracy)

### 2. **Usage Example**

```java
// Initialize sketch
SlidingKMVSketch sketch = new SlidingKMVSketch(1000, 64, 100);

long currentTime = 0;
long lastCleanTime = 0;
long cleanInterval = 1000; // Clean every N time units

// Process stream
for (DataItem item : dataStream) {
    currentTime = item.getTimestamp();
    
    // Periodic cleaning
    if (currentTime - lastCleanTime >= cleanInterval) {
        sketch.periodicClean(currentTime);
        lastCleanTime = currentTime;
    }
    
    // Record item
    sketch.recordItem(item.getFlowLabel(), item.getElementID(), currentTime);
    
    // Query cardinality
    double estimate = sketch.estimateCardinality();
}
```

## Cleaning Frequency Trade-offs

| Interval | Memory Efficiency | Computation Cost | Accuracy |
|----------|------------------|------------------|----------|
| Every N | High | Low | Good |
| Every N/2 | Very High | Medium | Very Good |
| Every N/4 | Highest | High | Excellent |
| Lazy (only during operations) | Low | Lowest | Acceptable |

## Recommendation

**For production use:** Clean every `N/2` to `N` time units
- Balances memory efficiency and computation cost
- Ensures outdated entries don't accumulate
- Maintains accuracy of cardinality estimates

## Implementation Changes

All timestamp operations now use AT methods:
- `AdjustedTimestamp.record(t)` - Store timestamp
- `AdjustedTimestamp.lookup(T)` - Check if in window
- `AdjustedTimestamp.clean(T)` - Remove if outdated

This ensures consistent behavior across the entire sketch implementation.
