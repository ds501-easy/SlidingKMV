package com.example.slidingdistinctcounter;

/**
 * Represents a data item with a timestamp label for experiments.
 */
public class DataItem {
    private final Object value;
    private final long timestamp;
    
    /**
     * Creates a new DataItem with the specified value and timestamp.
     * 
     * @param value The data value
     * @param timestamp The timestamp when this item was created/processed
     */
    public DataItem(Object value, long timestamp) {
        this.value = value;
        this.timestamp = timestamp;
    }
    
    /**
     * Gets the data value.
     * 
     * @return The data value
     */
    public Object getValue() {
        return value;
    }
    
    /**
     * Gets the timestamp.
     * 
     * @return The timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }
    
    @Override
    public String toString() {
        return String.format("DataItem{value=%s, timestamp=%d}", value, timestamp);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        DataItem dataItem = (DataItem) obj;
        return timestamp == dataItem.timestamp && 
               (value != null ? value.equals(dataItem.value) : dataItem.value == null);
    }
    
    @Override
    public int hashCode() {
        return value != null ? value.hashCode() : 0;
    }
}