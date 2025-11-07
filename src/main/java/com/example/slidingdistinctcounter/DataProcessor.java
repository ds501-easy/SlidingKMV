package com.example.slidingdistinctcounter;

import java.util.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Data processor that loads and labels datasets with timestamps for experiments.
 */
public class DataProcessor {
    
    /**
     * Loads data from a file and labels each item with a timestamp.
     * 
     * @param filePath Path to the data file
     * @return List of labeled data items
     * @throws IOException If file reading fails
     */
    public static List<DataItem> loadDataFromFile(String filePath) throws IOException {
        List<DataItem> dataItems = new ArrayList<>();
        List<String> lines = Files.readAllLines(Paths.get(filePath));
        
        long baseTimestamp = System.currentTimeMillis();
        
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i).trim();
            if (!line.isEmpty()) {
                // Parse the data value (could be integer, string, etc.)
                Object value = parseValue(line);
                // Create timestamp with incremental offset
                long timestamp = baseTimestamp + i;
                dataItems.add(new DataItem(value, timestamp));
            }
        }
        
        return dataItems;
    }
    
    /**
     * Generates synthetic dataset for experiments.
     * 
     * @param size Number of data items to generate
     * @param valueRange Range of possible values (for integers)
     * @return List of labeled synthetic data items
     */
    public static List<DataItem> generateSyntheticData(int size, int valueRange) {
        List<DataItem> dataItems = new ArrayList<>();
        Random random = new Random(42); // Fixed seed for reproducibility
        long baseTimestamp = System.currentTimeMillis();
        
        for (int i = 0; i < size; i++) {
            int value = random.nextInt(valueRange);
            long timestamp = baseTimestamp + i;
            dataItems.add(new DataItem(value, timestamp));
        }
        
        return dataItems;
    }
    
    /**
     * Creates a dataset from an array of values, adding timestamps.
     * 
     * @param values Array of values to convert to DataItems
     * @return List of labeled data items
     */
    public static List<DataItem> createDataset(Object[] values) {
        List<DataItem> dataItems = new ArrayList<>();
        long baseTimestamp = System.currentTimeMillis();
        
        for (int i = 0; i < values.length; i++) {
            long timestamp = baseTimestamp + i;
            dataItems.add(new DataItem(values[i], timestamp));
        }
        
        return dataItems;
    }
    
    /**
     * Parses a string value to appropriate type (tries integer first, then string).
     * 
     * @param valueStr String representation of the value
     * @return Parsed value as Object
     */
    private static Object parseValue(String valueStr) {
        try {
            return Integer.parseInt(valueStr);
        } catch (NumberFormatException e) {
            return valueStr;
        }
    }
    
    /**
     * Prints statistics about a dataset.
     * 
     * @param dataItems The dataset to analyze
     */
    public static void printDatasetStats(List<DataItem> dataItems) {
        System.out.println("Dataset Statistics:");
        System.out.println("- Total items: " + dataItems.size());
        
        if (!dataItems.isEmpty()) {
            System.out.println("- First item: " + dataItems.get(0));
            System.out.println("- Last item: " + dataItems.get(dataItems.size() - 1));
            
            // Count unique values
            Set<Object> uniqueValues = new HashSet<>();
            for (DataItem item : dataItems) {
                uniqueValues.add(item.getValue());
            }
            System.out.println("- Unique values: " + uniqueValues.size());
        }
        System.out.println();
    }
}