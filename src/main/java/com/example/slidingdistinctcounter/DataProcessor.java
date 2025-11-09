package com.example.slidingdistinctcounter;

import java.util.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;

/**
 * Data processor that loads and labels datasets with timestamps for experiments.
 */
public class DataProcessor {
    
    /**
     * Process CAIDA traffic files and attach timestamps
     * 
     * @param inputFolder Path to folder containing o#.txt files (e.g., "C:\Users\hwa281\UFL Dropbox\Haibo Wang\CAIDA")
     * @throws IOException If file reading/writing fails
     */
    public static void processCAIDATrafficFiles(String inputFolder) throws IOException {
        System.out.println("Processing CAIDA traffic files from: " + inputFolder);
        System.out.println("=".repeat(80));
        
        // Process all 60 minute files (o1.txt to o60.txt)
        for (int fileNum = 1; fileNum <= 60; fileNum++) {
            String inputFileName = "o" + fileNum + ".txt";
            String outputFileName = "o" + fileNum + "timestamp.txt";
            
            Path inputPath = Paths.get(inputFolder, inputFileName);
            Path outputPath = Paths.get(inputFolder, outputFileName);
            
            if (!Files.exists(inputPath)) {
                System.err.println("Warning: File not found - " + inputPath);
                continue;
            }
            
            processCAIDASingleFile(inputPath.toString(), outputPath.toString(), fileNum);
        }
        
        System.out.println("\n" + "=".repeat(80));
        System.out.println("CAIDA traffic processing completed!");
    }
    
    /**
     * Process a single CAIDA traffic file and attach timestamps
     * 
     * @param inputFilePath Path to input file (o#.txt)
     * @param outputFilePath Path to output file (o#timestamp.txt)
     * @param minuteNumber Minute number (0-59) for timestamp calculation
     * @throws IOException If file reading/writing fails
     */
    private static void processCAIDASingleFile(String inputFilePath, String outputFilePath, int minuteNumber) 
            throws IOException {
        
        System.out.println("\nProcessing file: " + inputFilePath);
        
        // Read all lines from input file
        List<String> lines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (!line.isEmpty() && !line.startsWith("#")) {
                    lines.add(line);
                }
            }
        }
        
        int totalItems = lines.size();
        System.out.println("  Total items in this minute: " + totalItems);
        
        if (totalItems == 0) {
            System.out.println("  Skipping empty file");
            return;
        }
        
        // Calculate items per second (60 seconds in a minute)
        double itemsPerSecond = totalItems / 60.0;
        System.out.println("  Items per second (avg): " + String.format("%.2f", itemsPerSecond));
        
        // Base timestamp: (minuteNumber - 1) * 60 (converts minute to seconds)
        // File o1.txt is minute 0, o2.txt is minute 1, ..., o60.txt is minute 59
        int baseTimestamp = (minuteNumber - 1) * 60;
        
        // Write output file with timestamps
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath))) {
            for (int i = 0; i < lines.size(); i++) {
                String line = lines.get(i);
                
                // Parse srcIP and dstIP
                String[] parts = line.split("\\s+");
                if (parts.length < 2) {
                    System.err.println("  Warning: Invalid line format: " + line);
                    continue;
                }
                
                String srcIP = parts[0];
                String dstIP = parts[1];
                
                // Calculate timestamp for this item
                // Item i is in second floor(i / itemsPerSecond)
                int secondOffset = (int) Math.floor(i / itemsPerSecond);
                // Ensure secondOffset is within [0, 59]
                secondOffset = Math.min(59, secondOffset);
                
                int timestamp = baseTimestamp + secondOffset;
                
                // Write: srcIP dstIP timestamp
                writer.write(srcIP + " " + dstIP + " " + timestamp);
                writer.newLine();
            }
        }
        
        System.out.println("  Output written to: " + outputFilePath);
        System.out.println("  Timestamp range: [" + baseTimestamp + ", " + (baseTimestamp + 59) + "]");
    }
    
    /**
     * Main method for standalone execution of CAIDA processing
     */
    public static void main(String[] args) {
        String caidaFolder = "C:\\Users\\hwa281\\UFL Dropbox\\Haibo Wang\\CAIDA";
        
        // Allow custom folder path from command line
        if (args.length > 0) {
            caidaFolder = args[0];
        }
        
        try {
            processCAIDATrafficFiles(caidaFolder);
        } catch (IOException e) {
            System.err.println("Error processing CAIDA files: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
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