package com.example.slidingdistinctcounter;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * Main class for SKMV experiments
 * Handles parameter configuration, dataset loading, and measurements
 */
public class Main {
    
    /**
     * Data record from dataset
     */
    public static class DataRecord {
        public long flowLabel;
        public long elementID;
        public long timestamp;
        
        public DataRecord(long flowLabel, long elementID, long timestamp) {
            this.flowLabel = flowLabel;
            this.elementID = elementID;
            this.timestamp = timestamp;
        }
        
        @Override
        public String toString() {
            return String.format("DataRecord{flow=%d, element=%d, time=%d}", 
                flowLabel, elementID, timestamp);
        }
    }
    
    /**
     * Parameter configuration for SKMV
     */
    public static class SKMVConfig {
        public int delta1;      // Bit-width for hash values
        public int delta2;      // Bit-width for timestamps
        public int k;           // k-minimum value count per bucket
        public int m;           // Number of buckets (calculated)
        public long totalMemory; // Total memory budget in bytes
        public long N;          // Window size
        
        public SKMVConfig(int delta1, int delta2, int k, long totalMemory, long N) {
            this.delta1 = delta1;
            this.delta2 = delta2;
            this.k = k;
            this.totalMemory = totalMemory;
            this.N = N;
            this.m = calculateM();
        }
        
        /**
         * Calculate number of buckets m based on memory budget
         * Memory per bucket = k * (delta1 + delta2) bits + overhead
         * Overhead includes: lock (1 bit), lock_time (delta2 bits), lock_maxV (delta1 bits), head (log2(k) bits)
         */
        private int calculateM() {
            // Memory per entry: delta1 bits (hash) + delta2 bits (timestamp)
            int bitsPerEntry = delta1 + delta2;
            
            // Memory per bucket: k entries + bucket overhead
            // Overhead: lock (1 bit) + lock_time (delta2 bits) + lock_maxV (delta1 bits) + head (32 bits for int)
            int bucketOverheadBits = 1 + delta2 + delta1 + 32;
            int bitsPerBucket = k * bitsPerEntry + bucketOverheadBits;
            
            // Convert total memory from bytes to bits
            long totalBits = totalMemory * 8;
            
            // Calculate number of buckets
            int calculatedM = (int) (totalBits / bitsPerBucket);
            
            // Ensure at least 1 bucket
            return Math.max(1, calculatedM);
        }
        
        @Override
        public String toString() {
            return String.format("Config{delta1=%d, delta2=%d, k=%d, m=%d, memory=%d bytes, N=%d}",
                delta1, delta2, k, m, totalMemory, N);
        }
    }
    
    /**
     * Load dataset from file
     * Each line contains: flowLabel elementID timestamp
     * 
     * @param filePath Path to the dataset file
     * @return List of data records
     * @throws IOException If file reading fails
     */
    public static List<DataRecord> loadDataset(String filePath) throws IOException {
        List<DataRecord> records = new ArrayList<>();
        
        System.out.println("Loading dataset from: " + filePath);
        
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(filePath))) {
            String line;
            int lineNumber = 0;
            
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                line = line.trim();
                
                // Skip empty lines and comments
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                
                try {
                    // Parse the three values: flowLabel elementID timestamp
                    String[] parts = line.split("\\s+");
                    if (parts.length != 3) {
                        System.err.println("Warning: Line " + lineNumber + 
                            " has incorrect format (expected 3 values): " + line);
                        continue;
                    }
                    
                    long flowLabel = Long.parseLong(parts[0]);
                    long elementID = Long.parseLong(parts[1]);
                    long timestamp = Long.parseLong(parts[2]);
                    
                    records.add(new DataRecord(flowLabel, elementID, timestamp));
                    
                } catch (NumberFormatException e) {
                    System.err.println("Warning: Line " + lineNumber + 
                        " contains invalid number: " + line);
                }
            }
        }
        
        System.out.println("Loaded " + records.size() + " records from dataset");
        return records;
    }
    
    /**
     * Run experiment with specific configuration
     * 
     * @param config SKMV configuration
     * @param records Dataset records
     * @return Measurement results
     */
    public static void runExperiment(SKMVConfig config, List<DataRecord> records) {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("Running experiment with: " + config);
        System.out.println("=".repeat(80));
        
        try {
            // Create SKMV instance
            SKMV sketch = new SKMV(config.N, config.k, config.m, config.delta1, config.delta2);
            
            System.out.println("SKMV instance created successfully");
            System.out.println("  Hash range: [0, " + sketch.getHashRange() + "]");
            System.out.println("  Timestamp range: [0, " + sketch.getTimestampRange() + "]");
            
            // Process records
            long startTime = System.currentTimeMillis();
            int processedCount = 0;
            long lastCleanTime = 0;
            long cleanInterval = config.N; // Clean every N time units
            
            for (DataRecord record : records) {
                // Periodic cleaning
                if (record.timestamp - lastCleanTime >= cleanInterval) {
                    sketch.periodicClean(record.timestamp);
                    lastCleanTime = record.timestamp;
                }
                
                // Record item
                sketch.recordItem(record.flowLabel, record.elementID, record.timestamp);
                processedCount++;
                
                // Progress update every 100k records
                if (processedCount % 100000 == 0) {
                    System.out.println("Processed " + processedCount + " records...");
                }
            }
            
            long endTime = System.currentTimeMillis();
            double processingTime = (endTime - startTime) / 1000.0;
            
            // Get final cardinality estimate
            double estimatedCardinality = sketch.estimateCardinality();
            
            // Print results
            System.out.println("\n" + "-".repeat(80));
            System.out.println("RESULTS:");
            System.out.println("  Total records processed: " + processedCount);
            System.out.println("  Processing time: " + String.format("%.2f", processingTime) + " seconds");
            System.out.println("  Throughput: " + String.format("%.2f", processedCount / processingTime) + " records/sec");
            System.out.println("  Estimated cardinality: " + String.format("%.2f", estimatedCardinality));
            System.out.println("  Current time: " + sketch.getCurrentTime());
            System.out.println("-".repeat(80));
            
        } catch (Exception e) {
            System.err.println("Error during experiment: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Main entry point
     */
    public static void main(String[] args) {
        System.out.println("SKMV Experiment Runner");
        System.out.println("=".repeat(80));
        
        // Dataset file path
        String datasetPath = "dataset.txt"; // Default path
        if (args.length > 0) {
            datasetPath = args[0];
        }
        
        // Parameter lists for iteration
        int[] delta1_list = {16, 32, 64};           // Hash value bit-widths
        int[] delta2_list = {16, 24, 32};           // Timestamp bit-widths
        int[] k_list = {32, 64, 128};               // k-minimum values
        long[] memory_list = {1024, 4096, 16384};   // Memory budgets in bytes (1KB, 4KB, 16KB)
        
        // Window size N (can be adjusted based on dataset)
        long N = 1000; // Default window size
        
        try {
            // Load dataset
            List<DataRecord> records = loadDataset(datasetPath);
            
            if (records.isEmpty()) {
                System.err.println("No records loaded. Exiting.");
                return;
            }
            
            // Find time range in dataset to set appropriate N
            long minTime = records.stream().mapToLong(r -> r.timestamp).min().orElse(0);
            long maxTime = records.stream().mapToLong(r -> r.timestamp).max().orElse(0);
            long timeRange = maxTime - minTime;
            System.out.println("Dataset time range: [" + minTime + ", " + maxTime + "] (span: " + timeRange + ")");
            
            // Optionally adjust N based on time range
            // N = timeRange / 10; // Example: window is 10% of total time range
            
            System.out.println("Using window size N = " + N);
            
            // Iterate through all parameter combinations
            int experimentCount = 0;
            int totalExperiments = delta1_list.length * delta2_list.length * k_list.length * memory_list.length;
            
            for (int delta1 : delta1_list) {
                for (int delta2 : delta2_list) {
                    for (int k : k_list) {
                        for (long memory : memory_list) {
                            experimentCount++;
                            
                            System.out.println("\n\n" + "=".repeat(80));
                            System.out.println("EXPERIMENT " + experimentCount + " / " + totalExperiments);
                            System.out.println("=".repeat(80));
                            
                            // Create configuration
                            SKMVConfig config = new SKMVConfig(delta1, delta2, k, memory, N);
                            
                            // Validate configuration
                            if (config.m < 1) {
                                System.err.println("Skipping: Insufficient memory for configuration");
                                continue;
                            }
                            
                            // Run experiment
                            runExperiment(config, records);
                        }
                    }
                }
            }
            
            System.out.println("\n\n" + "=".repeat(80));
            System.out.println("ALL EXPERIMENTS COMPLETED!");
            System.out.println("Total experiments run: " + experimentCount);
            System.out.println("=".repeat(80));
            
        } catch (IOException e) {
            System.err.println("Error loading dataset: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("Unexpected error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
