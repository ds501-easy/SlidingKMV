package com.example.slidingdistinctcounter;

/**
 * Sliding KMV (S-KMV) Sketch for Flow Cardinality Estimation
 * 
 * A research implementation of the Sliding KMV sketch for approximate distinct counting
 * in time-based sliding windows. This implementation follows the algorithm described in
 * Sections III and IV of the Sliding KMV research paper.
 * 
 * The sketch uses k-minimum hash values with a P2C lock zone
 * mechanism to handle sliding window constraints efficiently.
 * 
 * @author Research Implementation
 */
public class SlidingKMVSketch {
    
    // Core parameters
    private final long N;      // Window length (time units)
    private final int k;       // k-minimum value count per bucket
    private final int m;       // Number of buckets
    
    // Global state
    private long T;           // Current global time (initialized to 0)
    private Bucket[] C;       // Array of m buckets
    
    /**
     * Memory-efficient timestamp representation for sliding windows
     * Uses modular arithmetic to compress timestamps into smaller space
     */
    public static class AdjustedTimestamp {
        private final long N;    // Window length
        private long vAT;        // Adjusted timestamp value
        
        /**
         * Constructor for AdjustedTimestamp
         * @param N Window length (time units)
         */
        public AdjustedTimestamp(long N) {
            this.N = N;
            this.vAT = 2 * N;  // Initialize to 2*N (indicates empty/unset)
        }
        
        /**
         * Record a timestamp using modular arithmetic compression
         * @param t The actual timestamp to record
         */
        public void record(long t) {
            this.vAT = t % (2 * N);
        }
        
        /**
         * Check if the recorded timestamp is within the current sliding window
         * @param T Current global time
         * @return true if timestamp is in window, false otherwise
         */
        public boolean lookup(long T) {
            if (vAT == 2 * N) {
                return false;  // Unset/empty timestamp
            }
            long diff = (T + 2 * N - vAT) % (2 * N);
            return diff < N;
        }
        
        /**
         * Clean/reset timestamp if it's outside the sliding window
         * @param T Current global time
         */
        public void clean(long T) {
            long diff = (T + 2 * N - vAT) % (2 * N);
            if (diff >= N) {
                vAT = 2 * N;  // Reset to unset state
            }
        }
        
        /**
         * Get the raw adjusted timestamp value (for debugging)
         * @return The vAT value
         */
        public long getRawValue() {
            return vAT;
        }
        
        @Override
        public String toString() {
            return String.format("AT{vAT=%d, N=%d}", vAT, N);
        }
    }
    
    /**
     * Represents a single entry in the k-minimum set with compressed timestamp
     */
    public static class Entry {
        public long h;                    // Hash value of the k-minimum
        public AdjustedTimestamp t;       // Compressed arrival timestamp
        
        public Entry(long N) {
            this.h = Long.MAX_VALUE;      // Initialize to maximum (indicates empty)
            this.t = new AdjustedTimestamp(N);  // Initialize compressed timestamp
        }
        
        public Entry(long h, long N) {
            this.h = h;
            this.t = new AdjustedTimestamp(N);
        }
        
        @Override
        public String toString() {
            return String.format("Entry{h=%d, t=%s}", h, t);
        }
    }
    
    /**
     * Represents a bucket C[i] containing k-minimum entries with P2C lock zone
     */
    public static class Bucket {
        public Entry[] entries;   // Array of 'k' entries (size k)
        public int lock;          // Lock bit: 0 (deactivated) or 1 (activated)
        public long lock_time;    // Time when the lock was set
        public long lock_maxV;    // Upper-bound hash value (use Long.MAX_VALUE initially)
        public int head;          // Index (0 to k-1) of entry with highest hash value in sliding window
        
        public Bucket(int k, long N) {
            this.entries = new Entry[k];
            for (int i = 0; i < k; i++) {
                this.entries[i] = new Entry(N);
            }
            this.lock = 0;
            this.lock_time = 0;
            this.lock_maxV = Long.MAX_VALUE;
            this.head = 0;
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("Bucket{lock=%d, lock_time=%d, lock_maxV=%d, head=%d, entries=[", 
                     lock, lock_time, lock_maxV, head));
            for (int i = 0; i < entries.length; i++) {
                if (i > 0) sb.append(", ");
                sb.append(entries[i]);
            }
            sb.append("]}");
            return sb.toString();
        }
    }
    
    /**
     * Constructor for the Sliding KMV Sketch
     * 
     * @param N Window length (time units)
     * @param k k-minimum value count per bucket
     * @param m Number of buckets
     */
    public SlidingKMVSketch(long N, int k, int m) {
        this.N = N;
        this.k = k;
        this.m = m;
        this.T = 0;
        
        // Initialize bucket array
        this.C = new Bucket[m];
        for (int i = 0; i < m; i++) {
            this.C[i] = new Bucket(k, N);
        }
    }
    
    /**
     * Hash function H(): Maps flow label to bucket index using FNV-1a hash
     * 
     * @param flowLabel The flow identifier
     * @return Bucket index (0 to m-1)
     */
    private int H(long flowLabel) {
        // FNV-1a hash for bucket assignment
        long hash = fnv1aHash64(flowLabel);
        return (int) ((hash & Long.MAX_VALUE) % m);
        }
        
        /**
         * Hash function h(): Produces uniform hash value for elements using MurmurHash3
         * 
         * @param elementID The element identifier
         * @return Uniform hash value in range [0, Long.MAX_VALUE]
         */
    private long h(long elementID) {
        // MurmurHash3 for uniform hash distribution
        long hash = murmurHash3_64(elementID, 0x9747b28c);  // Fixed seed for reproducibility
        return hash & Long.MAX_VALUE;  // Ensure positive value in range [0, Long.MAX_VALUE]
        }
        
        /**
         * FNV-1a 64-bit hash implementation
         * Fast and well-distributed hash function
         * 
         * @return Hash value in range [0, Long.MAX_VALUE]
         * 
         */
    private long fnv1aHash64(long data) {
        final long FNV_OFFSET_BASIS_64 = 0xcbf29ce484222325L;
        final long FNV_PRIME_64 = 0x100000001b3L;
        
        long hash = FNV_OFFSET_BASIS_64;
        
        // Process each byte of the long value
        for (int i = 0; i < 8; i++) {
            byte b = (byte) (data >> (i * 8));
            hash ^= (b & 0xff);
            hash *= FNV_PRIME_64;
        }
        
        return hash;
    }
    
    /**
     * MurmurHash3 64-bit implementation (simplified version)
     * Excellent uniformity and avalanche properties
     */
    private long murmurHash3_64(long key, int seed) {
        long h1 = seed;
        long h2 = seed;
        
        final long c1 = 0x87c37b91114253d5L;
        final long c2 = 0x4cf5ad432745937fL;
        
        // Process the key
        long k1 = key;
        
        k1 *= c1;
        k1 = Long.rotateLeft(k1, 31);
        k1 *= c2;
        h1 ^= k1;
        
        h1 = Long.rotateLeft(h1, 27);
        h1 = h1 * 5 + 0x52dce729;
        
        // Finalization
        h1 ^= 8; // length in bytes
        h2 ^= 8;
        
        h1 += h2;
        h2 += h1;
        
        h1 = fmix64(h1);
        h2 = fmix64(h2);
        
        h1 += h2;
        
        return h1;
    }
    
    /**
     * Finalization mix function for MurmurHash3
     */
    private long fmix64(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
    }
    
    /**
     * Online Item Recording - Core algorithm for processing streaming elements
     * 
     * @param flowLabel The flow identifier
     * @param elementID The element identifier to process
     * @param timestamp The timestamp of the arriving item (becomes current time T)
     */
    public void recordItem(long flowLabel, long elementID, long timestamp) {
        // Step 1: Time Update and Hashing
        T = timestamp;  // Update global time to the item's timestamp
        int bucketIndex = H(flowLabel);
        long h_y = h(elementID);
        
        Bucket bucket = C[bucketIndex];
        
        // Step 2: Check and Reset P2C Lock Zone (Section IV-A, Step 1)
        
        // Check Lock Timeout
        if (bucket.lock == 1 && (T - bucket.lock_time >= N)) {
            bucket.lock = 0;
        }
        
        // Check Head Outdate (Lock Activation)
        if (bucket.lock == 0) {
            Entry headEntry = bucket.entries[bucket.head];
            if (!headEntry.t.lookup(T)) {  // Use AT lookup method
                bucket.lock = 1;
                bucket.lock_time = T;
                bucket.lock_maxV = Long.MAX_VALUE;
            }
        }
        
        // Step 3: Update Item y (Section IV-A, Step 2)
        
        // Case 0: Check for Duplicate
        for (int i = 0; i < k; i++) {
            if (bucket.entries[i].h == h_y) {
                bucket.entries[i].t.record(T);  // Update timestamp using AT method
                return;  // Exit early
            }
        }
        
        if (bucket.lock == 0) {
            // Case 1: No Lock
            updateNoLock(bucket, h_y, T);
        } else {
            // Case 2: Lock Active
            updateWithLock(bucket, h_y, T);
        }
    }
    
    /**
     * Handle Case 1: No Lock (Section IV-A, Step 2, Case 1)
     */
    private void updateNoLock(Bucket bucket, long h_y, long currentTime) {
        Entry headEntry = bucket.entries[bucket.head];
        
        // Subcase 1a: h_y is smaller than head's hash value
        if (h_y < headEntry.h) {
            // Find an empty or outdated entry to insert
            int insertIndex = findInsertPosition(bucket, currentTime);
            if (insertIndex != -1) {
                bucket.entries[insertIndex].h = h_y;
                bucket.entries[insertIndex].t.record(currentTime);  // Use AT record method
                updateHead(bucket, currentTime);
            }
        }
        // Subcase 1b: Check for empty or outdated entries even if h_y >= head
        else {
            int insertIndex = findInsertPosition(bucket, currentTime);
            if (insertIndex != -1) {
                bucket.entries[insertIndex].h = h_y;
                bucket.entries[insertIndex].t.record(currentTime);  // Use AT record method
                updateHead(bucket, currentTime);
            }
        }
    }
    
    /**
     * Handle Case 2: Lock Active (Section IV-A, Step 2, Case 2)
     */
    private void updateWithLock(Bucket bucket, long h_y, long currentTime) {
        Entry headEntry = bucket.entries[bucket.head];
        
        if (h_y < headEntry.h) {
            // Subcase 2a: k-Minimum
            int outdatedIndex = findOutdatedEntry(bucket, currentTime);
            if (outdatedIndex != -1) {
                // Update outdated entry
                bucket.entries[outdatedIndex].h = h_y;
                bucket.entries[outdatedIndex].t.record(currentTime);  // Use AT record method
            } else {
                // All entries up-to-date, overwrite head
                bucket.entries[bucket.head].h = h_y;
                bucket.entries[bucket.head].t.record(currentTime);  // Use AT record method
                updateHead(bucket, currentTime);
                // Reset lock
                bucket.lock = 0;
            }
        } else if (headEntry.h < h_y && h_y < bucket.lock_maxV) {
            // Subcase 2b: Falls in P2C Zone
            bucket.lock_maxV = h_y;
        }
        // Subcase 2c: Falls Beyond (h_y >= lock_maxV) - Do nothing
    }
    
    /**
     * Find position to insert new entry (empty or outdated)
     */
    private int findInsertPosition(Bucket bucket, long currentTime) {
        // First try to find empty entry
        for (int i = 0; i < k; i++) {
            if (bucket.entries[i].h == Long.MAX_VALUE) {
                return i;
            }
        }
        
        // Then try to find outdated entry using AT lookup
        for (int i = 0; i < k; i++) {
            if (!bucket.entries[i].t.lookup(currentTime)) {
                return i;
            }
        }
        
        return -1;  // No available position
    }
    
    /**
     * Find an outdated entry in the bucket
     */
    private int findOutdatedEntry(Bucket bucket, long currentTime) {
        for (int i = 0; i < k; i++) {
            if (!bucket.entries[i].t.lookup(currentTime)) {
                return i;
            }
        }
        return -1;  // No outdated entry found
    }
    
    /**
     * Update head index to point to entry with highest hash value in sliding window
     */
    private void updateHead(Bucket bucket, long currentTime) {
        long maxHash = -1;
        int maxIndex = 0;
        
        for (int i = 0; i < k; i++) {
            Entry entry = bucket.entries[i];
            // Only consider entries in sliding window using AT lookup
            if (entry.h != Long.MAX_VALUE && entry.t.lookup(currentTime)) {
                if (entry.h > maxHash) {
                    maxHash = entry.h;
                    maxIndex = i;
                }
            }
        }
        
        bucket.head = maxIndex;
    }
    
    /**
     * Periodic cleaning method for AT implementation
     * Should be called every N time units or at regular intervals to clean outdated entries
     * 
     * @param currentTime Current global time for cleaning
     */
    public void periodicClean(long currentTime) {
        T = currentTime;  // Update global time
        
        // Iterate through all buckets
        for (int i = 0; i < m; i++) {
            periodicCleanBucket(currentTime, i);
        }
    }
    
    /**
     * Periodic cleaning method for a specific bucket
     * Cleans outdated entries in the specified bucket only
     * 
     * @param currentTime Current global time for cleaning
     * @param bucketIndex Index of the bucket to clean (0 to m-1)
     */
    public void periodicCleanBucket(long currentTime, int bucketIndex) {
        if (bucketIndex < 0 || bucketIndex >= m) {
            throw new IllegalArgumentException("Bucket index out of range: " + bucketIndex);
        }
        
        T = currentTime;  // Update global time
        Bucket bucket = C[bucketIndex];
        
        // Clean all entries using AT clean method
        for (int j = 0; j < k; j++) {
            bucket.entries[j].t.clean(T);
            
            // If timestamp was cleaned (outdated), also clear the hash value
            if (bucket.entries[j].t.getRawValue() == 2 * N) {
                bucket.entries[j].h = Long.MAX_VALUE;
            }
        }
        
        // Update head pointer after cleaning
        updateHead(bucket, T);
        
        // Update bucket lock status
        updateBucketStatus(bucket);
    }
    
    /**
     * Query method for cardinality estimation
     * 
     * @return Estimated cardinality of distinct elements in sliding window
     */
    public double estimateCardinality() {
        double harmonicSum = 0.0;
        
        // Iterate through all buckets
        for (int i = 0; i < m; i++) {
            Bucket bucket = C[i];
            
            // Update bucket status (P2C Management)
            updateBucketStatus(bucket);
            
            // Collect hash values in sliding window
            java.util.List<Long> hashValues = collectValidHashValues(bucket);
            
            if (hashValues.isEmpty()) {
                continue; // Skip empty buckets
            }
            
            // Sort hash values to find k'-th minimum
            hashValues.sort(Long::compareTo);
            
            int kPrime = hashValues.size();
            long alpha_k = hashValues.get(kPrime - 1); // k'-th minimum (largest of k' values)
            
            // Calculate per-bucket cardinality using KMV formula
            // n̂_i = k' / α_k' - 1
            double n_i = (double) kPrime / alpha_k * Long.MAX_VALUE - 1;
            
            // Add to harmonic sum
            if (n_i > 0) {
                harmonicSum += 1.0 / n_i;
            }
        }
        
        // Return harmonic mean: n̂ = m / Σ(1/n̂_i)
        if (harmonicSum > 0) {
            return m / harmonicSum;
        } else {
            return 0.0;
        }
    }
    
    /**
     * Update bucket status for querying
     */
    private void updateBucketStatus(Bucket bucket) {
        // Check Lock Timeout
        if (bucket.lock == 1 && (T - bucket.lock_time >= N)) {
            bucket.lock = 0;
        }
        
        // Check Head Outdate (Lock Activation)
        if (bucket.lock == 0) {
            Entry headEntry = bucket.entries[bucket.head];
            if (!headEntry.t.lookup(T)) {  // Use AT lookup method
                bucket.lock = 1;
                bucket.lock_time = T;
                bucket.lock_maxV = Long.MAX_VALUE;
            }
        }
    }
    
    /**
     * Collect valid hash values for cardinality estimation
     */
    private java.util.List<Long> collectValidHashValues(Bucket bucket) {
        java.util.List<Long> hashValues = new java.util.ArrayList<>();
        
        for (int i = 0; i < k; i++) {
            Entry entry = bucket.entries[i];
            
            // Check if entry is in sliding window using AT lookup
            if (entry.h != Long.MAX_VALUE && entry.t.lookup(T)) {
                // Special case: If lock=1, exclude head entry
                if (bucket.lock == 1 && i == bucket.head) {
                    continue;
                }
                hashValues.add(entry.h);
            }
        }
        
        return hashValues;
    }
    
    // Getter methods for debugging and analysis
    public long getCurrentTime() { return T; }
    public long getWindowSize() { return N; }
    public int getK() { return k; }
    public int getM() { return m; }
    public Bucket getBucket(int index) { return C[index]; }
}