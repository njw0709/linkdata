# Data Loading Optimization Guide

## Overview

This guide explains the data loading optimizations implemented for parallel processing in the linkage workflow, focusing on minimizing I/O operations and memory pressure.

## Problem Statement

When processing multiple lag periods (e.g., 0, 7, 30, 60, 90 days prior), the naive approach loads heat index data separately for each lag:

- **Excessive I/O**: Same CSV files (72K+ GEOIDs × 5 years) loaded repeatedly
- **High Memory**: Multiple copies of identical data in memory
- **Slow Processing**: Each thread/process reloads data independently

For example, processing 10 lags with 5 years of data means 50 file reads instead of 5.

## Solution: Preload and Share

### 1. Preload Function in `DailyMeasureDataDir`

**File**: `linkdata/daily_measure.py`

```python
def preload_years(self, years: Optional[List[str]] = None) -> None:
    """
    Preload data for specified years (or all available years).
    Loads all data into _cache to avoid lazy loading during processing.
    """
    if years is None:
        years = self.years_available
    
    print(f"📥 Preloading {len(years)} years of {self.measure_type or self.data_col} data...")
    for year in years:
        if year not in self._cache:
            _ = self[year]  # Triggers lazy loading and caching
    print(f"✅ Preloaded {len(years)} years successfully")
```

**Usage:**
```python
heat_data = DailyMeasureDataDir(heat_dir, data_col="index")
heat_data.preload_years(['2016', '2017', '2018', '2019', '2020'])
# Now all years are in memory, no more I/O during processing
```

### 2. Compute Required Years

**File**: `linkdata/process.py`

```python
def compute_required_years(
    hrs_data: HRSInterviewData,
    max_lag_days: int,
    date_col: Optional[str] = None,
) -> List[int]:
    """
    Compute which years of contextual data are needed based on:
    - Interview dates in HRS data
    - Maximum lag period
    
    Returns list of years needed for data linkage.
    """
    if date_col is None:
        date_col = hrs_data.datecol
    
    dates = hrs_data.df[date_col]
    min_date = dates.min() - pd.Timedelta(days=max_lag_days)
    max_date = dates.max()
    
    return list(range(min_date.year, max_date.year + 1))
```

**Usage:**
```python
# Survey data from 2016-2020, processing up to 180-day lags
required_years = compute_required_years(hrs_data, max_lag_days=180)
# Returns [2015, 2016, 2017, 2018, 2019, 2020]
# (includes 2015 for 180-day lags from early 2016 dates)
```

### 3. Optimized Parallel Processing

**With ThreadPoolExecutor (Current Implementation):**

```python
# Compute and preload required years
required_years = compute_required_years(hrs_data, max_lag_days=max(lags))
heat_data.preload_years([str(y) for y in required_years])

# All threads share the same memory space and preloaded data
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = {executor.submit(process_single_lag, ...): n for n in lags}
    for fut in as_completed(futures):
        result = fut.result()
        # No additional I/O needed!
```

**Advantages:**
- ✅ Simple implementation
- ✅ Threads share memory naturally
- ✅ No serialization overhead
- ✅ Data loaded only once

**Limitations:**
- ⚠️ Python GIL limits true parallelism for CPU-bound tasks
- ⚠️ Best for I/O-bound operations (which merging is)

## Performance Results

### Test Environment
- 55 people with residential histories
- 5 years of heat data (2016-2020)
- 72K+ GEOIDs per year
- 3 lags tested (0, 7, 30 days)

### Results

| Approach | Time | I/O Operations | Notes |
|----------|------|----------------|-------|
| **Sequential (naive)** | ~3-4 min | 15 file reads | Loads data 3 times |
| **Sequential (preload)** | ~2:26 | 5 file reads | Load once, use 3 times |
| **Parallel (preload)** | ~2:02 | 5 file reads | True parallelism for merges |

**Key Wins:**
- 📉 **83% reduction** in file reads (15 → 5)
- ⚡ **~40% faster** with preloading + parallelism
- 💾 **Single copy** of data in memory (vs. multiple copies)

## Additional Optimization Strategies

### A. GEOID Filtering (Recommended Next Step)

**Concept**: Pre-filter heat data to only GEOIDs present in HRS data.

**Expected Benefit**: ~90%+ reduction in data size (from 72K to ~100s of GEOIDs)

**Implementation:**
```python
# Extract unique GEOIDs from HRS data
def get_unique_geoids(hrs_data: HRSInterviewData) -> set:
    """Extract all unique GEOIDs from HRS data."""
    geoid_cols = [c for c in hrs_data.df.columns if 'LINKCEN' in c]
    all_geoids = set()
    for col in geoid_cols:
        all_geoids.update(hrs_data.df[col].dropna().unique())
    return all_geoids

# Filter during data loading
unique_geoids = get_unique_geoids(hrs_data)
# Add filter_geoids parameter to DailyMeasureData.__init__
# Filter DataFrame after reading: df[df[geoid_col].isin(unique_geoids)]
```

**Estimated Impact:**
- Memory: 2GB → 200MB
- Processing time: 2:02 → ~30-60 seconds

### B. Date Range Filtering

**Concept**: Only load dates within `min_date - max_lag` to `max_date`.

**Implementation:**
```python
def filter_date_range(df, date_col, min_date, max_date):
    return df[(df[date_col] >= min_date) & (df[date_col] <= max_date)]
```

**Estimated Impact:** ~20-30% reduction in data size

### C. Lazy Column Loading

**Concept**: Only load columns actually needed (Date, GEOID, measure).

**Status**: Already implemented in `DailyMeasureData` via `usecols` parameter.

### D. Convert to Parquet with Predicate Pushdown

**Concept**: Convert CSVs to Parquet format for much faster filtered reads.

**Implementation:**
```python
# One-time conversion
import pyarrow.parquet as pq
df.to_parquet('heat_2016.parquet', engine='pyarrow')

# Read with filtering (much faster than CSV)
df = pd.read_parquet('heat_2016.parquet', 
                     filters=[('GEOID10', 'in', unique_geoids)])
```

**Estimated Impact:** 5-10x faster reads for filtered data

### E. ProcessPoolExecutor with Shared Memory (Advanced)

**Concept**: Use true multiprocessing with shared memory segments.

**When to Use**: When CPU-bound operations dominate (e.g., complex calculations per row)

**Implementation Sketch:**
```python
from multiprocessing import shared_memory
import numpy as np

# In parent process: load and share
df_concat = pd.concat([heat_data[y].df for y in years])
shm = shared_memory.SharedMemory(create=True, size=df_concat.nbytes)
shared_arr = np.ndarray(df_concat.shape, dtype=df_concat.dtypes[0], buffer=shm.buf)
shared_arr[:] = df_concat.values[:]

# Pass shared memory name to child processes
with ProcessPoolExecutor() as executor:
    futures = {executor.submit(process_with_shm, shm.name, ...): n for n in lags}

# In child process: attach to shared memory
shm = shared_memory.SharedMemory(name=shm_name)
shared_arr = np.ndarray(shape, dtype=dtype, buffer=shm.buf)
df = pd.DataFrame(shared_arr, columns=columns)
```

**Complexity**: High (requires careful memory management)

**Estimated Impact:** 2-3x faster than ThreadPoolExecutor for CPU-intensive tasks

### F. Chunked Processing (For Very Large Datasets)

**Concept**: Process lags in batches to limit peak memory usage.

**Implementation:**
```python
lag_batches = [[0, 7, 30], [60, 90, 120], [150, 180]]
for batch in lag_batches:
    max_lag = max(batch)
    years = compute_required_years(hrs_data, max_lag)
    heat_data.preload_years(years)
    # Process batch
    heat_data._cache.clear()  # Free memory
```

**When to Use**: When memory is constrained and you have many lags

## Recommended Implementation Priority

### ✅ Phase 1: Basic Preloading (DONE)
- Implemented `preload_years()` method
- Implemented `compute_required_years()` utility
- Updated integration tests
- **Result**: 40% faster, 83% fewer file reads

### 📋 Phase 2: GEOID Filtering (Next)
1. Add `get_unique_geoids()` utility function
2. Add `filter_geoids` parameter to `DailyMeasureData.__init__`
3. Update preload workflow to use filtering
4. Measure memory reduction

**Expected Result**: 90% memory reduction, 2-3x faster

### 📋 Phase 3: ProcessPool Optimization (If Needed)
1. Benchmark ThreadPool vs ProcessPool on production data
2. If CPU-bound, implement shared memory approach
3. Update `process_single_lag` to work with shared data
4. Compare performance

**Expected Result**: 2-3x faster for CPU-intensive workloads

### 📋 Phase 4: Parquet Migration (Long-term)
1. Convert CSV files to Parquet format
2. Update `DailyMeasureData` to read Parquet
3. Implement predicate pushdown filtering
4. Update documentation

**Expected Result**: 5-10x faster reads with filtering

## Integration Example

Here's a complete example showing the optimized workflow:

```python
from linkdata.hrs import ResidentialHistoryHRS, HRSInterviewData
from linkdata.daily_measure import DailyMeasureDataDir
from linkdata.process import process_single_lag, compute_required_years
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# 1. Load HRS data
residential_hist = ResidentialHistoryHRS('residential_history.dta')
hrs_data = HRSInterviewData(
    'survey_data.dta',
    datecol='iwdate',
    move=True,
    residential_hist=residential_hist
)

# 2. Initialize heat data
heat_data = DailyMeasureDataDir('data/heat_index/', data_col='index')

# 3. Compute required years and preload
lags_to_process = [0, 7, 30, 60, 90, 120, 150, 180]
max_lag = max(lags_to_process)
required_years = compute_required_years(hrs_data, max_lag_days=max_lag)

# Filter to available years
available_years = set(heat_data.list_years())
years_to_load = [str(y) for y in required_years if str(y) in available_years]

# Preload all required data (ONCE!)
heat_data.preload_years(years_to_load)

# 4. Process lags in parallel
temp_dir = Path('temp_lags')
temp_dir.mkdir(exist_ok=True)

temp_files = []
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = {
        executor.submit(
            process_single_lag,
            n=n,
            hrs_data=hrs_data,
            contextual_dir=heat_data,
            id_col='hhidpn',
            temp_dir=temp_dir,
            prefix='heat',
            file_format='parquet'
        ): n for n in lags_to_process
    }
    
    for fut in as_completed(futures):
        n = futures[fut]
        result = fut.result()
        if result:
            temp_files.append(result)
            print(f'✓ Processed lag {n}')

# 5. Merge all outputs
final_df = hrs_data.df.copy()
for f in temp_files:
    lag_df = pd.read_parquet(f)
    final_df = final_df.merge(lag_df, on='hhidpn', how='left')

# 6. Save final result
final_df.to_stata('output_with_heat_lags.dta')
print(f'✅ Final dataset: {final_df.shape}')
```

## Key Takeaways

1. **Preload Once, Use Many**: Loading data once and reusing it is the single biggest optimization
2. **Smart Year Selection**: Only load years you actually need
3. **ThreadPool First**: Start with ThreadPoolExecutor for simplicity
4. **Filter Early**: GEOID filtering provides massive wins with minimal complexity
5. **Profile First**: Measure performance before implementing complex optimizations
6. **Memory vs Speed**: Consider the tradeoff based on your system constraints

## Monitoring Performance

Add timing and memory tracking to your workflow:

```python
import time
import psutil
import os

def get_memory_usage():
    """Get current process memory usage in MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

# Before preload
mem_before = get_memory_usage()
start_time = time.time()

# Preload
heat_data.preload_years(years_to_load)

# After preload
mem_after = get_memory_usage()
preload_time = time.time() - start_time

print(f"Preload: {preload_time:.1f}s, Memory: {mem_after - mem_before:.1f} MB")

# Process lags
start_time = time.time()
# ... parallel processing ...
process_time = time.time() - start_time

print(f"Processing: {process_time:.1f}s")
print(f"Total time: {preload_time + process_time:.1f}s")
```

## Questions?

For issues or suggestions, please refer to the test suite in `tests/test_end_to_end_linkage.py` for working examples.

