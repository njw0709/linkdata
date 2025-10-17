# Flexible Data I/O Implementation Summary

## Overview
Successfully implemented flexible data I/O capabilities across the linkage package. The classes can now read and write multiple file formats with automatic format detection from file extensions.

## Supported File Formats

### Reading & Writing
- **CSV** (`.csv`) - With optimized chunked reading for large files
- **Stata** (`.dta`) - For survey data
- **Parquet** (`.parquet`, `.pq`) - Efficient columnar format
- **Feather** (`.feather`) - Fast binary format
- **Excel** (`.xlsx`, `.xls`) - Spreadsheet format

## Changes Made

### 1. New Module: `linkdata/io_utils.py`
Created a centralized I/O module with three main functions:

- **`read_data(file_path, **kwargs)`**: Automatically detects file format from extension and uses the appropriate pandas reader
- **`write_data(df, file_path, **kwargs)`**: Automatically detects file format and uses the appropriate pandas writer
- **`get_file_format(file_path)`**: Returns the file format type for a given path

### 2. Updated Classes

#### `ResidentialHistoryHRS` (linkdata/hrs.py)
- Changed from `pd.read_stata()` to `read_data()`
- Now accepts any supported file format

#### `HRSInterviewData` (linkdata/hrs.py)
- Changed from `pd.read_stata()` to `read_data()` for loading
- Changed `save()` method from `to_stata()` to `write_data()`
- Automatically saves to the format specified by the output filename extension

#### `DailyMeasureData` (linkdata/daily_measure.py)
- Kept optimized CSV reading with chunked processing for large files
- Added flexible reading for non-CSV formats (Stata, Parquet, Feather, Excel)
- Updated `_read_header()` method to handle all formats
- Format detection happens early in the initialization

#### `DailyMeasureDataDir` (linkdata/daily_measure.py)
- Updated `_validate_files_have_datacol()` to handle all formats
- Can now work with directories containing mixed file formats

#### Processing Functions (linkdata/process.py)
- `process_multiple_lags_batch()`: Now uses `write_data()` for flexible output
- `process_multiple_lags_parallel()`: Now uses `write_data()` for flexible output
- Output format determined by the `file_format` parameter (e.g., "parquet", "csv", "feather")

### 3. Package Exports (linkdata/__init__.py)
Updated to export the new I/O utilities:
- `read_data`
- `write_data`
- `get_file_format`

## Backward Compatibility

✅ **Fully backward compatible** - All existing code continues to work:
- Existing `.dta` (Stata) files are read/written as before
- All 20 existing tests pass successfully
- Only internal implementation changed; external API remains the same

## Usage Examples

### Reading Different Formats
```python
from linkdata import read_data

# Automatically detects format from extension
df_stata = read_data("survey.dta")
df_csv = read_data("measures.csv")
df_parquet = read_data("results.parquet")
df_feather = read_data("cache.feather")
df_excel = read_data("data.xlsx")
```

### Writing Different Formats
```python
from linkdata import write_data

# Format determined by extension
write_data(df, "output.csv", index=False)
write_data(df, "output.dta")
write_data(df, "output.parquet", compression="gzip")
write_data(df, "output.feather")
write_data(df, "output.xlsx")
```

### Using Classes with Different Formats
```python
from linkdata import ResidentialHistoryHRS, HRSInterviewData

# Works with any format now
rh = ResidentialHistoryHRS("residential_history.parquet")  # or .csv, .dta, etc.
hrs = HRSInterviewData("survey.csv")  # or .dta, .parquet, etc.

# Save to any format
hrs.save("output.parquet")  # or .csv, .dta, etc.
```

### Processing with Different Output Formats
```python
from linkdata import process_multiple_lags_batch

# Output format specified by file_format parameter
temp_files = process_multiple_lags_batch(
    hrs_data=hrs_data,
    contextual_dir=heat_dir,
    n_days=[0, 7, 30],
    id_col="hhidpn",
    temp_dir=temp_dir,
    file_format="parquet"  # or "csv", "feather", etc.
)
```

## Performance Notes

- **CSV files**: Maintain optimized chunked reading with pyarrow engine for large files
- **Parquet/Feather**: Already efficient binary formats, read in single pass
- **Stata files**: Read using pandas native Stata reader
- **Excel files**: Standard pandas Excel reader (may be slower for large files)

## Testing

All existing tests pass:
- ✅ 20 tests passed
- ✅ 1 test skipped (as before)
- ✅ No breaking changes
- ✅ New I/O utilities tested with multiple formats

## Benefits

1. **Flexibility**: Users can now use their preferred file format
2. **Performance**: Can use Parquet for faster I/O with large datasets
3. **Interoperability**: Easy to share data in common formats like CSV or Excel
4. **Future-proof**: Easy to add new formats by updating `io_utils.py`
5. **Backward Compatible**: Existing code continues to work without changes

