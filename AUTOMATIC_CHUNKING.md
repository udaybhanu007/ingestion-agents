# Automatic Chunk Size Calculation

## Overview

The ingestion system now automatically determines optimal chunk sizes based on intelligent content analysis, eliminating the need for manual configuration while ensuring optimal performance and reliability.

## How It Works

### 1. Content Structure Analysis

The system automatically analyzes content to determine:

- **Content type**: Structured (CSV, TSV) vs unstructured (text)
- **Data complexity**: Number of columns, average line length, data density
- **File size**: Total lines, data lines (excluding headers)
- **Token estimation**: Accurate token count prediction per line and total

### 2. Intelligent Chunking Algorithm

```python
# Automatic chunking decision process:
if estimated_total_tokens <= 30000 and data_lines <= 1000:
    return -1  # No chunking needed
else:
    # Calculate optimal chunk size based on:
    available_tokens = max_response_tokens * 0.6  # 60% for content
    optimal_chunk_size = available_tokens / (tokens_per_line * complexity)
    
    # Apply practical constraints based on file size
    if data_lines > 100K: chunk_size = min(optimal, 150)
    elif data_lines > 50K: chunk_size = min(optimal, 200)
    elif data_lines > 10K: chunk_size = min(optimal, 300)
    else: chunk_size = min(optimal, 500)
```

### 3. Complexity-Based Adjustments

The system considers data complexity:
- **Simple data** (basic CSV): Complexity = 1.0x
- **Medium complexity** (10+ columns): Complexity = 1.5-2.0x  
- **High complexity** (20+ columns, long text): Complexity = 2.5-3.0x

Higher complexity reduces chunk size to maintain processing quality.

## Real-World Example: Data_Entry_2017.csv

### Automatic Analysis Results:
```
📁 File: doc/Data_Entry_2017.csv
📊 Full File Analysis:
   • Total lines: 112,121
   • Data lines: 112,120
   • Structured: True
   • Avg line length: 62.4 chars
   • Complexity: 2.16x (medical data with multiple fields)
   • Est. tokens per line: 15.6
   • Est. total tokens: 1,749,072

✅ Automatic Recommendation: 150 lines per chunk
   • Total chunks needed: 747
   • Estimated processing time: 24.9 hours
   • Processing rate: 4,500 lines/minute
```

### Why 150 Lines Per Chunk?

1. **Token Budget**: 30,000 max response tokens × 60% = 18,000 available
2. **Per-line tokens**: 15.6 tokens/line × 2.16 complexity = 33.7 effective tokens/line
3. **Optimal size**: 18,000 ÷ 33.7 = ~534 lines theoretical max
4. **Reliability constraint**: For 100K+ files, cap at 150 lines for stability
5. **Final decision**: 150 lines per chunk (conservative and reliable)

## Chunk Size Examples by File Type

### Small Files (< 1K records)
- **Simple CSV**: No chunking (process all at once)
- **Complex data**: 200-500 lines per chunk
- **Text documents**: No chunking for < 20K tokens

### Medium Files (1K-10K records)  
- **Simple CSV**: 300-500 lines per chunk
- **Complex data**: 200-300 lines per chunk
- **Medical/financial data**: 100-200 lines per chunk

### Large Files (10K-100K records)
- **Simple CSV**: 200-300 lines per chunk  
- **Complex data**: 150-200 lines per chunk
- **High-complexity data**: 100-150 lines per chunk

### Very Large Files (100K+ records)
- **Any complexity**: 100-150 lines per chunk (reliability first)
- **Processing time**: Scaled linearly with chunk count
- **Memory management**: Automatic cleanup enabled

## Benefits of Automatic Chunking

### 1. **Zero Configuration Required**
```python
# Before (manual configuration needed):
metadata = {
    "processing_options": {
        "chunk_size": 200,  # User had to guess
        "chunking_strategy": "large_file"  # User had to choose
    }
}

# After (fully automatic):
metadata = {}  # System handles everything automatically
```

### 2. **Optimal Performance**
- Token usage maximized within safety limits
- Processing time minimized through intelligent sizing
- Memory usage optimized for file size

### 3. **Reliability**
- Conservative chunking for very large files
- Complexity-adjusted sizing prevents token overflow
- Automatic error recovery and fallback strategies

### 4. **Transparency**
Complete logging of chunking decisions:
```
📊 Automatic Chunking Analysis:
   • Content: 112,120 data lines
   • Complexity: 2.16x
   • Estimated tokens: 1,749,072
   • Tokens per line: 15.6
   • Available content tokens: 18,000
   • Optimal chunk size: 150 lines
   • Estimated chunks: 747
   • Estimated time: 1494 minutes
```

## Usage

### Direct Tool Usage
```python
from agents.tools.graph_tool import GraphIngestionTool

tool = GraphIngestionTool()
result = tool.ingest_content(content)  # Automatic chunking applied
```

### API Usage
```python
payload = {
    "doc_uri": "azure://container/large_file.csv",
    "document_type": "csv"
    # No processing_options needed - automatic chunking applied
}
```

### Force Manual Override (if needed)
```python
payload = {
    "doc_uri": "azure://container/file.csv",
    "processing_options": {
        "force_chunk_size": True,  # Override automatic calculation
        "chunk_size": 100          # Manual chunk size
    }
}
```

## Performance Characteristics

### Token Efficiency
- 60% of token budget allocated to content
- 40% reserved for prompts, schema, and response structure
- Optimal balance between throughput and quality

### Processing Speed
- **Simple data**: ~300-500 lines/minute per chunk
- **Complex data**: ~150-300 lines/minute per chunk  
- **Very complex data**: ~75-150 lines/minute per chunk

### Memory Usage
- Automatic cleanup for files > 50K records
- Progressive processing reduces peak memory usage
- Garbage collection optimized for large datasets

## Future Enhancements

### Planned Improvements
1. **Learning-based optimization**: Adjust chunking based on historical performance
2. **Content-aware chunking**: Semantic boundaries for text documents
3. **Parallel processing**: Multiple chunks processed simultaneously
4. **Streaming ingestion**: Real-time processing for very large files

### Advanced Features (Roadmap)
1. **Adaptive chunking**: Adjust chunk size during processing based on performance
2. **Predictive scaling**: Anticipate optimal settings based on content preview
3. **Resource-aware sizing**: Adjust for available memory and processing power
4. **Quality-based optimization**: Balance speed vs extraction accuracy
