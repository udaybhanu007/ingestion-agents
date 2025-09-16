"""
Common Functions for Ingestion Agent Application

This module contains utility functions and common operations used across
the ingestion agent application.
"""

import logging
import json
import hashlib
import re
import uuid
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timezone
from pathlib import Path


def setup_logging(log_level: str = "INFO", log_file: Optional[str] = None) -> logging.Logger:
    """
    Set up logging configuration for the application.
    
    Args:
        log_level (str): Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file (str, optional): Path to log file
        
    Returns:
        logging.Logger: Configured logger
    """
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Set up root logger
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load configuration from a JSON file.
    
    Args:
        config_path (str): Path to configuration file
        
    Returns:
        Dict[str, Any]: Configuration dictionary
        
    Raises:
        FileNotFoundError: If config file doesn't exist
        json.JSONDecodeError: If config file is invalid JSON
    """
    config_file = Path(config_path)
    
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_file, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    return config


def save_config(config: Dict[str, Any], config_path: str) -> bool:
    """
    Save configuration to a JSON file.
    
    Args:
        config (Dict[str, Any]): Configuration dictionary
        config_path (str): Path to save configuration file
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        config_file = Path(config_path)
        config_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        
        return True
    except Exception as e:
        logging.error(f"Failed to save configuration: {str(e)}")
        return False


def save_json(data: Any, file_path: str) -> bool:
    """
    Save data to a JSON file.
    
    Args:
        data (Any): Data to save (must be JSON serializable)
        file_path (str): Path to save JSON file
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        output_file = Path(file_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
        
        logging.info(f"Data saved to {file_path}")
        return True
    except Exception as e:
        logging.error(f"Failed to save JSON data to {file_path}: {str(e)}")
        return False


def generate_unique_id(prefix: str = "", suffix: str = "") -> str:
    """
    Generate a unique identifier.
    
    Args:
        prefix (str): Prefix for the ID
        suffix (str): Suffix for the ID
        
    Returns:
        str: Unique identifier
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")[:-3]
    unique_id = f"{prefix}{timestamp}{suffix}" if prefix or suffix else timestamp
    return unique_id


def calculate_file_hash(file_path: str, algorithm: str = "sha256") -> Optional[str]:
    """
    Calculate hash of a file.
    
    Args:
        file_path (str): Path to the file
        algorithm (str): Hash algorithm (md5, sha1, sha256)
        
    Returns:
        Optional[str]: File hash or None if error
    """
    try:
        hash_func = hashlib.new(algorithm)
        
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_func.update(chunk)
        
        return hash_func.hexdigest()
    except Exception as e:
        logging.error(f"Failed to calculate hash for {file_path}: {str(e)}")
        return None


def sanitize_filename(filename: str) -> str:
    """
    Sanitize a filename by removing invalid characters.
    
    Args:
        filename (str): Original filename
        
    Returns:
        str: Sanitized filename
    """
    # Remove or replace invalid characters
    sanitized = re.sub(r'[<>:"/\\|?*]', '_', filename)
    
    # Remove leading/trailing spaces and dots
    sanitized = sanitized.strip(' .')
    
    # Ensure filename is not empty
    if not sanitized:
        sanitized = "unnamed_file"
    
    return sanitized


def format_file_size(size_bytes: int) -> str:
    """
    Format file size in human-readable format.
    
    Args:
        size_bytes (int): Size in bytes
        
    Returns:
        str: Formatted size string
    """
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    
    return f"{size_bytes:.1f} {size_names[i]}"


def validate_email(email: str) -> bool:
    """
    Validate email address format.
    
    Args:
        email (str): Email address to validate
        
    Returns:
        bool: True if valid, False otherwise
    """
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))


def validate_url(url: str) -> bool:
    """
    Validate URL format.
    
    Args:
        url (str): URL to validate
        
    Returns:
        bool: True if valid, False otherwise
    """
    pattern = r'^https?://(?:[-\w.])+(?:\:[0-9]+)?(?:/(?:[\w/_.])*(?:\?(?:[\w&=%.])*)?(?:\#(?:[\w.])*)?)?$'
    return bool(re.match(pattern, url))


def chunk_list(lst: List[Any], chunk_size: int) -> List[List[Any]]:
    """
    Split a list into chunks of specified size.
    
    Args:
        lst (List[Any]): List to chunk
        chunk_size (int): Size of each chunk
        
    Returns:
        List[List[Any]]: List of chunks
    """
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


def merge_dictionaries(dict1: Dict[str, Any], dict2: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge two dictionaries recursively.
    
    Args:
        dict1 (Dict[str, Any]): First dictionary
        dict2 (Dict[str, Any]): Second dictionary
        
    Returns:
        Dict[str, Any]: Merged dictionary
    """
    result = dict1.copy()
    
    for key, value in dict2.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = merge_dictionaries(result[key], value)
        else:
            result[key] = value
    
    return result


def get_timestamp() -> str:
    """
    Get current timestamp in ISO format.
    
    Returns:
        str: Current timestamp
    """
    return datetime.now(timezone.utc).isoformat()


def parse_timestamp(timestamp_str: str) -> Optional[datetime]:
    """
    Parse timestamp string to datetime object.
    
    Args:
        timestamp_str (str): Timestamp string
        
    Returns:
        Optional[datetime]: Parsed datetime or None if invalid
    """
    try:
        return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
    except ValueError:
        try:
            return datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            logging.warning(f"Failed to parse timestamp: {timestamp_str}")
            return None


def retry_operation(func, max_retries: int = 3, delay: float = 1.0) -> Any:
    """
    Retry an operation with exponential backoff.
    
    Args:
        func: Function to retry
        max_retries (int): Maximum number of retries
        delay (float): Initial delay between retries
        
    Returns:
        Any: Result of the function
        
    Raises:
        Exception: Last exception if all retries fail
    """
    import time
    
    last_exception = None
    
    for attempt in range(max_retries + 1):
        try:
            return func()
        except Exception as e:
            last_exception = e
            if attempt < max_retries:
                time.sleep(delay * (2 ** attempt))  # Exponential backoff
                logging.warning(f"Attempt {attempt + 1} failed, retrying in {delay * (2 ** attempt)} seconds")
            else:
                logging.error(f"All {max_retries + 1} attempts failed")
    
    if last_exception:
        raise last_exception


# === Document Classification Utility Functions ===

import uuid
import re
from typing import Dict, List, Any, Optional


def generate_uuid() -> str:
    """Generate a new UUID v4 string."""
    return str(uuid.uuid4())


def clean_text(text: str) -> str:
    """Clean and normalize text content."""
    if not text:
        return ""
    
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text)
    # Remove special characters that might interfere with analysis
    text = re.sub(r'[^\w\s\.\,\!\?\:\;\-\(\)\[\]\{\}]', ' ', text)
    return text.strip()


def extract_entities(content: str) -> List[str]:
    """Extract potential entities from content using pattern matching."""
    entity_patterns = [
        r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b',  # Proper nouns
        r'\b[A-Z]{2,}\b',  # Acronyms
        r'\b\w+(?:_\w+)+\b',  # Technical terms with underscores
    ]
    
    entities = set()
    for pattern in entity_patterns:
        entities.update(re.findall(pattern, content))
    
    return list(entities)


def count_pattern_occurrences(content: str, patterns: List[str]) -> int:
    """Count total occurrences of multiple regex patterns in content."""
    total_count = 0
    for pattern in patterns:
        total_count += len(re.findall(pattern, content, re.IGNORECASE))
    return total_count


def analyze_document_structure(content: str) -> Dict[str, Any]:
    """Analyze basic document structure indicators."""
    structure_indicators = {
        'has_headers': bool(re.search(r'#{1,6}\s|^[A-Z][^a-z]*$', content, re.MULTILINE)),
        'has_numbered_sections': bool(re.search(r'\d+\.\d+\.', content)),
        'has_bullet_points': bool(re.search(r'^\s*[-*•]\s', content, re.MULTILINE)),
        'has_tables': bool(re.search(r'\|.*\|', content)),
        'has_links': bool(re.search(r'http[s]?://[^\s]+|\[.*?\]\(.*?\)', content)),
        'word_count': len(content.split()),
        'sentence_count': len(re.findall(r'[.!?]+', content)),
        'paragraph_count': len(re.split(r'\n\s*\n', content.strip()))
    }
    
    return structure_indicators


def extract_metadata_from_uri(doc_uri: str) -> Dict[str, str]:
    """Extract metadata from document URI."""
    metadata = {
        'source': 'unknown',
        'file_id': '',
        'container': '',
        'version': '',
        'file_type': ''
    }
    
    if doc_uri.startswith('azure://'):
        # azure://container/blob_name
        parts = doc_uri.replace('azure://', '').split('/')
        if len(parts) >= 2:
            metadata['source'] = 'azure'
            metadata['container'] = parts[0]
            metadata['file_id'] = '/'.join(parts[1:])
            
    elif doc_uri.startswith('box://'):
        # box://file/file_id?v=etag
        match = re.match(r'box://file/([^?]+)(?:\?v=(.+))?', doc_uri)
        if match:
            metadata['source'] = 'box'
            metadata['file_id'] = match.group(1)
            metadata['version'] = match.group(2) or 'latest'
            
    elif doc_uri.startswith('confluence://'):
        # confluence://page/page_id
        match = re.match(r'confluence://page/(.+)', doc_uri)
        if match:
            metadata['source'] = 'confluence'
            metadata['file_id'] = match.group(1)
    
    # Extract file type from URI
    if '.' in metadata['file_id']:
        metadata['file_type'] = metadata['file_id'].split('.')[-1].lower()
    
    return metadata


def validate_json_plan(plan: Dict[str, Any]) -> Dict[str, Any]:
    """Validate that a plan follows the correct JSON schema."""
    errors = []
    required_fields = ['plan_id', 'steps']
    
    # Check required top-level fields
    for field in required_fields:
        if field not in plan:
            errors.append(f"Missing required field: {field}")
    
    # Validate plan_id is a valid UUID if present
    if 'plan_id' in plan:
        try:
            uuid.UUID(plan['plan_id'])
        except (ValueError, TypeError):
            errors.append("Invalid plan_id: must be a valid UUID")
    
    # Validate steps if present
    if 'steps' in plan:
        if not isinstance(plan['steps'], list):
            errors.append("Steps must be a list")
        elif len(plan['steps']) == 0:
            errors.append("Steps must be a non-empty list")
        else:
            for i, step in enumerate(plan['steps']):
                step_errors = validate_step(step, i)
                if step_errors:
                    errors.extend(step_errors)
    
    return {
        'valid': len(errors) == 0,
        'errors': errors
    }


def validate_step(step: Dict[str, Any], step_index: int) -> List[str]:
    """Validate a single step in the plan."""
    errors = []
    required_step_fields = ['task_id', 'tool', 'args', 'depends_on']
    valid_tools = ['vector_ingestion', 'graph_ingestion']
    
    # Check required step fields
    for field in required_step_fields:
        if field not in step:
            errors.append(f"Step {step_index}: Missing required field: {field}")
    
    # Validate tool if present
    if 'tool' in step and step['tool'] not in valid_tools:
        errors.append(f"Step {step_index}: Invalid tool '{step['tool']}'. Must be one of: {valid_tools}")
    
    # Validate args contains doc_uri if present
    if 'args' in step:
        if not isinstance(step['args'], dict):
            errors.append(f"Step {step_index}: args must be a dictionary")
        elif 'doc_uri' not in step['args']:
            errors.append(f"Step {step_index}: args must contain 'doc_uri'")
    
    # Validate depends_on is a list if present
    if 'depends_on' in step and not isinstance(step['depends_on'], list):
        errors.append(f"Step {step_index}: depends_on must be a list")
    
    return errors


def format_classification_result(classification: str, confidence: float, 
                               document_type: str, doc_uri: str) -> str:
    """Format classification result for display."""
    return f"""
Document Classification Result:
- URI: {doc_uri}
- Classification: {classification}
- Document Type: {document_type}
- Confidence: {confidence:.2f}
"""


def create_ingestion_plan_schema(plan_id: str, steps: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Create an ingestion plan following the required schema."""
    return {
        "plan_id": plan_id,
        "steps": steps
    }


def get_ingestion_plan_schema_example() -> str:
    """Get an example schema for the ingestion plan in JSON format."""
    example_schema = {
        "plan_id": "uuid-string",
        "steps": [
            {
                "task_id": "1",
                "tool": "vector_ingestion",
                "args": {
                    "doc_uri": "document_uri"
                },
                "depends_on": []
            },
            {
                "task_id": "2", 
                "tool": "graph_ingestion",
                "args": {
                    "doc_uri": "document_uri"
                },
                "depends_on": ["1"]
            }
        ]
    }
    return json.dumps(example_schema, indent=2)


def create_ingestion_step(task_id: str, tool: str, doc_uri: str, 
                         depends_on: Optional[List[str]] = None) -> Dict[str, Any]:
    """Create a single ingestion step following the required schema."""
    if depends_on is None:
        depends_on = []
    
    args = {
        "doc_uri": doc_uri
    }
    
    return {
        "task_id": task_id,
        "tool": tool,
        "args": args,
        "depends_on": depends_on
    }
