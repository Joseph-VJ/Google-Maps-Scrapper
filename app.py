from flask import Flask, render_template, request, jsonify, send_file, flash, redirect, url_for
import subprocess
import os
import json
import pandas as pd
from datetime import datetime
import threading
import uuid
import hashlib
import logging
from main import CacheManager  # Import our cache manager
from flask_socketio import SocketIO
from job_service import JobNotFound, JobService, RateLimitExceeded
from collections import OrderedDict

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'
socketio = SocketIO(app, cors_allowed_origins="*")
job_service = JobService(socketio)

# Store running jobs
running_jobs = {}

import hashlib
import json
import mmap
import struct
import time
from collections import defaultdict, OrderedDict
import sqlite3
import pickle

class LRUCache:
    def __init__(self, max_size=100):
        self.cache = OrderedDict()
        self.max_size = max_size
    
    def get(self, key):
        if key in self.cache:
            self.cache.move_to_end(key)
            return self.cache[key]
        return None
    
    def put(self, key, value):
        if key in self.cache:
            self.cache.move_to_end(key)
        elif len(self.cache) >= self.max_size:
            self.cache.popitem(last=False)
        self.cache[key] = value

# Single consolidated cache for duplicate checking
_duplicate_cache = LRUCache(max_size=200)

# Global caches for ultra-fast duplicate checking (deprecated - kept for backward compatibility)
_file_signature_cache = {}
_content_hash_cache = {}
_metadata_cache = {}
_file_index_cache = {}  # New: Pre-computed file indexes for instant lookups

def build_file_index(output_file, force_rebuild=False):
    """Build an inverted index of the file for lightning-fast searches"""
    try:
        file_stat = os.stat(output_file)
        index_key = f"{output_file}:{file_stat.st_size}:{file_stat.st_mtime}"
        
        # Return cached index if available and not forced rebuild
        if not force_rebuild and index_key in _file_index_cache:
            return _file_index_cache[index_key]
        
        index = {
            'business_types': defaultdict(int),
            'areas': defaultdict(int),
            'total_rows': 0,
            'sample_names': [],
            'key_stats': {},
            'timestamp': time.time()
        }
        
        # For very large files, use streaming with sampling
        if file_stat.st_size > 5 * 1024 * 1024:  # > 5MB
            with open(output_file, 'r', encoding='utf-8') as f:
                # Skip header
                header = f.readline()
                columns = [col.strip().lower() for col in header.split(',')]
                
                name_idx = next((i for i, col in enumerate(columns) if 'name' in col), -1)
                address_idx = next((i for i, col in enumerate(columns) if 'address' in col), -1)
                
                # Sample every Nth line for large files
                line_count = 0
                sample_rate = max(1, file_stat.st_size // (1024 * 1024))  # Sample based on file size
                
                for line_num, line in enumerate(f):
                    line_count += 1
                    
                    # Sample lines for indexing
                    if line_num % sample_rate == 0:
                        parts = line.strip().split(',')
                        
                        # Extract business type indicators from name
                        if name_idx >= 0 and name_idx < len(parts):
                            name = parts[name_idx].lower().strip('"')
                            if name:
                                # Extract business keywords
                                business_words = ['restaurant', 'shop', 'store', 'hospital', 'clinic', 
                                                'bank', 'hotel', 'pharmacy', 'school', 'office', 'center',
                                                'medical', 'dental', 'auto', 'service', 'repair', 'salon']
                                for word in business_words:
                                    if word in name:
                                        index['business_types'][word] += 1
                                
                                index['sample_names'].append(name[:50])  # Store truncated names
                        
                        # Extract area information
                        if address_idx >= 0 and address_idx < len(parts):
                            address = parts[address_idx].lower().strip('"')
                            if address:
                                # Extract common area indicators
                                area_words = address.split()[-3:]  # Last 3 words usually contain area info
                                for word in area_words:
                                    if len(word) > 3:  # Skip short words
                                        index['areas'][word] += 1
                
                index['total_rows'] = line_count
        
        else:
            # For smaller files, build complete index
            try:
                df = pd.read_csv(output_file, dtype=str, na_filter=False)
                index['total_rows'] = len(df)
                
                if 'name' in df.columns:
                    names = df['name'].str.lower().fillna('')
                    for name in names.head(100):  # Index first 100 names
                        business_words = ['restaurant', 'shop', 'store', 'hospital', 'clinic', 
                                        'bank', 'hotel', 'pharmacy', 'school', 'office', 'center']
                        for word in business_words:
                            if word in name:
                                index['business_types'][word] += 1
                        index['sample_names'].append(name[:50])
                
                if 'address' in df.columns:
                    addresses = df['address'].str.lower().fillna('')
                    for address in addresses.head(100):
                        words = address.split()[-2:]
                        for word in words:
                            if len(word) > 3:
                                index['areas'][word] += 1
                
            except Exception:
                pass  # Fallback to basic stats
        
        # Store key statistics
        index['key_stats'] = {
            'top_business_types': dict(sorted(index['business_types'].items(), 
                                            key=lambda x: x[1], reverse=True)[:10]),
            'top_areas': dict(sorted(index['areas'].items(), 
                                   key=lambda x: x[1], reverse=True)[:10]),
            'sample_count': len(index['sample_names'])
        }
        
        # Cache the index
        _file_index_cache[index_key] = index
        
        return index
        
    except Exception:
        return None

def check_duplicate_file(business_type, areas, output_file):
    """Simplified duplicate check with consolidated cache"""
    if not os.path.exists(output_file):
        return False, None
    
    try:
        file_size = os.path.getsize(output_file)
        if file_size == 0:
            return False, None
        
        # Check consolidated cache first
        cache_key = f"{output_file}:{file_size}:{business_type.lower()}"
        cached_result = _duplicate_cache.get(cache_key)
        if cached_result is not None:
            return cached_result['is_duplicate'], cached_result['count']
        
        # Use simplified fingerprinting approach
        is_duplicate, count = _check_content_fingerprint(output_file, business_type, areas, file_size)
        
        # Cache the result
        if is_duplicate is not None:
            _duplicate_cache.put(cache_key, {
                'is_duplicate': is_duplicate,
                'count': count
            })
            return is_duplicate, count
        
        # Fallback to streaming search for medium files
        if file_size > 1024 * 1024:  # > 1MB
            return _check_duplicate_streaming(output_file, business_type, areas, file_size)
        
        # Fallback to pandas for small files
        else:
            return _check_duplicate_cached_pandas(output_file, business_type, areas)
    
    except Exception as e:
        logging.warning(f"Error checking duplicate file: {e}")
        return False, None

def _check_index_based_duplicate(output_file, business_type, areas):
    """Layer 0: Instant index-based duplicate detection (nanoseconds)"""
    try:
        # Get or build file index
        index = build_file_index(output_file)
        if not index:
            return None, None
        
        business_lower = business_type.lower()
        
        # Ultra-fast business type matching using pre-computed index
        business_score = 0
        
        # Check if business type keywords exist in index
        business_keywords = business_lower.split()
        for keyword in business_keywords:
            if keyword in index['business_types']:
                business_score += index['business_types'][keyword]
        
        # Check top business types for partial matches
        for indexed_type, count in index['key_stats']['top_business_types'].items():
            if indexed_type in business_lower or business_lower in indexed_type:
                business_score += count
        
        # Area-based scoring
        area_score = 0
        if areas:
            for area in areas:
                area_lower = area.lower()
                area_words = area_lower.split()
                for word in area_words:
                    if word in index['areas']:
                        area_score += index['areas'][word]
        
        # Quick sample name check
        name_matches = 0
        for sample_name in index['sample_names'][:20]:  # Check first 20 samples
            for keyword in business_keywords:
                if keyword in sample_name:
                    name_matches += 1
        
        # Intelligent scoring algorithm
        total_rows = index['total_rows']
        if total_rows == 0:
            return False, 0
        
        # Calculate match confidence
        business_confidence = min(business_score / max(total_rows * 0.1, 1), 1.0)
        name_confidence = min(name_matches / 20, 1.0)
        area_confidence = min(area_score / max(total_rows * 0.05, 1), 1.0) if areas else 0
        
        # Weighted confidence score
        if areas:
            overall_confidence = (business_confidence * 0.5 + name_confidence * 0.3 + area_confidence * 0.2)
        else:
            overall_confidence = (business_confidence * 0.7 + name_confidence * 0.3)
        
        # Dynamic thresholds based on file size
        if total_rows < 100:
            threshold = 0.6  # High threshold for small files
        elif total_rows < 1000:
            threshold = 0.4  # Medium threshold
        else:
            threshold = 0.25  # Lower threshold for large files
        
        is_duplicate = overall_confidence > threshold and (business_score > 5 or name_matches > 2)
        
        return is_duplicate, total_rows
        
    except Exception:
        return None, None

def _check_file_signature(output_file, business_type, areas):
    """Layer 1: Ultra-fast file signature check using file stats and metadata"""
    try:
        # Create signature from file stats + search parameters
        file_stat = os.stat(output_file)
        signature_data = {
            'size': file_stat.st_size,
            'mtime': int(file_stat.st_mtime),
            'business_type': business_type.lower(),
            'areas_hash': hashlib.md5('|'.join(sorted(areas)).encode()).hexdigest() if areas else ''
        }
        
        signature_key = f"{output_file}:{signature_data['size']}:{signature_data['mtime']}"
        
        # Check cache first (microsecond lookup)
        if signature_key in _file_signature_cache:
            cached_result = _file_signature_cache[signature_key]
            if (cached_result['business_type'] == signature_data['business_type'] and 
                cached_result['areas_hash'] == signature_data['areas_hash']):
                return cached_result['is_duplicate'], cached_result['count']
        
        # If file was modified recently, signature method can't determine - return None for next layer
        if time.time() - file_stat.st_mtime < 300:  # Less than 5 minutes old
            return None, None
        
        # Store in cache for next time
        return None, None  # Let next layer handle first-time analysis
        
    except:
        return None, None

def _check_content_fingerprint(output_file, business_type, areas, file_size):
    """Layer 2: Hash-based content fingerprinting for instant duplicate detection"""
    try:
        # Create content fingerprint using strategic sampling
        fingerprint_key = f"{output_file}:{file_size}:{business_type.lower()}"
        
        if fingerprint_key in _content_hash_cache:
            cached = _content_hash_cache[fingerprint_key]
            return cached['is_duplicate'], cached['count']
        
        # Sample specific positions in file for fingerprinting with adaptive sampling
        with open(output_file, 'rb') as f:
            # Adaptive sample size based on file size
            sample_size = min(8192, max(1024, file_size // 1000))
            header_sample = f.read(sample_size)
            
            # Sample middle section
            if file_size > sample_size * 2:
                f.seek(file_size // 2)
                middle_sample = f.read(sample_size // 2)
            else:
                middle_sample = b''
            
            # Sample end section
            if file_size > sample_size * 3:
                f.seek(max(file_size - (sample_size // 2), 0))
                end_sample = f.read(sample_size // 2)
            else:
                end_sample = b''
        
        # Create composite hash
        content_hash = hashlib.blake2b(header_sample + middle_sample + end_sample).hexdigest()
        
        # Count estimation from header analysis
        header_text = header_sample.decode('utf-8', errors='ignore')
        newline_count = header_text.count('\n')
        estimated_count = max(1, (file_size * newline_count) // len(header_sample)) if newline_count > 0 else 1
        
        # Simple pattern matching in samples
        business_lower = business_type.lower()
        total_sample = (header_sample + middle_sample + end_sample).decode('utf-8', errors='ignore').lower()
        
        # Quick heuristic: if business type appears frequently in samples, likely duplicate
        business_occurrences = total_sample.count(business_lower)
        is_duplicate = business_occurrences > 3 and estimated_count > 50
        
        # Cache the result
        _content_hash_cache[fingerprint_key] = {
            'is_duplicate': is_duplicate,
            'count': estimated_count,
            'hash': content_hash
        }
        
        return is_duplicate, estimated_count
        
    except:
        return None, None

def _check_duplicate_memory_mapped(output_file, business_type, areas, file_size):
    """Layer 3: Memory-mapped binary search for very large files (>10MB)"""
    try:
        business_bytes = business_type.lower().encode('utf-8')
        match_count = 0
        total_lines = 0
        
        with open(output_file, 'rb') as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                # Count total lines using memory mapping
                total_lines = mm[:].count(b'\n')
                
                # Search for business type occurrences using Boyer-Moore-like algorithm
                search_pos = 0
                while search_pos < len(mm):
                    pos = mm.find(business_bytes, search_pos)
                    if pos == -1:
                        break
                    match_count += 1
                    search_pos = pos + len(business_bytes)
                    
                    # Stop early if we find enough matches
                    if match_count > 20:
                        break
        
        # Heuristic: if >10% of lines contain business type, likely duplicate
        is_duplicate = match_count > max(10, total_lines * 0.1) if total_lines > 0 else False
        
        return is_duplicate, max(total_lines - 1, 0)  # Subtract header
        
    except:
        return False, 0

def _check_duplicate_streaming(output_file, business_type, areas, file_size):
    """Layer 4: Optimized streaming search for medium files (1-10MB)"""
    try:
        business_lower = business_type.lower()
        match_count = 0
        line_count = 0
        buffer_size = 64 * 1024  # 64KB buffer
        
        with open(output_file, 'r', encoding='utf-8', buffering=buffer_size) as f:
            # Skip header
            header = f.readline()
            line_count = 1
            
            # Stream through file with optimized pattern matching
            buffer = ""
            while True:
                chunk = f.read(buffer_size)
                if not chunk:
                    break
                
                buffer += chunk
                
                # Process complete lines
                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    line_count += 1
                    
                    # Fast case-insensitive search
                    if business_lower in line.lower():
                        match_count += 1
                        
                        # Early termination for obvious duplicates
                        if match_count > 15:
                            return True, line_count
        
        # Heuristic for duplicate detection
        is_duplicate = match_count > max(8, line_count * 0.08) if line_count > 0 else False
        
        return is_duplicate, max(line_count - 1, 0)
        
    except:
        return False, 0

def _check_duplicate_cached_pandas(output_file, business_type, areas):
    """Layer 5: Optimized pandas with intelligent caching for small files"""
    try:
        # Create cache key
        file_stat = os.stat(output_file)
        cache_key = f"{output_file}:{file_stat.st_size}:{file_stat.st_mtime}:{business_type.lower()}"
        
        if cache_key in _metadata_cache:
            cached = _metadata_cache[cache_key]
            return cached['is_duplicate'], cached['count']
        
        # Load with optimized pandas settings
        df = pd.read_csv(output_file, 
                        dtype=str,  # All strings to avoid type inference overhead
                        na_filter=False,  # Don't parse NaN values
                        low_memory=False)
        
        if df.empty:
            return False, None
        
        result = _check_duplicate_in_dataframe_optimized(df, business_type, areas)
        
        # Cache the result
        _metadata_cache[cache_key] = {
            'is_duplicate': result[0],
            'count': result[1]
        }
        
        return result
        
    except:
        return False, 0

def _check_duplicate_in_dataframe_optimized(df, business_type, areas):
    """Optimized dataframe duplicate checking with vectorized operations"""
    business_type_lower = business_type.lower()
    total_rows = len(df)
    
    # Vectorized string operations for maximum speed
    match_count = 0
    
    # Check all text columns simultaneously using vectorized operations
    text_columns = ['name', 'title', 'category', 'type', 'business_type', 'address']
    existing_columns = [col for col in text_columns if col in df.columns]
    
    if not existing_columns:
        return False, total_rows
    
    # Create a combined search space using vectorized concatenation
    combined_text = df[existing_columns].fillna('').apply(lambda x: ' '.join(x).lower(), axis=1)
    
    # Single vectorized search across all text
    matches = combined_text.str.contains(business_type_lower, case=False, na=False, regex=False)
    match_count = matches.sum()
    
    # Enhanced heuristics for duplicate detection
    match_percentage = match_count / total_rows if total_rows > 0 else 0
    
    # Different thresholds based on file size
    if total_rows < 50:
        threshold = 0.3  # 30% for small files
    elif total_rows < 200:
        threshold = 0.15  # 15% for medium files
    else:
        threshold = 0.08  # 8% for large files
    
    is_duplicate = match_percentage > threshold and match_count > 5
    
    return is_duplicate, total_rows

def clear_duplicate_check_caches():
    """Clear all duplicate check caches for memory management"""
    global _file_signature_cache, _content_hash_cache, _metadata_cache, _file_index_cache
    _file_signature_cache.clear()
    _content_hash_cache.clear()
    _metadata_cache.clear()
    _file_index_cache.clear()

def optimize_duplicate_check_caches():
    """Optimize caches by removing old entries"""
    global _file_signature_cache, _content_hash_cache, _metadata_cache
    current_time = time.time()
    
    # Remove entries older than 1 hour
    cache_timeout = 3600
    
    # Clean file signature cache
    expired_keys = [k for k, v in _file_signature_cache.items() 
                   if current_time - v.get('timestamp', 0) > cache_timeout]
    for key in expired_keys:
        del _file_signature_cache[key]
    
    # Clean content hash cache
    expired_keys = [k for k, v in _content_hash_cache.items() 
                   if current_time - v.get('timestamp', 0) > cache_timeout]
    for key in expired_keys:
        del _content_hash_cache[key]
    
    # Clean metadata cache
    expired_keys = [k for k, v in _metadata_cache.items() 
                   if current_time - v.get('timestamp', 0) > cache_timeout]
    for key in expired_keys:
        del _metadata_cache[key]


class ChennaiScrapingJob:
    def __init__(self, job_id, business_type, areas, results_per_area, output_file, append_mode=False, fast_append=False):
        self.job_id = job_id
        self.business_type = business_type
        self.areas = areas
        self.results_per_area = results_per_area
        self.output_file = output_file
        self.append_mode = append_mode
        self.fast_append = fast_append
        self.status = "running"
        self.progress = 0
        self.result_count = 0
        self.current_area = ""
        self.completed_areas = 0
        self.total_areas = len(areas)
        self.error_message = None
        self.start_time = datetime.now()
        self.end_time = None
        
        # Calculate total expected results
        self.total_results = len(areas) * results_per_area

def run_chennai_scraper(job):
    """Run the Chennai area scraper in a separate thread"""
    try:
        def emit_progress(status_override=None, error_message=None):
            status = status_override or job.status
            payload = {
                'job_id': job.job_id,
                'status': status,
                'progress': job.progress,
                'result_count': job.result_count,
                'timestamp': time.time(),
                'current_area': getattr(job, 'current_area', None),
                'completed_areas': getattr(job, 'completed_areas', None),
                'total_areas': getattr(job, 'total_areas', None),
                'error_message': error_message or job.error_message,
                'end_time': job.end_time.strftime('%Y-%m-%d %H:%M:%S') if job.end_time else None,
            }
            socketio.emit('job_progress', payload)
            elapsed_seconds = max((datetime.now() - job.start_time).total_seconds(), 0.0)
            throughput_per_minute = 0.0
            if elapsed_seconds > 0 and job.result_count > 0:
                throughput_per_minute = (job.result_count / elapsed_seconds) * 60
            remaining = max(job.total_results - job.result_count, 0)
            eta_seconds = None
            if throughput_per_minute > 0:
                per_second = throughput_per_minute / 60
                if per_second > 0:
                    eta_seconds = remaining / per_second
            metrics_payload = {
                'job_id': job.job_id,
                'throughput_per_minute': throughput_per_minute,
                'eta_seconds': eta_seconds,
                'elapsed_seconds': elapsed_seconds,
                'result_count': job.result_count,
                'total_results': job.total_results,
            }
            socketio.emit('job_metrics', metrics_payload)

        emit_progress()
        # Start with append mode false for first area, then true for subsequent areas
        first_area = True
        
        for area in job.areas:
            job.current_area = area
            
            # Build search query
            search_query = f"{job.business_type} in {area}, Chennai, Tamil Nadu, India"
            
            # Build command
            cmd = [
                "C:/Python313/python.exe", 
                "main.py", 
                "-s", search_query, 
                "-t", str(job.results_per_area),
                "-o", job.output_file
            ]
            
            # Use append mode for all areas except the first (unless user specifically chose append)
            if not first_area or job.append_mode:
                cmd.append("--append")
                
            # Add ultra-fast flag for maximum append speed  
            if hasattr(job, 'fast_append') and job.fast_append and (not first_area or job.append_mode):
                cmd.append("--ultra-fast")
            
            # Run the scraper for this area
            process = subprocess.Popen(
                cmd, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE,
                text=True,
                cwd=os.path.dirname(os.path.abspath(__file__))
            )
            
            # Monitor progress for this area
            area_results = 0
            while True:
                output = process.stdout.readline() if process.stdout else ''
                if output == '' and process.poll() is not None:
                    break
                if output:
                    # Parse progress from output
                    if "Currently Found:" in output:
                        try:
                            current = int(output.split("Currently Found:")[1].strip())
                            # Calculate overall progress
                            total_progress = (job.completed_areas * job.results_per_area + current) / job.total_results
                            job.progress = min(total_progress * 100, 100)
                            # Emit progress update via SocketIO
                            emit_progress()
                        except:
                            pass
                    elif "Total Found:" in output:
                        try:
                            area_results = int(output.split("Total Found:")[1].strip())
                            job.result_count += area_results
                            # Emit progress update via SocketIO
                            emit_progress()
                        except:
                            pass
            
            # Check if area scraping completed successfully
            if process.returncode == 0:
                job.completed_areas += 1
                job.progress = (job.completed_areas / job.total_areas) * 100
                # Emit progress update via SocketIO
                emit_progress()
            else:
                stderr = process.stderr.read() if process.stderr else ""
                job.error_message = f"Error in area {area}: {stderr if stderr else 'Unknown error'}"
                job.status = "failed"
                job.end_time = datetime.now()
                # Emit error update via SocketIO
                emit_progress(status_override="failed", error_message=job.error_message)
                break
            
            first_area = False
        
        # Mark as completed if all areas processed successfully
        if job.status != "failed":
            job.status = "completed"
            job.progress = 100
            job.end_time = datetime.now()
            # Emit final status update via SocketIO
            emit_progress(status_override="completed")
            
    except Exception as e:
        job.status = "failed"
        job.error_message = str(e)
        job.end_time = datetime.now()
        # Emit error update via SocketIO
        emit_progress(status_override="failed", error_message=job.error_message)
    
    job.end_time = datetime.now()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/chennai')
def chennai():
    return render_template('chennai.html')

@app.route('/check_cache', methods=['POST'])
def check_cache():
    """Check if cache exists for given search query and output file"""
    try:
        search_query = request.json.get('search_query', '').strip()
        output_file = request.json.get('output_file', 'result.csv').strip()
        
        if not search_query or not output_file:
            return jsonify({'has_cache': False})
        
        cache_manager = CacheManager()
        cache = cache_manager.load_cache(search_query, output_file)
        
        if cache:
            existing_count = cache_manager.get_existing_data_count(output_file)
            return jsonify({
                'has_cache': True,
                'cache_info': {
                    'scraped_count': cache.scraped_count,
                    'total_target': cache.total_target,
                    'last_scraped_index': cache.last_scraped_index,
                    'timestamp': cache.timestamp,
                    'existing_file_count': existing_count,
                    'can_resume': existing_count >= cache.scraped_count
                }
            })
        else:
            return jsonify({'has_cache': False})
            
    except Exception as e:
        return jsonify({'has_cache': False, 'error': str(e)})

@app.route('/start_scraping', methods=['POST'])
def start_scraping():
    # Optimize caches for performance
    optimize_duplicate_check_caches()
    
    try:
        search_query = request.form.get('search_query', '').strip()
        total_results = int(request.form.get('total_results', 1))
        output_file = request.form.get('output_file', 'result.csv').strip()
        append_mode = 'append_mode' in request.form
        fast_append = 'fast_append' in request.form
        
        if not search_query:
            flash('Search query is required!', 'error')
            return redirect(url_for('index'))
        
        if total_results < 1:
            flash('Total results must be at least 1!', 'error')
            return redirect(url_for('index'))
        
        # Check for existing cache first
        cache_manager = CacheManager()
        cache = cache_manager.load_cache(search_query, output_file)
        
        if cache:
            if fast_append:
                # Ultra-fast mode: use file size estimation instead of counting rows
                existing_count = cache_manager.get_existing_data_count_lightning(output_file)
            else:
                existing_count = cache_manager.get_existing_data_count(output_file)
                
            if existing_count >= cache.scraped_count:
                flash(f'🔄 Resuming scraping from previous session! Progress: {cache.scraped_count}/{cache.total_target} records found.', 'info')
                append_mode = True  # Force append mode when resuming
            else:
                # Cache is inconsistent, clear it
                cache_manager.clear_cache(search_query, output_file)
                cache = None
        
        # Check for duplicate file unless in append mode, fast append mode, or resuming
        if not append_mode and not fast_append and not cache:
            is_duplicate, existing_count = check_duplicate_file(search_query, [], output_file)
            if is_duplicate:
                flash(f'File "{output_file}" already contains data for "{search_query}" with {existing_count} records. Use append mode to add more data or choose a different filename.', 'warning')
                return redirect(url_for('index'))
        elif fast_append:
            flash('⚡ Ultra-Fast Append Mode activated! Skipping all duplicate checks for maximum speed.', 'success')
            append_mode = True  # Enable append mode when fast_append is selected
        
        # Generate unique job ID
        job_id = str(uuid.uuid4())

        try:
            job_service.start_job(
                job_id=job_id,
                search_query=search_query,
                total_results=total_results,
                output_file=output_file,
                append_mode=append_mode,
                fast_append=fast_append,
            )
        except RateLimitExceeded as exc:
            flash(str(exc), 'error')
            return redirect(url_for('index'))
        except Exception as exc:
            logging.exception("Error starting scraping job %s", job_id)
            flash(f'Error starting scraping job: {exc}', 'error')
            return redirect(url_for('index'))
        
        return redirect(url_for('monitor_job', job_id=job_id))
        
    except ValueError:
        flash('Invalid number for total results!', 'error')
        return redirect(url_for('index'))
    except Exception as e:
        flash(f'Error starting scraping job: {str(e)}', 'error')
        return redirect(url_for('index'))

@app.route('/start_chennai_scraping', methods=['POST'])
def start_chennai_scraping():
    # Optimize caches for performance
    optimize_duplicate_check_caches()
    
    try:
        business_type = request.form.get('business_type', '').strip()
        custom_keyword = request.form.get('custom_keyword', '').strip()
        results_per_area = int(request.form.get('results_per_area', 10))
        output_file = request.form.get('output_file', 'chennai-results.csv').strip()
        append_mode = 'append_mode' in request.form
        fast_append = 'fast_append' in request.form
        areas = request.form.getlist('areas')
        
        # Determine the actual search term
        if business_type == 'custom':
            if not custom_keyword:
                flash('Custom keyword is required when selecting custom search!', 'error')
                return redirect(url_for('chennai'))
            search_term = custom_keyword
        else:
            if not business_type:
                flash('Business type is required!', 'error')
                return redirect(url_for('chennai'))
            search_term = business_type
        
        if not areas:
            flash('Please select at least one area!', 'error')
            return redirect(url_for('chennai'))
        
        if results_per_area < 1:
            flash('Results per area must be at least 1!', 'error')
            return redirect(url_for('chennai'))
        
        # Check for duplicate file unless in append mode or fast append mode
        if not append_mode and not fast_append:
            is_duplicate, existing_count = check_duplicate_file(search_term, areas, output_file)
            if is_duplicate:
                flash(f'File "{output_file}" already contains data for "{search_term}" with {existing_count} records. Use append mode to add more data or choose a different filename.', 'warning')
                return redirect(url_for('chennai'))
        elif fast_append:
            flash('⚡ Ultra-Fast Append Mode activated! Skipping all duplicate checks for maximum speed.', 'success')
            append_mode = True  # Enable append mode when fast_append is selected
        
        # Generate unique job ID
        job_id = str(uuid.uuid4())
        
        # Create Chennai job with fast_append support
        job = ChennaiScrapingJob(job_id, search_term, areas, results_per_area, output_file, append_mode, fast_append)
        running_jobs[job_id] = job
        
        # Start scraping in background thread
        thread = threading.Thread(target=run_chennai_scraper, args=(job,))
        thread.daemon = True
        thread.start()
        
        return redirect(url_for('monitor_chennai_job', job_id=job_id))
        
    except ValueError:
        flash('Invalid number for results per area!', 'error')
        return redirect(url_for('chennai'))
    except Exception as e:
        flash(f'Error starting Chennai scraping job: {str(e)}', 'error')
        return redirect(url_for('chennai'))

@app.route('/monitor/<job_id>')
def monitor_job(job_id):
    try:
        job = job_service.get_job(job_id)
    except JobNotFound:
        flash('Job not found!', 'error')
        return redirect(url_for('index'))
    
    return render_template('monitor.html', job=job)

@app.route('/monitor_chennai/<job_id>')
def monitor_chennai_job(job_id):
    job = running_jobs.get(job_id)
    if not job:
        flash('Job not found!', 'error')
        return redirect(url_for('chennai'))
    
    return render_template('monitor_chennai.html', job=job)

@app.route('/job_status/<job_id>')
def job_status(job_id):
    try:
        job = job_service.get_job(job_id)
        response_data = {
            'status': job.status,
            'progress': job.progress,
            'result_count': job.result_count,
            'error_message': job.error_message,
            'start_time': job.start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'end_time': job.end_time.strftime('%Y-%m-%d %H:%M:%S') if job.end_time else None,
            'throughput_per_minute': job.throughput_per_minute,
            'eta_seconds': job.eta_seconds,
            'elapsed_seconds': job.elapsed_seconds,
        }
        return jsonify(response_data)
    except JobNotFound:
        job = running_jobs.get(job_id)
        if not job:
            return jsonify({'error': 'Job not found'}), 404
        response_data = {
            'status': job.status,
            'progress': job.progress,
            'result_count': job.result_count,
            'error_message': job.error_message,
            'start_time': job.start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'end_time': job.end_time.strftime('%Y-%m-%d %H:%M:%S') if job.end_time else None
        }
        if isinstance(job, ChennaiScrapingJob):
            response_data.update({
                'current_area': job.current_area,
                'completed_areas': job.completed_areas,
                'total_areas': job.total_areas,
                'business_type': job.business_type
            })
        return jsonify(response_data)

@app.route('/download/<job_id>')
def download_results(job_id):
    try:
        job = job_service.get_job(job_id)
    except JobNotFound:
        job = running_jobs.get(job_id)
    if not job or job.status != 'completed':
        flash('File not ready for download!', 'error')
        return redirect(url_for('index'))
    
    file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), job.output_file)
    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True)
    else:
        flash('Output file not found!', 'error')
        return redirect(url_for('index'))

def get_file_preview(file_path, lines=10):
    """Stream file previews instead of loading entire files"""
    preview_data = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            header = f.readline().strip()
            columns = header.split(',')
            for i, line in enumerate(f):
                if i >= lines:
                    break
                values = line.strip().split(',')
                preview_data.append(dict(zip(columns, values)))
        return preview_data
    except Exception as e:
        logging.error(f"Error reading file preview: {e}")
        return []

@app.route('/preview/<job_id>')
def preview_results(job_id):
    try:
        job = job_service.get_job(job_id)
        preview = job_service.get_preview(job_id)
        response = {
            'data': preview['records'],
            'total_rows': preview['result_count'],
            'columns': preview['columns'],
            'status': job.status,
        }
        if job.status == 'completed' and not preview['records']:
            # Fallback to file preview for completed jobs with no cached records
            file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), job.output_file)
            if not os.path.exists(file_path):
                return jsonify({'error': 'File not found'}), 404
            preview_data = get_file_preview(file_path, 10)
            with open(file_path, 'r', encoding='utf-8') as f:
                columns = f.readline().strip().split(',')
                total_rows = sum(1 for _ in f)
            response.update({'data': preview_data, 'columns': columns, 'total_rows': max(total_rows, 0)})
        return jsonify(response)
    except JobNotFound:
        job = running_jobs.get(job_id)
        if not job or job.status != 'completed':
            return jsonify({'error': 'Results not ready'}), 400
        file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), job.output_file)
        if not os.path.exists(file_path):
            return jsonify({'error': 'File not found'}), 404
        try:
            preview_data = get_file_preview(file_path, 10)
            with open(file_path, 'r', encoding='utf-8') as f:
                columns = f.readline().strip().split(',')
                total_rows = sum(1 for _ in f)
            return jsonify({
                'data': preview_data,
                'total_rows': max(total_rows, 0),
                'columns': columns
            })
        except Exception as exc:
            return jsonify({'error': f'Error reading file: {exc}'}), 500

@app.route('/history')
def history():
    service_jobs = [job for job in job_service.list_jobs() if job.status in ['completed', 'failed', 'interrupted']]
    legacy_jobs = [job for job in running_jobs.values() if job.status in ['completed', 'failed', 'interrupted']]
    all_jobs = service_jobs + legacy_jobs
    all_jobs.sort(key=lambda job: job.start_time, reverse=True)
    return render_template('history.html', jobs=all_jobs)

if __name__ == '__main__':
    socketio.run(app, debug=True, host='0.0.0.0', port=5000)
