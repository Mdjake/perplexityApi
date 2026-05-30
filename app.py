from flask import Flask, request, jsonify, Response, stream_with_context
import requests
import json
import uuid
import time
import re
from collections import deque
from functools import lru_cache
import gc
import random

from flask_cors import CORS



app = Flask(__name__)
CORS(app)  # This enables CORS for all routes


# Configuration
MAX_HISTORY = 100
MAX_RESPONSE_SIZE = 1024 * 1024 * 10  # 10MB limit
STREAM_TIMEOUT = 120
CLEANUP_INTERVAL = 3600  # 1 hour

# In-memory store with limits
class MemoryEfficientStore:
    def __init__(self, max_size=MAX_HISTORY):
        self.sessions = {}
        self.max_size = max_size
        self.access_times = {}
    
    def add_session(self, session_id, session_data):
        if len(self.sessions) >= self.max_size:
            # Remove least recently used session
            oldest = min(self.access_times, key=self.access_times.get)
            del self.sessions[oldest]
            del self.access_times[oldest]
            gc.collect()
        
        self.sessions[session_id] = session_data
        self.access_times[session_id] = time.time()
    
    def get_session(self, session_id):
        if session_id in self.sessions:
            self.access_times[session_id] = time.time()
            return self.sessions[session_id]
        return None
    
    def cleanup(self):
        """Remove expired sessions"""
        current_time = time.time()
        expired = [
            sid for sid, atime in self.access_times.items()
            if current_time - atime > CLEANUP_INTERVAL
        ]
        for sid in expired:
            if sid in self.sessions:
                # Close session properly
                if 'session' in self.sessions[sid]:
                    self.sessions[sid]['session'].close()
                del self.sessions[sid]
            del self.access_times[sid]
        gc.collect()

store = MemoryEfficientStore()

@app.route('/', methods=['GET'])
def home():
    return jsonify({
        'status': 'success',
        'message': 'Perplexity AI API (Memory Optimized)',
        'memory_limits': {
            'max_history': MAX_HISTORY,
            'max_response_size': f'{MAX_RESPONSE_SIZE // 1024}KB',
            'session_timeout': f'{CLEANUP_INTERVAL // 60} minutes'
        }
    })

def stream_parse_response(response_stream):
    """Memory-efficient streaming parser - yields chunks instead of loading all"""
    answer_parts = []
    sources = None
    metadata = {}
    total_size = 0
    
    for line in response_stream.iter_lines(decode_unicode=True):
        if not line or not line.startswith('data: '):
            continue
        
        # Limit response size
        total_size += len(line)
        if total_size > MAX_RESPONSE_SIZE:
            yield {"error": "Response too large", "partial": ''.join(answer_parts)}
            break
        
        json_str = line[6:].strip()
        if not json_str or json_str == '{}':
            continue
        
        try:
            data = json.loads(json_str)
            
            if 'backend_uuid' in data:
                metadata['backend_uuid'] = data['backend_uuid']
            
            if 'text' in data and data.get('step_type') == 'FINAL':
                text_content = data['text']
                try:
                    steps = json.loads(text_content)
                    if isinstance(steps, list):
                        for step in steps:
                            if step.get('step_type') == 'FINAL':
                                answer_str = step.get('content', {}).get('answer', '')
                                if answer_str:
                                    answer_data = json.loads(answer_str)
                                    answer_text = answer_data.get('answer', '')
                                    sources = answer_data.get('web_results', [])
                                    if answer_text:
                                        # Stream answer in chunks
                                        for chunk in chunk_text(answer_text, 256):
                                            yield {"chunk": chunk}
                                        answer_parts.append(answer_text)
                                    break
                except:
                    pass
            
            if 'blocks' in data and not answer_parts:
                for block in data['blocks']:
                    if block.get('intended_usage') in ['ask_text_0_markdown', 'ask_text']:
                        markdown_block = block.get('markdown_block', {})
                        if markdown_block.get('answer'):
                            answer_text = markdown_block['answer']
                            for chunk in chunk_text(answer_text, 256):
                                yield {"chunk": chunk}
                            answer_parts.append(answer_text)
                            break
        
        except json.JSONDecodeError:
            continue
        
        # Force garbage collection periodically
        if total_size % (1024 * 100) == 0:  # Every 100KB
            gc.collect()
    
    # Send final response
    yield {
        "final": ''.join(answer_parts),
        "sources": sources or [],
        "metadata": metadata
    }

def chunk_text(text, chunk_size=256):
    """Split text into smaller chunks for streaming"""
    for i in range(0, len(text), chunk_size):
        yield text[i:i + chunk_size]

@lru_cache(maxsize=128)
def get_cached_session_data(session_key):
    """Cache session data with LRU eviction"""
    # This would need to be implemented properly
    # Simplified for example
    return None

def scrape_fresh_session_memory_efficient():
    """Memory-optimized session scraper"""
    session = requests.Session()
    
    # Reuse headers dict
    headers = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Encoding': 'gzip, deflate',  # Removed br/zstd for compatibility
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    }
    
    try:
        with session.get('https://www.perplexity.ai', headers=headers, timeout=30, stream=True) as response:
            # Process in chunks instead of loading all HTML
            html_chunks = []
            total_size = 0
            
            for chunk in response.iter_content(chunk_size=8192, decode_unicode=True):
                if chunk:
                    html_chunks.append(chunk)
                    total_size += len(chunk)
                    if total_size > 1024 * 100:  # Limit to 100KB
                        break
            
            html = ''.join(html_chunks)
            
            # Extract data using more memory-efficient methods
            cookies = {cookie.name: cookie.value for cookie in session.cookies}
            
            visitor_id = cookies.get('pplx.visitor-id', str(uuid.uuid4()))
            session_id = cookies.get('pplx.session-id', str(uuid.uuid4()))
            
            # Use more efficient regex patterns
            version_match = re.search(r'"version":"([\d.]+)"', html)
            version = version_match.group(1) if version_match else '2.18'
            
            csrf_match = re.search(r'csrf-token["\']?\s*[:=]\s*["\']([^"\']+)', html)
            csrf_token = csrf_match.group(1) if csrf_match else f'{uuid.uuid4().hex}%7C{uuid.uuid4().hex}'
            
            api_url_match = re.search(r'"apiUrl":"([^"]+)"', html)
            api_url = api_url_match.group(1) if api_url_match else 'https://www.perplexity.ai/rest/sse/perplexity_ask'
            
            # Clear HTML from memory
            del html
            del html_chunks
            gc.collect()
            
            return {
                'session': session,
                'cookies': cookies,
                'visitor_id': visitor_id,
                'session_id': session_id,
                'version': version,
                'csrf_token': csrf_token,
                'api_url': api_url,
                'timestamp': int(time.time())
            }
    
    except Exception as e:
        session.close()
        raise e

@app.route('/api/ask', methods=['GET'])
def perplexity_ask_memory_efficient():
    prompt = request.args.get('prompt')
    
    if not prompt:
        return jsonify({'error': 'Prompt required'}), 400
    
    mode = request.args.get('mode', 'concise')
    model = request.args.get('model', 'turbo')
    search_focus = request.args.get('search_focus', 'internet')
    stream_mode = request.args.get('stream', 'false').lower() == 'true'
    
    # Clean up old sessions periodically
    if random.random() < 0.01:  # 1% chance on each request
        store.cleanup()
    
    try:
        # Reuse session if available
        session_key = f"{mode}_{model}_{search_focus}"
        scraped = store.get_session(session_key)
        
        if not scraped:
            scraped = scrape_fresh_session_memory_efficient()
            store.add_session(session_key, scraped)
        
        session = scraped['session']
        
        # Build payload with reuse of common structures
        base_params = {
            "attachments": [],
            "language": "en-US",
            "timezone": "Asia/Dhaka",
            "sources": ["web"],
            "is_related_query": False,
            "is_sponsored": False,
            "prompt_source": "user",
            "query_source": "followup",
            "is_incognito": False,
            "local_search_enabled": False,
            "use_schematized_api": True,
            "send_back_text_in_streaming_api": False,
            "client_coordinates": None,
            "mentions": [],
            "skip_search_enabled": True,
            "is_nav_suggestions_disabled": False,
            "followup_source": "link",
            "source": "mweb",
            "always_search_override": False,
            "override_no_search": False,
            "should_ask_for_mcp_tool_confirmation": True,
            "supported_features": ["browser_agent_permission_banner_v1.1"],
            "version": scraped['version']
        }
        
        # Create payload efficiently
        payload = {
            "params": {
                **base_params,
                "last_backend_uuid": str(uuid.uuid4()),
                "read_write_token": str(uuid.uuid4()),
                "search_focus": search_focus,
                "frontend_uuid": str(uuid.uuid4()),
                "mode": mode,
                "model_preference": model,
            },
            "query_str": prompt
        }
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36',
            'Accept': 'text/event-stream',
            'Content-Type': 'application/json',
            'x-request-id': str(uuid.uuid4()),
            'origin': 'https://www.perplexity.ai',
        }
        
        # Make streaming request
        response = session.post(
            scraped['api_url'],
            json=payload,
            headers=headers,
            stream=True,  # Important for memory efficiency
            timeout=STREAM_TIMEOUT
        )
        
        if stream_mode:
            # Stream response back to client
            def generate():
                for chunk_data in stream_parse_response(response):
                    yield f"data: {json.dumps(chunk_data)}\n\n"
            return Response(stream_with_context(generate()), mimetype='text/event-stream')
        else:
            # Non-streaming but memory efficient
            answer_parts = []
            sources = []
            metadata = {}
            
            for chunk_data in stream_parse_response(response):
                if 'chunk' in chunk_data:
                    answer_parts.append(chunk_data['chunk'])
                elif 'final' in chunk_data:
                    answer_text = chunk_data['final']
                    sources = chunk_data['sources']
                    metadata = chunk_data['metadata']
            
            result = {
                'status': 'success',
                'prompt': prompt,
                'answer': ''.join(answer_parts),
                'mode': mode,
                'model': model,
                
            }
            
            # Clear large objects
            del answer_parts
            gc.collect()
            
            return jsonify(result)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/clear_cache', methods=['POST'])
def clear_cache():
    """Manual cache clearing endpoint"""
    store.cleanup()
    gc.collect()
    return jsonify({'status': 'cleared', 'memory_freed': True})

if __name__ == '__main__':
    import random
    app.run(debug=False, host='0.0.0.0', port=5000, threaded=True)
