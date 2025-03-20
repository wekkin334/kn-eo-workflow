from parliament import Context
from cloudevents.http import CloudEvent
import os
import time
import logging
import json
import uuid
from datetime import datetime, timezone
from flask import Request, jsonify
from minio import Minio
import io

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_config():
    return {
        'event_source': os.getenv('EVENT_SOURCE', 'eo-workflow/webhook'),
        'minio_endpoint': os.getenv('MINIO_ENDPOINT', 'minio.eo-workflow.svc.cluster.local:9000'),
        'minio_access_key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
        'minio_secret_key': os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
        'minio_secure': os.getenv('MINIO_SECURE', 'false').lower() == 'true',
        'request_queue': os.getenv('REQUEST_QUEUE', 'requests')
    }

def initialize_minio_client(config):
    """Initialize and return a MinIO client"""
    return Minio(
        config['minio_endpoint'],
        access_key=config['minio_access_key'],
        secret_key=config['minio_secret_key'],
        secure=config['minio_secure']
    )

def ensure_request_queue(minio_client, bucket_name):
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
        logger.info(f"Created request queue bucket: {bucket_name}")

def submit_request(minio_client, bucket_name, request_data):
    request_id = request_data.get('request_id', str(uuid.uuid4()))
    
    if 'request_id' not in request_data:
        request_data['request_id'] = request_id
    
    request_data['timestamp'] = int(time.time())
    
    request_json = json.dumps(request_data)
    object_name = f"request-{request_id}-{int(time.time())}.json"
    
    minio_client.put_object(
        bucket_name,
        object_name,
        io.BytesIO(request_json.encode('utf-8')),
        length=len(request_json),
        content_type="application/json"
    )
    
    return request_id, object_name

def main(request: Request):
    try:
        logger.info("EO Webhook activated")
        
        config = get_config()
        
        if request.method != 'POST':
            return jsonify({
                "error": "Only POST method is allowed"
            }), 405
        
        try:
            body = request.get_json()
        except Exception as e:
            return jsonify({
                "error": f"Invalid JSON: {str(e)}"
            }), 400
        
        bbox = body.get('bbox')
        time_range = body.get('time_range')
        
        if not bbox or not time_range:
            return jsonify({
                "error": "Missing required parameters: bbox, time_range"
            }), 400
            
        if not isinstance(bbox, list) or len(bbox) != 4:
            return jsonify({
                "error": "bbox must be a list of 4 numbers [west, south, east, north]"
            }), 400
            
        cloud_cover = body.get('cloud_cover', 20)
        max_items = body.get('max_items', 3)
        
        request_id = str(uuid.uuid4())
        
        request_data = {
            "bbox": bbox,
            "time_range": time_range,
            "cloud_cover": cloud_cover,
            "max_items": max_items,
            "request_id": request_id,
            "timestamp": int(time.time())
        }
        
        minio_client = initialize_minio_client(config)
        ensure_request_queue(minio_client, config['request_queue'])    
        request_id, object_name = submit_request(minio_client, config['request_queue'], request_data)
    
        return jsonify({
            "status": "success",
            "message": "Earth Observation request submitted to processing queue",
            "request_id": request_id,
            "queue_object": object_name,
            "parameters": {
                "bbox": bbox,
                "time_range": time_range,
                "cloud_cover": cloud_cover,
                "max_items": max_items
            }
        }), 202
        
    except Exception as e:
        logger.exception(f"Error in webhook handler: {str(e)}")
        return jsonify({
            "error": str(e)
        }), 500