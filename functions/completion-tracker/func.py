from parliament import Context
from cloudevents.http import CloudEvent
from minio import Minio
import os
import time
import logging
import json
import uuid
import threading
import redis
from functools import wraps
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SECRETS_PATH = '/vault/secrets/minio-config'

def get_config():
    return {
        'minio_endpoint': os.getenv('MINIO_ENDPOINT', 'minio.eo-workflow.svc.cluster.local:9000'),
        'cog_bucket': os.getenv('COG_BUCKET', 'cog-assets'),
        'max_retries': int(os.getenv('MAX_RETRIES', '3')),
        'connection_timeout': int(os.getenv('CONNECTION_TIMEOUT', '5')),
        'secure_connection': os.getenv('MINIO_SECURE', 'false').lower() == 'true',
        'event_source': os.getenv('EVENT_SOURCE', 'eo-workflow/completion-tracker'),
        'redis_host': os.getenv('REDIS_HOST', 'redis.eo-workflow.svc.cluster.local'),
        'redis_port': int(os.getenv('REDIS_PORT', '6379')),
        'redis_key_prefix': os.getenv('REDIS_KEY_PREFIX', 'eo:tracker:'),
        'redis_key_expiry': int(os.getenv('REDIS_KEY_EXPIRY', '86400'))  # 24 hours
    }

def wait_for_vault_secrets():
    max_retries = 30
    retry_count = 0
    secrets_file = SECRETS_PATH
    
    while not os.path.exists(secrets_file):
        if retry_count >= max_retries:
            raise RuntimeError("Timed out waiting for Vault secrets")
        logger.info(f"Waiting for Vault secrets (attempt {retry_count + 1}/{max_retries})...")
        time.sleep(2)
        retry_count += 1
    
    with open(secrets_file, 'r') as f:
        for line in f:
            if line.startswith('export'):
                key, value = line.replace('export ', '').strip().split('=', 1)
                os.environ[key.strip()] = value.strip().strip('"\'')
    
    logger.info("Vault secrets loaded successfully")

def ensure_vault_secrets(func):
    initialization_complete = threading.Event()
    
    @wraps(func)
    def wrapper(*args, **kwargs):
        if not initialization_complete.is_set():
            try:
                wait_for_vault_secrets()
                initialization_complete.set()
            except Exception as e:
                logger.error(f"Failed to initialize Vault secrets: {str(e)}")
                raise
        return func(*args, **kwargs)
    return wrapper

class MinioClientManager:
    def __init__(self, config):
        self.config = config
        self.endpoint = config['minio_endpoint']
        self.max_retries = config['max_retries']
        self.secure = config['secure_connection']
        self.connection_timeout = config.get('connection_timeout', 5)

    def get_credentials(self):
        try:
            access_key = os.getenv("MINIO_ROOT_USER")
            secret_key = os.getenv("MINIO_ROOT_PASSWORD")
            
            logger.info("Checking for Vault-injected MinIO credentials")
            logger.info(f"MinIO credentials present: access_key={'YES' if access_key else 'NO'}, secret_key={'YES' if secret_key else 'NO'}")
            
            if not access_key or not secret_key:
                logger.error("MinIO credentials not found - ensure Vault injection is working")
                raise RuntimeError("MinIO credentials not available")
            
            return access_key, secret_key
            
        except Exception as e:
            logger.error(f"Error retrieving credentials: {str(e)}")
            raise RuntimeError(f"Failed to get MinIO credentials: {str(e)}")

    def initialize_client(self):
        for attempt in range(self.max_retries):
            try:
                access_key, secret_key = self.get_credentials()
                logger.info(f"Initializing MinIO client with endpoint: {self.endpoint}")
                
                client = Minio(
                    self.endpoint,
                    access_key=access_key,
                    secret_key=secret_key,
                    secure=self.secure
                )
                
                client.list_buckets()
                logger.info("Successfully connected to MinIO")
                return client
                
            except Exception as e:
                if attempt == self.max_retries - 1:
                    logger.error(f"Failed to initialize MinIO client after {self.max_retries} attempts: {str(e)}")
                    raise
                logger.warning(f"Attempt {attempt + 1} failed, retrying: {str(e)}")
                time.sleep(self.connection_timeout)

class RedisTracker:
    def __init__(self, config):
        """Initialize the Redis connection"""
        self.host = config['redis_host']
        self.port = config['redis_port']
        self.key_prefix = config['redis_key_prefix']
        self.key_expiry = config['redis_key_expiry']
        self.max_retries = config.get('max_retries', 3)
        self.connection_timeout = config.get('connection_timeout', 5)
        self.redis_client = self._initialize_connection()
        
    def _initialize_connection(self):
        """Establish connection to Redis with retries"""
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Connecting to Redis at {self.host}:{self.port}")
                client = redis.Redis(
                    host=self.host,
                    port=self.port,
                    socket_timeout=self.connection_timeout,
                    socket_connect_timeout=self.connection_timeout,
                    retry_on_timeout=True,
                    decode_responses=True  # Return string values instead of bytes
                )
                client.ping()  # Verify connection
                logger.info("Successfully connected to Redis")
                return client
            except Exception as e:
                if attempt == self.max_retries - 1:
                    logger.error(f"Failed to connect to Redis after {self.max_retries} attempts: {str(e)}")
                    raise
                logger.warning(f"Redis connection attempt {attempt + 1} failed, retrying: {str(e)}")
                time.sleep(self.connection_timeout)
    
    def _get_scene_key(self, request_id, item_id):
        """Get Redis key for a specific scene"""
        return f"{self.key_prefix}scene:{request_id}:{item_id}"
    
    def _get_processed_key(self, request_id, item_id):
        """Get Redis key for processed assets of a scene"""
        return f"{self._get_scene_key(request_id, item_id)}:processed"
    
    def _get_expected_key(self, request_id, item_id):
        """Get Redis key for expected assets of a scene"""
        return f"{self._get_scene_key(request_id, item_id)}:expected"
        
    def _get_complete_key(self, request_id, item_id):
        """Get Redis key for completion status of a scene"""
        return f"{self._get_scene_key(request_id, item_id)}:complete"
        
    def _get_metadata_key(self, request_id, item_id):
        """Get Redis key for metadata of a scene"""
        return f"{self._get_scene_key(request_id, item_id)}:metadata"
        
    def set_expected_assets(self, request_id, item_id, assets, metadata=None):
        try:
            expected_key = self._get_expected_key(request_id, item_id)
            processed_key = self._get_processed_key(request_id, item_id)
            complete_key = self._get_complete_key(request_id, item_id)
            metadata_key = self._get_metadata_key(request_id, item_id)
            
            # Use a pipeline to execute multiple commands atomically
            with self.redis_client.pipeline() as pipe:
                # Clear any existing data (in case of reprocessing)
                pipe.delete(expected_key)
                
                # Add all expected assets
                if assets:
                    pipe.sadd(expected_key, *assets)
                
                # Set expiry for expected assets key
                pipe.expire(expected_key, self.key_expiry)
                
                # Initialize processed assets set if it doesn't exist
                pipe.expire(processed_key, self.key_expiry)
                
                # Set completion flag to 0 (not complete)
                pipe.set(complete_key, "0")
                pipe.expire(complete_key, self.key_expiry)
                
                # Store metadata if provided
                if metadata:
                    pipe.delete(metadata_key)
                    pipe.hset(metadata_key, mapping=metadata)
                    pipe.expire(metadata_key, self.key_expiry)
                
                # Execute all commands
                pipe.execute()
                
            logger.info(f"Set {len(assets)} expected assets for scene {item_id} (request {request_id})")
            return True
            
        except Exception as e:
            logger.error(f"Error setting expected assets: {str(e)}")
            return False
    
    def record_asset(self, request_id, item_id, asset_id):
        try:
            processed_key = self._get_processed_key(request_id, item_id)
            
            # Add the asset to the processed set
            self.redis_client.sadd(processed_key, asset_id)
            self.redis_client.expire(processed_key, self.key_expiry)
            
            logger.info(f"Recorded asset {asset_id} for scene {item_id} (request {request_id})")
            return True
            
        except Exception as e:
            logger.error(f"Error recording asset {asset_id}: {str(e)}")
            return False
    
    def is_complete(self, request_id, item_id):
        try:
            # Check if completion has already been flagged
            complete_key = self._get_complete_key(request_id, item_id)
            if self.redis_client.get(complete_key) == "1":
                logger.info(f"Scene {item_id} already marked as complete")
                return False  # Already marked as complete, no need to trigger again
            
            expected_key = self._get_expected_key(request_id, item_id)
            processed_key = self._get_processed_key(request_id, item_id)
            
            # Get the expected and processed assets
            expected_assets = self.redis_client.smembers(expected_key)
            processed_assets = self.redis_client.smembers(processed_key)
            
            # If there are no expected assets, the scene is not complete
            if not expected_assets:
                logger.warning(f"No expected assets found for scene {item_id} (request {request_id})")
                return False
            
            # Check if all expected assets have been processed
            missing_assets = expected_assets - processed_assets
            is_complete = len(missing_assets) == 0
            
            if is_complete:
                logger.info(f"All {len(expected_assets)} assets have been processed for scene {item_id}")
                
                # Mark as complete to avoid duplicate processing
                self.redis_client.set(complete_key, "1")
                self.redis_client.expire(complete_key, self.key_expiry)
                
                return True
            else:
                logger.info(f"Scene {item_id} not yet complete. Missing {len(missing_assets)} assets: {missing_assets}")
                return False
                
        except Exception as e:
            logger.error(f"Error checking completion status: {str(e)}")
            return False
    
    def get_scene_metadata(self, request_id, item_id):
        try:
            metadata_key = self._get_metadata_key(request_id, item_id)
            metadata = self.redis_client.hgetall(metadata_key)
            return metadata
        except Exception as e:
            logger.error(f"Error getting scene metadata: {str(e)}")
            return {}
    
    def get_processed_assets(self, request_id, item_id):
        try:
            processed_key = self._get_processed_key(request_id, item_id)
            return self.redis_client.smembers(processed_key)
        except Exception as e:
            logger.error(f"Error getting processed assets: {str(e)}")
            return set()

def create_cloud_event(event_type, data, source):
    return CloudEvent({
        "specversion": "1.0",
        "type": event_type,
        "source": source,
        "id": f"{event_type}-{int(time.time())}-{uuid.uuid4()}",
        "time": datetime.now(timezone.utc).isoformat(),
        "datacontenttype": "application/json",
    }, data)

class CompletionTracker:
    def __init__(self, minio_client, redis_tracker, config):
        self.minio_client = minio_client
        self.redis_tracker = redis_tracker
        self.config = config
        self.cog_bucket = config['cog_bucket']

    def record_transformed_asset(self, event_data):
        request_id = event_data.get("request_id", "unknown")
        item_id = event_data.get("item_id", "unknown")
        asset_id = event_data.get("asset_id", "unknown")
        collection = event_data.get("collection", "unknown")
        
        logger.info(f"Recording transformed asset: {asset_id} for item: {item_id}, request: {request_id}")
        
        self.redis_tracker.record_asset(request_id, item_id, asset_id)
        
        is_complete = self.redis_tracker.is_complete(request_id, item_id)
        if is_complete:
            logger.info(f"All assets for item {item_id} have been processed!")
            
            processed_assets = list(self.redis_tracker.get_processed_assets(request_id, item_id))
            
            metadata = self.redis_tracker.get_scene_metadata(request_id, item_id)
            
            result = {
                "request_id": request_id,
                "item_id": item_id,
                "collection": collection,
                "assets": processed_assets,
                "cog_bucket": self.cog_bucket,
                "timestamp": int(time.time())
            }
            
            if "bbox" in metadata:
                try:
                    result["bbox"] = json.loads(metadata["bbox"])
                except Exception as e:
                    logger.warning(f"Could not parse bbox from metadata: {str(e)}")
            
            if "acquisition_date" in metadata:
                result["acquisition_date"] = metadata["acquisition_date"]
            
            return result
        
        return None

    def set_expected_assets(self, event_data):
        request_id = event_data.get("request_id", "unknown")
        item_id = event_data.get("item_id", "unknown")
        assets = event_data.get("assets", [])
        collection = event_data.get("collection", "unknown")
        
        metadata = {
            "collection": collection,
            "bbox": json.dumps(event_data.get("bbox", [])),
            "acquisition_date": event_data.get("acquisition_date", ""),
            "cloud_cover": str(event_data.get("cloud_cover", "")),
            "asset_count": str(len(assets))
        }
        
        logger.info(f"Setting {len(assets)} expected assets for item: {item_id}, request: {request_id}")
        
        self.redis_tracker.set_expected_assets(request_id, item_id, assets, metadata)
        
        return {
            "request_id": request_id,
            "item_id": item_id,
            "collection": collection,
            "assets_count": len(assets),
            "status": "registered"
        }

# @ensure_vault_secrets
def main(context: Context):
    try:
        logger.info("Completion Tracker function activated")
        config = get_config()
        
        if not hasattr(context, 'cloud_event'):
            error_msg = "No cloud event in context"
            logger.error(error_msg)
            return {"error": error_msg}, 400
        
        event_data = context.cloud_event.data
        if isinstance(event_data, str):
            event_data = json.loads(event_data)
        
        event_type = context.cloud_event["type"]
        
        minio_manager = MinioClientManager(config)
        minio_client = minio_manager.initialize_client()
        redis_tracker = RedisTracker(config)
        tracker = CompletionTracker(minio_client, redis_tracker, config)
        
        if event_type == "eo.scene.assets.registered":
            # This is a scene registration, record the expected assets
            result = tracker.set_expected_assets(event_data)
            return {"status": "recorded_expectations", 
                    "request_id": event_data.get("request_id"),
                    "item_id": event_data.get("item_id"),
                    "assets_count": len(event_data.get("assets", []))}, 200
            
        elif event_type == "eo.asset.transformed":
            completion_result = tracker.record_transformed_asset(event_data)
            
            if completion_result:
                # scene ready event
                response_event = create_cloud_event(
                    "eo.scene.ready",
                    completion_result,
                    config['event_source']
                )
                logger.info(f"Emitting eo.scene.ready event for item {completion_result['item_id']}")
                return response_event
            else:
                return {
                    "status": "asset_recorded", 
                    "asset_id": event_data.get("asset_id"),
                    "item_id": event_data.get("item_id"),
                    "request_id": event_data.get("request_id")
                }, 200
        
        else:
            logger.warning(f"Unknown event type: {event_type}")
            return {"error": f"Unknown event type: {event_type}"}, 400
        
    except Exception as e:
        logger.exception(f"Error in completion tracker function: {str(e)}")
        error_data = {
            "error": str(e),
            "error_type": type(e).__name__,
            "request_id": event_data.get("request_id") if "event_data" in locals() else str(uuid.uuid4())
        }
        error_event = create_cloud_event(
            "eo.processing.error",
            error_data,
            config['event_source']
        )
        return error_event