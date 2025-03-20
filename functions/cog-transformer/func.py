from parliament import Context
from cloudevents.http import CloudEvent
from minio import Minio
import os
import time
import logging
import json
import uuid
import io
import tempfile
import threading
from functools import wraps
from datetime import datetime, timezone
import rasterio
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SECRETS_PATH = '/vault/secrets/minio-config'

def get_config():
    return {
        'minio_endpoint': os.getenv('MINIO_ENDPOINT', 'minio.eo-workflow.svc.cluster.local:9000'),
        'raw_bucket': os.getenv('RAW_BUCKET', 'raw-assets'),
        'cog_bucket': os.getenv('COG_BUCKET', 'cog-assets'),
        'fmask_raw_bucket': os.getenv('FMASK_RAW_BUCKET', 'fmask-raw'),
        'fmask_cog_bucket': os.getenv('FMASK_COG_BUCKET', 'fmask-cog'),
        'max_retries': int(os.getenv('MAX_RETRIES', '3')),
        'connection_timeout': int(os.getenv('CONNECTION_TIMEOUT', '5')),
        'secure_connection': os.getenv('MINIO_SECURE', 'false').lower() == 'true',
        'event_source': os.getenv('EVENT_SOURCE', 'eo-workflow/cog-transformer'),
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

def create_cloud_event(event_type, data, source):
    return CloudEvent({
        "specversion": "1.0",
        "type": event_type,
        "source": source,
        "id": f"{event_type}-{int(time.time())}-{uuid.uuid4()}",
        "time": datetime.now(timezone.utc).isoformat(),
        "datacontenttype": "application/json",
    }, data)

class COGTransformer:
    def __init__(self, minio_client, config):
        self.minio_client = minio_client
        self.config = config
        self.raw_bucket = config['raw_bucket']
        self.cog_bucket = config['cog_bucket']
        self.fmask_raw_bucket = config['fmask_raw_bucket']
        self.fmask_cog_bucket = config['fmask_cog_bucket']
        self.max_retries = config['max_retries']
        self.connection_timeout = config.get('connection_timeout', 5)

    def ensure_bucket(self, bucket):
        try:
            if not self.minio_client.bucket_exists(bucket):
                logger.info(f"Creating bucket: {bucket}")
                self.minio_client.make_bucket(bucket)
                logger.info(f"Successfully created bucket: {bucket}")
            else:
                logger.info(f"Bucket {bucket} already exists and is ready")
        except Exception as e:
            logger.error(f"Failed to ensure bucket '{bucket}' exists: {str(e)}")
            raise

    def download_asset(self, bucket, object_name):
        logger.info(f"Downloading asset from bucket: {bucket}, object: {object_name}")
        for attempt in range(self.max_retries):
            try:
                response = self.minio_client.get_object(bucket, object_name)
                data = response.read()
                try:
                    metadata = self.minio_client.stat_object(bucket, object_name).metadata
                except:
                    metadata = {}
                response.close()
                response.release_conn()
                return data, metadata
            except Exception as e:
                if attempt == self.max_retries - 1:
                    logger.error(f"Failed to download asset after {self.max_retries} attempts: {str(e)}")
                    raise
                logger.warning(f"Download attempt {attempt + 1} failed: {str(e)}")
                time.sleep(self.connection_timeout)

    def convert_to_cog(self, input_data, profile="deflate"):
        with tempfile.NamedTemporaryFile(suffix='.tif') as tmp_src:
            tmp_src.write(input_data)
            tmp_src.flush()
            
            with tempfile.NamedTemporaryFile(suffix='.tif') as tmp_dst:
                output_profile = cog_profiles.get(profile)
                output_profile["blocksize"] = 512
                
                # for certain types of data, we may want to adjust the profile
                try:
                    with rasterio.open(tmp_src.name) as src:
                        dtype = src.dtypes[0]
                        # for higher bit-depth data, adjust compression
                        if dtype in ['uint16', 'int16', 'float32']:
                            output_profile["compress"] = "deflate"
                            output_profile["predictor"] = 2
                except Exception as e:
                    logger.warning(f"Could not determine input datatype, using default profile: {str(e)}")
                
                try:
                    cog_translate(
                        tmp_src.name,
                        tmp_dst.name,
                        output_profile,
                        quiet=True
                    )
                except Exception as e:
                    logger.error(f"Error in COG conversion: {str(e)}")
                    raise RuntimeError(f"COG conversion failed: {str(e)}")
                
                with open(tmp_dst.name, 'rb') as f:
                    cog_data = f.read()
                
                return cog_data

    def process_asset(self, event_data):
        # we determine if this is a standard asset or a FMask
        event_type = event_data.get("event_type", "")
        is_fmask = event_type == "eo.fmask.completed"
        
        if is_fmask:
            # process FMask
            bucket = self.fmask_raw_bucket
            object_name = event_data.get("object_name")
            target_bucket = self.fmask_cog_bucket
        else:
            # process standard asset from raw-assets bucket
            bucket = self.raw_bucket
            object_name = event_data.get("object_name")
            target_bucket = self.cog_bucket
        
        asset_data, metadata = self.download_asset(bucket, object_name)
        
        try:
            cog_data = self.convert_to_cog(asset_data)
        except Exception as e:
            logger.error(f"COG conversion failed: {str(e)}")
            raise RuntimeError(f"COG conversion failed: {str(e)}")
        
        self.ensure_bucket(target_bucket)
        
        for attempt in range(self.max_retries):
            try:
                self.minio_client.put_object(
                    target_bucket,
                    object_name,
                    io.BytesIO(cog_data),
                    length=len(cog_data),
                    content_type="image/tiff",
                    metadata=metadata
                )
                logger.info(f"Stored COG in bucket {target_bucket}, object: {object_name}")
                break
            except Exception as e:
                if attempt == self.max_retries - 1:
                    logger.error(f"Failed to store COG after {self.max_retries} attempts: {str(e)}")
                    raise
                logger.warning(f"Storage attempt {attempt + 1} failed: {str(e)}")
                time.sleep(self.connection_timeout)
        
        result = {
            "source_bucket": bucket,
            "source_object": object_name,
            "cog_bucket": target_bucket,
            "cog_object": object_name,
            "size": len(cog_data),
            "item_id": event_data.get("item_id"),
            "asset_id": event_data.get("asset_id"),
            "collection": event_data.get("collection"),
            "request_id": event_data.get("request_id", str(uuid.uuid4())),
            "is_fmask": is_fmask
        }
        
        return result

# @ensure_vault_secrets
def main(context: Context):
    try:
        logger.info("COG Transformer function activated")
        
        config = get_config()
        
        if not hasattr(context, 'cloud_event'):
            error_msg = "No cloud event in context"
            logger.error(error_msg)
            return {"error": error_msg}, 400
        
        event_data = context.cloud_event.data
        if isinstance(event_data, str):
            event_data = json.loads(event_data)
        
        event_type = context.cloud_event["type"]
        event_data["event_type"] = event_type
        
        bucket = event_data.get("bucket")
        object_name = event_data.get("object_name")
        item_id = event_data.get("item_id")
        
        if not object_name or not item_id:
            error_msg = "Missing required parameters (object_name, item_id)"
            logger.error(error_msg)
            return {"error": error_msg}, 400
        
        logger.info(f"Processing asset: {object_name} from bucket: {bucket}")
        
        minio_manager = MinioClientManager(config)
        minio_client = minio_manager.initialize_client()
        
        transformer = COGTransformer(minio_client, config)
        
        result = transformer.process_asset(event_data)
        
        if result["is_fmask"]:
            response_event_type = "eo.fmask.transformed"
        else:
            response_event_type = "eo.asset.transformed"
        
        if "asset_id" not in result or not result["asset_id"]:
            # we extract asset_id from object_name if possible
            parts = object_name.split("/")
            if len(parts) >= 3:
                result["asset_id"] = parts[-1]
        
        response_event = create_cloud_event(
            response_event_type,
            result,
            config['event_source']
        )
        
        return response_event
        
    except Exception as e:
        logger.exception(f"Error in COG transformer function: {str(e)}")
        error_data = {
            "error": str(e),
            "error_type": type(e).__name__,
            "bucket": event_data.get("bucket") if "event_data" in locals() else None,
            "object_name": event_data.get("object_name") if "event_data" in locals() else None,
            "request_id": event_data.get("request_id") if "event_data" in locals() else str(uuid.uuid4())
        }
        error_event = create_cloud_event(
            "eo.processing.error",
            error_data,
            config['event_source']
        )
        return error_event