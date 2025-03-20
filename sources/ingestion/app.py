import os
import time
import json
import uuid
import logging
import threading
import io
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import signal
import sys
from minio import Minio
import planetary_computer
from pystac_client import Client
from cloudevents.http import CloudEvent, to_structured
import aiohttp
import asyncio

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

PC_CATALOG_URL = "https://planetarycomputer.microsoft.com/api/stac/v1"

def get_config():
    """Get configuration from environment variables with sensible defaults"""
    return {
        'minio_endpoint': os.getenv('MINIO_ENDPOINT', 'minio.eo-workflow.svc.cluster.local:9000'),
        'minio_access_key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
        'minio_secret_key': os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
        'raw_bucket': os.getenv('RAW_BUCKET', 'raw-assets'),
        'catalog_url': os.getenv('STAC_CATALOG_URL', PC_CATALOG_URL),
        'collection': os.getenv('STAC_COLLECTION', 'sentinel-2-l2a'),
        'max_cloud_cover': float(os.getenv('MAX_CLOUD_COVER', '20')),
        'max_items': int(os.getenv('MAX_ITEMS', '3')),
        'max_retries': int(os.getenv('MAX_RETRIES', '3')),
        'connection_timeout': int(os.getenv('CONNECTION_TIMEOUT', '5')),
        'max_workers': int(os.getenv('MAX_WORKERS', '4')),
        'secure_connection': os.getenv('MINIO_SECURE', 'false').lower() == 'true',
        'event_source': os.getenv('EVENT_SOURCE', 'eo-workflow/ingestion'),
        'sink_url': os.getenv('K_SINK'),
        'watch_interval': int(os.getenv('WATCH_INTERVAL', '60')),  # seconds
        'request_queue': os.getenv('REQUEST_QUEUE', 'requests')    # name of queue folder in MinIO
    }

class MinioClientManager:
    def __init__(self, config):
        self.config = config
        self.endpoint = config['minio_endpoint']
        self.access_key = config['minio_access_key']
        self.secret_key = config['minio_secret_key']
        self.max_retries = config['max_retries']
        self.secure = config['secure_connection']

    def initialize_client(self):
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Initializing MinIO client with endpoint: {self.endpoint}")
                
                client = Minio(
                    self.endpoint,
                    access_key=self.access_key,
                    secret_key=self.secret_key,
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
                time.sleep(self.config['connection_timeout'])

def create_cloud_event(event_type, data, source):
    return CloudEvent({
        "specversion": "1.0",
        "type": event_type,
        "source": source,
        "id": f"{event_type}-{int(time.time())}-{uuid.uuid4()}",
        "time": datetime.now(timezone.utc).isoformat(),
        "datacontenttype": "application/json",
    }, data)

class STACIngestionManager:
    def __init__(self, minio_client, config):
        self.minio_client = minio_client
        self.config = config
        self.raw_bucket = config['raw_bucket']
        self.request_queue = config['request_queue']
        self.connection_timeout = config.get('connection_timeout', 5)
        self.stac_client = Client.open(
            config['catalog_url'],
            modifier=planetary_computer.sign_inplace
        )
        self.max_workers = config['max_workers']

    def ensure_buckets(self):
        """Ensure the raw assets bucket and request queue bucket exist in MinIO"""
        for bucket in [self.raw_bucket, self.request_queue]:
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

    def search_scenes(self, bbox, time_range, cloud_cover=None, max_items=None):
        logger.info(f"Searching for {self.config['collection']} scenes in bbox: {bbox}, timerange: {time_range}")
        
        if cloud_cover is None:
            cloud_cover = self.config['max_cloud_cover']
        
        if max_items is None:
            max_items = self.config['max_items']
        
        search = self.stac_client.search(
            collections=[self.config['collection']],
            bbox=bbox,
            datetime=time_range,
            query={
                "eo:cloud_cover": {"lt": cloud_cover},
                "s2:degraded_msi_data_percentage": {"lt": 5},
                "s2:nodata_pixel_percentage": {"lt": 10}
            },
            limit=max_items
        )
        
        items = list(search.get_items())
        
        if not items:
            logger.warning("No scenes found for the specified parameters.")
            return []
        
        logger.info(f"Found {len(items)} scene(s)")
        return items

    def download_asset(self, item, asset_id):
        """Download a specific asset from a STAC item"""
        if asset_id not in item.assets:
            raise ValueError(f"Asset {asset_id} not found in item {item.id}")
        
        asset = item.assets[asset_id]
        
        asset_href = planetary_computer.sign(asset.href)
        
        for attempt in range(self.config['max_retries']):
            try:
                response = requests.get(asset_href, stream=True, timeout=self.connection_timeout)
                response.raise_for_status()
                
                metadata = {
                    "item_id": item.id,
                    "collection": item.collection_id,
                    "asset_id": asset_id,
                    "content_type": asset.media_type,
                    "original_href": asset.href,
                    "timestamp": int(time.time()),
                    "bbox": json.dumps(item.bbox),
                    "datetime": item.datetime.isoformat() if item.datetime else None
                }
                
                return response.content, metadata
            except requests.exceptions.RequestException as e:
                if attempt == self.config['max_retries'] - 1:
                    logger.error(f"Failed to download asset {asset_id} after {self.config['max_retries']} attempts: {str(e)}")
                    raise
                logger.warning(f"Download attempt {attempt + 1} failed: {str(e)}")
                time.sleep(self.connection_timeout)

    def store_asset(self, item_id, asset_id, data, metadata):
        collection = metadata.get("collection", "unknown")
        object_name = f"{collection}/{item_id}/{asset_id}"
        
        content_type = metadata.get("content_type", "application/octet-stream")
        
        for attempt in range(self.config['max_retries']):
            try:
                self.minio_client.put_object(
                    self.raw_bucket,
                    object_name,
                    io.BytesIO(data),
                    length=len(data),
                    content_type=content_type,
                    metadata=metadata
                )
                
                logger.info(f"Stored asset {asset_id} from item {item_id} in bucket {self.raw_bucket}")
                
                return {
                    "bucket": self.raw_bucket,
                    "object_name": object_name,
                    "item_id": item_id,
                    "asset_id": asset_id,
                    "collection": collection,
                    "size": len(data),
                    "content_type": content_type
                }
            except Exception as e:
                if attempt == self.config['max_retries'] - 1:
                    logger.error(f"Failed to store asset {asset_id} after {self.config['max_retries']} attempts: {str(e)}")
                    raise
                logger.warning(f"Storage attempt {attempt + 1} failed: {str(e)}")
                time.sleep(self.connection_timeout)

    def process_single_asset(self, item, asset_id, request_id):
        try:
            logger.info(f"Processing asset {asset_id} for item {item.id}")    
            asset_data, asset_metadata = self.download_asset(item, asset_id)
            result = self.store_asset(item.id, asset_id, asset_data, asset_metadata)
            result["request_id"] = request_id
            
            return result
        except Exception as e:
            logger.error(f"Error processing asset {asset_id}: {str(e)}")
            raise

    def process_scene(self, item, request_id, bbox):
        """Process a single scene's assets and prepare events"""
        scene_id = item.id
        collection_id = item.collection_id
        acquisition_date = item.datetime.strftime('%Y-%m-%d') if item.datetime else None
        cloud_cover = item.properties.get('eo:cloud_cover', 'N/A')
        
        logger.info(f"Processing scene: {scene_id} from {collection_id}, date: {acquisition_date}, cloud cover: {cloud_cover}%")
        
        # Determine which assets to ingest
        band_assets = [f"B{i:02d}" for i in range(1, 13)] + ["B8A", "SCL"]
        assets_to_ingest = [band for band in band_assets if band in item.assets]
        
        if not assets_to_ingest:
            logger.warning(f"No assets to ingest for scene {scene_id}")
            return None, []
        
        # Create registration event data
        registration_data = {
            "request_id": request_id,
            "item_id": scene_id,
            "collection": collection_id,
            "assets": assets_to_ingest,
            "bbox": bbox,
            "acquisition_date": acquisition_date,
            "cloud_cover": cloud_cover
        }
        
        # Create registration event
        registration_event = create_cloud_event(
            "eo.scene.assets.registered",
            registration_data,
            self.config['event_source']
        )
        
        processed_assets = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_asset = {
                executor.submit(self.process_single_asset, item, asset_id, request_id): asset_id
                for asset_id in assets_to_ingest
            }
            
            for future in as_completed(future_to_asset):
                asset_id = future_to_asset[future]
                try:
                    result = future.result()
                    processed_assets.append(result)
                    logger.info(f"Successfully processed asset {asset_id} for scene {scene_id}")
                except Exception as e:
                    logger.error(f"Failed to process asset {asset_id} for scene {scene_id}: {str(e)}")
        
        return registration_event, processed_assets

    def process_scenes(self, items, request_id, bbox):
        """Process multiple scenes and manage event emission"""
        results = []
        
        for item in items:
            try:
                # Process scene - returns registration event and processed assets
                registration_event, processed_assets = self.process_scene(item, request_id, bbox)
                
                if registration_event:
                    # Create asset ingestion events for each processed asset
                    asset_events = []
                    for asset_data in processed_assets:
                        asset_event = create_cloud_event(
                            "eo.asset.ingested",
                            asset_data,
                            self.config['event_source']
                        )
                        asset_events.append(asset_event)
                    
                    results.append({
                        "scene_id": item.id,
                        "registration_event": registration_event,
                        "asset_events": asset_events,
                        "processed_count": len(processed_assets)
                    })
            except Exception as e:
                logger.error(f"Error processing scene {item.id}: {str(e)}")
        
        return results

    async def send_events(self, events, sink_url):
        """Send multiple events to the sink asynchronously"""
        if not sink_url:
            logger.error("No sink URL provided. Cannot send events.")
            return False
            
        logger.info(f"Sending {len(events)} events to sink: {sink_url}")
        
        async with aiohttp.ClientSession() as session:
            tasks = []
            for event in events:
                # Convert CloudEvent to HTTP request
                headers, body = to_structured(event)
                
                # Create task for sending event
                task = asyncio.create_task(
                    session.post(
                        sink_url,
                        headers=headers,
                        data=body,
                        timeout=15  # 15 second timeout for event sending
                    )
                )
                tasks.append(task)
            
            # Wait for all tasks to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Check results
            success_count = 0
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"Failed to send event: {str(result)}")
                elif result.status >= 200 and result.status < 300:
                    success_count += 1
                else:
                    logger.error(f"Failed to send event, status: {result.status}")
            
            logger.info(f"Successfully sent {success_count} out of {len(events)} events")
            return success_count == len(events)

    def process_request(self, request):
        """Process a single EO request"""
        try:
            request_data = json.loads(request) if isinstance(request, str) else request
            
            # Extract request parameters
            bbox = request_data.get('bbox')
            time_range = request_data.get('time_range')
            cloud_cover = request_data.get('cloud_cover', self.config['max_cloud_cover'])
            max_items = request_data.get('max_items', self.config['max_items'])
            request_id = request_data.get('request_id', str(uuid.uuid4()))
            
            if not bbox or not time_range:
                logger.error("Missing required parameters (bbox, time_range)")
                return False
            
            # Search for scenes
            items = self.search_scenes(bbox, time_range, cloud_cover, max_items)
            
            if not items:
                logger.warning("No scenes found matching the search criteria")
                return False
            
            # Process scenes - this will download and store assets
            scene_results = self.process_scenes(items, request_id, bbox)
            
            if not scene_results:
                logger.warning("No scenes were successfully processed")
                return False
            
            # Collect all events to send
            all_events = []
            for result in scene_results:
                all_events.append(result["registration_event"])
                all_events.extend(result["asset_events"])
            
            loop = asyncio.get_event_loop()
            success = loop.run_until_complete(self.send_events(all_events, self.config['sink_url']))
            
            return success
            
        except Exception as e:
            logger.exception(f"Error processing request: {str(e)}")
            return False

    def check_for_requests(self):
        """Check for request objects in the request queue bucket"""
        try:
            objects = self.minio_client.list_objects(self.request_queue)
            for obj in objects:
                try:
                    # Get the request object
                    response = self.minio_client.get_object(self.request_queue, obj.object_name)
                    request_data = json.loads(response.read().decode('utf-8'))
                    response.close()
                    
                    logger.info(f"Processing request: {obj.object_name}")
                    
                    # Process the request
                    success = self.process_request(request_data)
                    
                    # Remove the request object if processed successfully
                    if success:
                        logger.info(f"Successfully processed request {obj.object_name}")
                        self.minio_client.remove_object(self.request_queue, obj.object_name)
                    else:
                        logger.warning(f"Failed to process request {obj.object_name}")
                        
                except Exception as e:
                    logger.error(f"Error processing request {obj.object_name}: {str(e)}")
        except Exception as e:
            logger.error(f"Error checking for requests: {str(e)}")

def main():
    config = get_config()
    
    if not config['sink_url']:
        logger.error("No K_SINK environment variable found. This container must be run as a ContainerSource.")
        sys.exit(1)
    
    minio_manager = MinioClientManager(config)
    minio_client = minio_manager.initialize_client()
    
    ingestion_manager = STACIngestionManager(minio_client, config)
    
    ingestion_manager.ensure_buckets()
    
    stop_event = threading.Event()
    
    def signal_handler(sig, frame):
        logger.info("Shutdown signal received, stopping...")
        stop_event.set()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.info(f"Starting STAC ingestion container, watching for requests in '{config['request_queue']}' bucket")
    
    while not stop_event.is_set():
        try:
            ingestion_manager.check_for_requests()
        except Exception as e:
            logger.error(f"Error in main loop: {str(e)}")
        
        for _ in range(config['watch_interval']):
            if stop_event.is_set():
                break
            time.sleep(1)
    
    logger.info("STAC ingestion container shutting down")

if __name__ == "__main__":
    main()