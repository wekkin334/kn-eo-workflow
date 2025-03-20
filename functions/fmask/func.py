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
import numpy as np
import xarray as xr
import rioxarray as rxr
from scipy import ndimage
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_config():
    return {
        'minio_endpoint': os.getenv('MINIO_ENDPOINT', 'minio.eo-workflow.svc.cluster.local:9000'),
        'minio_access_key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
        'minio_secret_key': os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
        'cog_bucket': os.getenv('COG_BUCKET', 'cog-assets'),
        'fmask_raw_bucket': os.getenv('FMASK_RAW_BUCKET', 'fmask-raw'),
        'max_retries': int(os.getenv('MAX_RETRIES', '3')),
        'connection_timeout': int(os.getenv('CONNECTION_TIMEOUT', '5')),
        'secure_connection': os.getenv('MINIO_SECURE', 'false').lower() == 'true',
        'event_source': os.getenv('EVENT_SOURCE', 'eo-workflow/fmask'),
        'cloud_dilate_size': int(os.getenv('CLOUD_DILATE_SIZE', '3')),
        'shadow_dilate_size': int(os.getenv('SHADOW_DILATE_SIZE', '3')),
    }

def initialize_minio_client(config):
    """Initialize MinIO client with retries"""
    for attempt in range(config['max_retries']):
        try:
            logger.info(f"Initializing MinIO client with endpoint: {config['minio_endpoint']}")
            client = Minio(
                config['minio_endpoint'],
                access_key=config['minio_access_key'],
                secret_key=config['minio_secret_key'],
                secure=config['secure_connection']
            )
            # Test connection
            client.list_buckets()
            logger.info("Successfully connected to MinIO")
            return client
        except Exception as e:
            if attempt == config['max_retries'] - 1:
                logger.error(f"Failed to initialize MinIO client after {config['max_retries']} attempts: {str(e)}")
                raise
            logger.warning(f"Attempt {attempt + 1} failed, retrying: {str(e)}")
            time.sleep(config['connection_timeout'])

def ensure_bucket(minio_client, bucket_name, config):
    """Ensure bucket exists with retries"""
    for attempt in range(config['max_retries']):
        try:
            if not minio_client.bucket_exists(bucket_name):
                logger.info(f"Creating bucket: {bucket_name}")
                minio_client.make_bucket(bucket_name)
                logger.info(f"Successfully created bucket: {bucket_name}")
            return True
        except Exception as e:
            if attempt == config['max_retries'] - 1:
                logger.error(f"Failed to ensure bucket exists after {config['max_retries']} attempts: {str(e)}")
                raise
            logger.warning(f"Attempt {attempt + 1} failed, retrying: {str(e)}")
            time.sleep(config['connection_timeout'])

def download_asset(minio_client, bucket, object_name, config):
    """Download asset from MinIO with retries"""
    for attempt in range(config['max_retries']):
        try:
            logger.info(f"Downloading asset from bucket: {bucket}, object: {object_name}")
            response = minio_client.get_object(bucket, object_name)
            data = response.read()
            response.close()
            return data
        except Exception as e:
            if attempt == config['max_retries'] - 1:
                logger.error(f"Failed to download asset after {config['max_retries']} attempts: {str(e)}")
                raise
            logger.warning(f"Download attempt {attempt + 1} failed: {str(e)}")
            time.sleep(config['connection_timeout'])

def upload_fmask(minio_client, fmask_data, bucket, object_name, metadata, config):
    """Upload FMask to MinIO with retries"""
    for attempt in range(config['max_retries']):
        try:
            ensure_bucket(minio_client, bucket, config)
            
            minio_client.put_object(
                bucket,
                object_name,
                io.BytesIO(fmask_data),
                length=len(fmask_data),
                content_type="image/tiff",
                metadata=metadata
            )
            logger.info(f"Uploaded FMask to bucket: {bucket}, object: {object_name}")
            return True
        except Exception as e:
            if attempt == config['max_retries'] - 1:
                logger.error(f"Failed to upload FMask after {config['max_retries']} attempts: {str(e)}")
                raise
            logger.warning(f"Upload attempt {attempt + 1} failed: {str(e)}")
            time.sleep(config['connection_timeout'])

def load_asset_as_xarray(asset_data, asset_id):
    """Load asset data as xarray DataArray"""
    try:
        with tempfile.NamedTemporaryFile(suffix='.tif') as tmp:
            tmp.write(asset_data)
            tmp.flush()
            
            da = rxr.open_rasterio(tmp.name, masked=True).load()
            da = da.squeeze('band', drop=True)
            
            # Normalize band values for Sentinel-2
            # Most bands have reflectance values in 0-10000 range
            if asset_id.startswith('B'):
                max_val = float(da.max().values)
                if max_val > 100:  # Probably in the 0-10000 range
                    da = da / 10000.0
            
            return da
    except Exception as e:
        logger.error(f"Error loading asset {asset_id} as xarray: {str(e)}")
        raise

def create_cloud_event(event_type, data, source):
    """Create a CloudEvent object"""
    return CloudEvent({
        "specversion": "1.0",
        "type": event_type,
        "source": source,
        "id": f"{event_type}-{int(time.time())}-{uuid.uuid4()}",
        "time": datetime.now(timezone.utc).isoformat(),
        "datacontenttype": "application/json",
    }, data)

# Cloud masking utility functions adapted from your centralized workflow
def compute_ndvi(red, nir):
    """Compute Normalized Difference Vegetation Index"""
    return (nir - red) / (nir + red + 1e-10)

def compute_ndwi(green, nir):
    """Compute Normalized Difference Water Index"""
    return (green - nir) / (green + nir + 1e-10)

def compute_mndwi(green, swir):
    """Compute Modified Normalized Difference Water Index"""
    return (green - swir) / (green + swir + 1e-10)

def compute_cloud_shadow_mask(ds, config):
    """
    Generate cloud and shadow mask from a dataset of bands
    Adapted from the Sentinel2Analyzer.compute_enhanced_land_cover method
    """
    logger.info("Computing cloud and shadow mask")
    
    # Extract bands (normalized to 0-1 range)
    blue = ds['B02'].values
    green = ds['B03'].values
    red = ds['B04'].values
    nir = ds['B08'].values
    
    # For SWIR bands, check if they exist
    if 'B11' in ds and 'B12' in ds:
        swir1 = ds['B11'].values
        swir2 = ds['B12'].values
        has_swir = True
    else:
        swir1 = None
        swir2 = None
        has_swir = False
    
    # Get SCL band if available
    if 'SCL' in ds:
        scl = ds['SCL'].values.astype(np.uint8)
        has_scl = True
    else:
        scl = None
        has_scl = False
    
    # Compute indices
    ndvi = compute_ndvi(red, nir)
    ndwi = compute_ndwi(green, nir)
    
    if has_swir:
        mndwi = compute_mndwi(green, swir1)
    else:
        mndwi = None
    
    # Water detection
    if has_scl:
        water_scl = (scl == 6)
    else:
        water_scl = np.zeros_like(ndvi, dtype=bool)
    
    water_spectral = ndwi > 0.2
    if mndwi is not None:
        water_spectral = water_spectral | (mndwi > 0.2)
    
    water_mask = water_scl | (water_spectral & (nir < 0.15) & (blue > red))
    
    # Cloud detection
    if has_scl:
        cloud_scl = np.isin(scl, [8, 9, 10])
    else:
        cloud_scl = np.zeros_like(ndvi, dtype=bool)
    
    bright_pixels = (blue > 0.2) & (green > 0.2) & (red > 0.2)
    spectral_cloud = (ndvi < 0.1) & bright_pixels & (blue > red)
    
    if has_swir:
        cirrus_test = (swir1 < 0.1) & bright_pixels
        spectral_cloud = spectral_cloud | cirrus_test
    
    cloud_mask = cloud_scl | spectral_cloud
    
    # Dilate clouds to catch edges
    if config['cloud_dilate_size'] > 0:
        cloud_mask = ndimage.binary_opening(cloud_mask, iterations=1)
        cloud_mask = ndimage.binary_dilation(cloud_mask, iterations=config['cloud_dilate_size'])
    
    # Shadow detection
    if has_scl:
        shadow_scl = (scl == 3)
    else:
        shadow_scl = np.zeros_like(ndvi, dtype=bool)
    
    nir_dark = (nir < 0.1)
    if has_swir:
        swir_dark = (swir1 < 0.1)
    else:
        swir_dark = np.zeros_like(ndvi, dtype=bool)
    
    dark_pixels = nir_dark & (red < 0.1) & (blue < 0.1)
    shadow_mask = shadow_scl | (dark_pixels & ~water_mask & ~cloud_mask)
    
    # Dilate shadows
    shadow_mask = ndimage.binary_opening(shadow_mask, iterations=1)
    if config['shadow_dilate_size'] > 0:
        shadow_mask = ndimage.binary_dilation(shadow_mask, iterations=config['shadow_dilate_size'])
    
    # Create final mask
    # 0 = clear, 1 = water, 2 = cloud shadow, 3 = cloud
    fmask = np.zeros_like(ndvi, dtype=np.uint8)
    fmask[water_mask] = 1
    fmask[shadow_mask] = 2
    fmask[cloud_mask] = 3
    
    logger.info(f"Generated FMask with shape {fmask.shape}")
    logger.info(f"Clear pixels: {np.sum(fmask == 0)}")
    logger.info(f"Water pixels: {np.sum(fmask == 1)}")
    logger.info(f"Shadow pixels: {np.sum(fmask == 2)}")
    logger.info(f"Cloud pixels: {np.sum(fmask == 3)}")
    
    return fmask

def save_fmask_to_tiff(fmask, reference_da):
    """Save FMask as GeoTIFF using reference DataArray for geospatial info"""
    try:
        # Create a new DataArray with the same coordinates and CRS as the reference
        mask_da = xr.DataArray(
            fmask,
            dims=reference_da.dims,
            coords=reference_da.coords,
            attrs={'long_name': 'Cloud and shadow mask', 'units': 'class'}
        )
        
        # Set the CRS from the reference DataArray
        if hasattr(reference_da, 'rio'):
            mask_da.rio.write_crs(reference_da.rio.crs, inplace=True)
        
        # Save to a temporary file and read back as bytes
        with tempfile.NamedTemporaryFile(suffix='.tif') as tmp:
            mask_da.rio.to_raster(tmp.name, driver="GTiff")
            tmp.flush()
            
            with open(tmp.name, 'rb') as f:
                return f.read()
    except Exception as e:
        logger.error(f"Error saving FMask to TIFF: {str(e)}")
        raise

def main(context: Context):
    try:
        logger.info("FMask function activated")
        config = get_config()
        
        if not hasattr(context, 'cloud_event'):
            error_msg = "No cloud event in context"
            logger.error(error_msg)
            return {"error": error_msg}, 400
        
        event_data = context.cloud_event.data
        if isinstance(event_data, str):
            event_data = json.loads(event_data)
        
        # Extract scene details
        request_id = event_data.get("request_id")
        item_id = event_data.get("item_id")
        collection = event_data.get("collection")
        cog_bucket = event_data.get("cog_bucket")
        assets = event_data.get("assets", [])
        
        logger.info(f"Processing FMask for scene {item_id}, collection {collection}")
        logger.info(f"Available assets: {assets}")
        
        required_bands = ['B02', 'B03', 'B04', 'B08']
        
        missing_bands = [band for band in required_bands if band not in assets]
        if missing_bands:
            error_msg = f"Missing required bands: {missing_bands}"
            logger.error(error_msg)
            error_data = {
                "error": error_msg,
                "error_type": "MissingBandsError",
                "request_id": request_id,
                "item_id": item_id
            }
            return create_cloud_event("eo.processing.error", error_data, config['event_source'])
        
        minio_client = initialize_minio_client(config)
        
        data_vars = {}
        for asset_id in assets:
            try:
                object_name = f"{collection}/{item_id}/{asset_id}"
                
                asset_data = download_asset(minio_client, cog_bucket, object_name, config)
                
                data_vars[asset_id] = load_asset_as_xarray(asset_data, asset_id)
                logger.info(f"Loaded asset {asset_id} with shape {data_vars[asset_id].shape}")
            except Exception as e:
                logger.error(f"Error processing asset {asset_id}: {str(e)}")
        
        ds = xr.Dataset(data_vars)
        fmask = compute_cloud_shadow_mask(ds, config)
        
        reference_da = data_vars[required_bands[0]]
        fmask_tiff = save_fmask_to_tiff(fmask, reference_da)
        fmask_object = f"{collection}/{item_id}/fmask"
        
        metadata = {
            "item_id": item_id,
            "collection": collection,
            "request_id": request_id,
            "content_type": "image/tiff",
            "timestamp": str(int(time.time())),
            "classes": "0=clear,1=water,2=shadow,3=cloud"
        }
        
        upload_fmask(minio_client, fmask_tiff, config['fmask_raw_bucket'], fmask_object, metadata, config)
        
        result = {
            "request_id": request_id,
            "item_id": item_id,
            "collection": collection,
            "bucket": config['fmask_raw_bucket'],
            "object_name": fmask_object,
            "asset_id": "fmask",
            "timestamp": int(time.time())
        }
        
        response_event = create_cloud_event(
            "eo.fmask.completed",
            result,
            config['event_source']
        )
        
        logger.info(f"FMask processing completed for scene {item_id}")
        return response_event
        
    except Exception as e:
        logger.exception(f"Error in FMask function: {str(e)}")
        error_data = {
            "error": str(e),
            "error_type": type(e).__name__,
            "request_id": event_data.get("request_id") if "event_data" in locals() else str(uuid.uuid4()),
            "item_id": event_data.get("item_id") if "event_data" in locals() else "unknown"
        }
        
        error_event = create_cloud_event(
            "eo.processing.error",
            error_data,
            config['event_source']
        )
        
        return error_event