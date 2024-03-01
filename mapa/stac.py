import logging
import warnings
from pydantic import PydanticDeprecatedSince20
from pathlib import Path
from typing import List, Union
from urllib import request
from odc.stac import stac_load
import rasterio as rio
import pandas as pd
import numpy as np
import rioxarray
from pathlib import Path


import geojson
from pystac.item import Item
from pystac_client import Client

from mapa import conf
from mapa.exceptions import NoSTACItemFound
from mapa.utils import ProgressBar
import pystac_client
with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=PydanticDeprecatedSince20)
    import planetary_computer

from mapa_streamlit.io import are_stac_items_planetary_computer

log = logging.getLogger(__name__)

def _bbox(coord_list):
    box = []
    for i in (0, 1):
        res = sorted(coord_list, key=lambda x: x[i])
        box.append((res[0][i], res[-1][i]))
    return [box[0][0], box[1][0], box[0][1], box[1][1]]


def _turn_geojson_into_bbox(geojson_bbox: dict) -> List[float]:
    coordinates = geojson_bbox["coordinates"]
    return _bbox(list(geojson.utils.coords(geojson.Polygon(coordinates))))


def save_images_from_xarr(xarray, filepath, bands:str, collection:str, datatype="float32"):
   
    count=len([bands])
    xarray.rio.set_crs(int(xarray.spatial_ref.values))
    tf = xarray.rio.transform()
    crs = xarray.rio.crs
    width, height = len(xarray.x), len(xarray.y)
    meta = {
        "transform": tf,
        "crs": crs,
        "width": width,
        "height": height,
        "count": count,
        "dtype": datatype,
        "nodata": 0,
        
    }
    
    xx=xarray.squeeze()
    print(type(bands))
  
    print(len([bands]))

    if count == 1 :
        #bands_str=', '.join(map(str, bands))
        paths=save_tifs_with_one_band(filepath, bands, collection, meta, xx)
 
    print("------------",paths)            
    return paths

def save_tifs_with_one_band(filepath, bands, collection, meta, xx):
    paths=[]

    for arr in xx[bands]:
        print("~~~~~~~~~~~",arr.time.values)
        filename=Path(collection+"_"
                + pd.to_datetime(arr.time.values)
                .to_pydatetime()
                .strftime("%Y-%m-%d_%H-%M-%S")
                + ".tif")
        with rio.open(
                filepath/filename,
                "w",
                **meta,
                bands= bands, 
                collection= collection,
                driver="COG",
            ) as dst:
            dst.write(arr.values, 1)
            dst.set_band_description(1,bands)
                
            paths.append(filepath/filename)
    return paths
                        

def fetch_stac_items_for_bbox(
    user_defined_bands:list, user_defined_collection:str, geojson: dict, allow_caching: bool, cache_dir: Path, progress_bar: Union[None, ProgressBar] = None
) -> List[Path]:
    
    bbox = _turn_geojson_into_bbox(geojson)
    
    catalog = pystac_client.Client.open(
        conf.PLANETARY_COMPUTER_API_URL,
        modifier=planetary_computer.sign_inplace,
    )

    search = catalog.search(
        collections=[user_defined_collection],  # landsat-c2-l2, sentinel-2-l2a
        bbox=bbox,
        datetime="2023-10-20/2023-10-28",
        query={
            "eo:cloud_cover": {"lt": 20},
        },
    )
    items = search.get_all_items()

    patch_url = None
    if are_stac_items_planetary_computer(items):
        patch_url = planetary_computer.sign

        xx=stac_load(
            items,
            geopolygon=geojson,
            chunks={},  # <-- use Dask
            groupby= "solar_day",
            patch_url=patch_url,
            resampling="bilinear",
            fail_on_error=False,
            no_data=0
        )
    
   
    n = len(items)
    if progress_bar:
        progress_bar.steps += n
    if n > 0:
        log.info(f"⬇️  fetching {n} stac items...")
        
        files=save_images_from_xarr(xx,cache_dir,user_defined_bands,user_defined_collection)
        #for cnt, item in enumerate(items):
            #files.append(_get_tiff_file(item, allow_caching, cache_dir, cnt + 1, n))
        if progress_bar:
            progress_bar.step()
        print("######",files)
        return files
    else:
        raise NoSTACItemFound("Could not find the desired STAC item for the given bounding box.")
