import logging
import os
from pathlib import Path
from typing import List, Union

import numpy as np
import rasterio as rio

from mapa import conf
from mapa.algorithm import ModelSize, compute_all_triangles, reduce_resolution
from mapa.caching import get_hash_of_geojson, tiff_for_bbox_is_cached
from mapa.raster import (
    clip_tiff_to_bbox,
    cut_array_to_square,
    #determine_elevation_scale,
    merge_tiffs,
    remove_empty_first_and_last_rows_and_cols,
    tiff_to_array,
)
from mapa.stac import fetch_stac_items_for_bbox
from mapa.stl_file import save_to_stl_file
from mapa.tiling import get_x_y_from_tiles_format, split_array_into_tiles
from mapa.utils import TMPDIR, ProgressBar, path_to_clipped_tiff
from mapa.verification import verify_input_and_output_are_valid
from mapa.zip import create_zip_archive

log = logging.getLogger(__name__)
logging.basicConfig()
log.setLevel(os.getenv("MAPA_LOG_LEVEL", "INFO"))


def convert_array_to_stl(
    array: np.ndarray,
    as_ascii: bool,
    desired_size: ModelSize,
    max_res: bool,
    z_offset: Union[None, float],
    z_scale: float,
    elevation_scale: float,
    output_file: Path,
) -> Path:
    x, y = array.shape
    # when merging tiffs, sometimes an empty row/col is added, which should be dropped (in case the array size suffices)
    if x > 1 and y > 1:
        array = remove_empty_first_and_last_rows_and_cols(array)

    if max_res:
        if x * y > conf.PERFORMANCE_WARNING_THRESHOLD:
            log.warning(
                "⛔️  Warning: Using max_res=True on the given bounding box might consume a lot of time and memory. "
                "Consider setting max_res=False."
            )
    else:
        bin_fac = round((x / conf.MAXIMUM_RESOLUTION + y / conf.MAXIMUM_RESOLUTION) / 2)
        if bin_fac > 1:
            log.debug("🔍  reducing image resolution...")
            array = reduce_resolution(array, bin_factor=bin_fac)

    triangles = compute_all_triangles(array, desired_size, z_offset, z_scale, elevation_scale)
    log.debug("💾  saving data to stl file...")

    output_file = save_to_stl_file(triangles, output_file, as_ascii)
    log.info(f"🎉  successfully generated STL file: {Path(output_file).absolute()}")
    return Path(output_file)


def _get_desired_size(array: np.ndarray, x: float, y: float, ensure_squared: bool) -> ModelSize:
    if ensure_squared:
        return ModelSize(x=x, y=y)
    else:
        rows, cols = array.shape
        return ModelSize(x=x, y=y / rows * cols)



def convert_bbox_to_tif(
    user_defined_collection:str,
    user_defined_bands:list,
    bbox_geometry: dict,
    #as_ascii: bool = False,
    #model_size: int = 200,
    output_file: str = "output",
    #max_res: bool = False,
    #z_offset: Union[None, float] = 0.0,
    #z_scale: float = 1.0,
    #ensure_squared: bool = False,
    split_area_in_tiles: str = "1x1",
    compress: bool = True,
    allow_caching: bool = True,
    cache_dir: Union[Path, str] = TMPDIR(),
    progress_bar: Union[None, object] = None,
) -> Union[Path, List[Path]]:
    """
    Takes a GeoJSON containing a bounding box as input, fetches the required STAC GeoTIFFs for the
    given bounding box and creates a STL file with elevation data from the GeoTIFFs.

    Parameters
    ----------
    bbox_geometry : dict
        GeoJSON containing the coordinates of the bounding box, selected on the ipyleaflet widget. Usually the
        value of `drawer.last_draw["geometry"]` is used for this.
    output_file : str, optional
        Name and path to output file. File ending should not be provided. Mapa will add .zip or .stl depending
        on the settings. By default "output"
    max_res : bool, optional
        Whether maximum resolution should be used. Note, that this flag potentially increases compute time
        and memory consumption dramatically. The default behavior (i.e. max_res=False) should return 3d models
        with sufficient resolution, while the output stl file should be < ~300 MB. By default False
    split_area_in_tiles : str, optional
        Split the selected bounding box into tiles with this option. The allowed format of a given string is
        "nxm" e.g. "1x1", "2x3", "4x4" or similar, where "1x1" would not split at all and result in only
        one stl file. If an allowed tile format is specified, `nxm` stl files will be computed. By default "1x1"
    compress : bool, optional
        If enabled, the output stl file(s) will be compressed to a zip file. Compressing is recommended as it
        reduces the data volume of typical stl files by a factor of ~4.
    allow_caching : bool, optional
        Whether caching previous downloaded GeoTIFF files should be enabled/disabled. By default True
    cache_dir: Union[Path, str]
        Path to a directory which should be used as local cache. This is helpful when intermediary tiff files
        should be persisted even after the temp directory gets cleaned-up by e.g. a restart. By default TMPDIR
    progress_bar : Union[None, object], optional
        A streamlit progress bar object can be used to indicate the progress of downloading the STAC items. By
        default None

    Returns
    -------
    Union[Path, List[Path]]
        Path or list of paths to the resulting output file(s) on your local machine.
    """

    # fail early in case of missing requirements
    if bbox_geometry is None:
        raise ValueError("⛔️  ERROR: make sure to draw a rectangle on the map first!")

    # evaluate tile format to fail early in case of invalid input value
    tiles = get_x_y_from_tiles_format(split_area_in_tiles)

    args = locals().copy()
    args.pop("progress_bar", None)
    log.info(f"⏳  converting bounding box to STL file with arguments: {args}")

    if progress_bar:
        steps = tiles.x * tiles.y * 2 if compress else tiles.x * tiles.y
        progress_bar = ProgressBar(progress_bar=progress_bar, steps=steps)

    list_paths_to_tiffs=fetch_stac_items_for_bbox(user_defined_bands,
    user_defined_collection,
    bbox_geometry,
    allow_caching,
    cache_dir,
    progress_bar)    
    print("######################",list_paths_to_tiffs)


    if progress_bar:
        progress_bar.step()
    if compress:
        return create_zip_archive(files=list_paths_to_tiffs, output_file=f"{output_file}.zip", progress_bar=progress_bar)
    else:
        return list_paths_to_tiffs[0] if len(list_paths_to_tiffs) == 1 else list_paths_to_tiffs
