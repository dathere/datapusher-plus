# -*- coding: utf-8 -*-
# flake8: noqa: E501

import os
import shutil
import uuid
import zipfile
import fiona
import pandas as pd
from shapely.geometry import shape, Polygon, MultiPolygon
from shapely.wkt import dumps
from pathlib import Path
import logging
from typing import Optional, Union, Tuple
import shapely
import numpy as np
from shapely.ops import transform
import pyproj

from ckanext.datapusher_plus.logging_utils import TRACE

# Create logger at module level as fallback
logger = logging.getLogger(__name__)


def simplify_polygon(
    geom: Union[Polygon, MultiPolygon],
    relative_tolerance: float,
    log: logging.Logger,
    to_meter_proj: Optional[callable] = None,
) -> Union[Polygon, MultiPolygon]:
    """Helper function to simplify polygon geometries while preserving topology.

    Args:
        geom: The geometry to simplify
        relative_tolerance: Simplification tolerance as a percentage (0-1) of the geometry's diagonal length
        log: Logger instance
        to_meter_proj: Optional transform function to convert coordinates to meters
    """
    if isinstance(geom, MultiPolygon):
        log.trace("Processing MultiPolygon with {} parts".format(len(geom.geoms)))
        # Handle each polygon in the multipolygon separately
        simplified_polys = []
        for poly in geom.geoms:
            simplified_poly = simplify_polygon(
                poly,
                relative_tolerance,
                log,
                to_meter_proj,
            )
            if simplified_poly and not simplified_poly.is_empty:
                simplified_polys.append(simplified_poly)
        if simplified_polys:
            return MultiPolygon(simplified_polys)
        return geom

    try:
        # Log initial geometry info

        # Transform to meters if projection is provided
        if to_meter_proj:
            try:
                geom = transform(to_meter_proj, geom)
            except Exception as e:
                log.debug(f"  Transform to meters failed: {str(e)}")
                return geom

        # Get the bounds to understand the scale
        try:
            minx, miny, maxx, maxy = geom.bounds
            diagonal = ((maxx - minx) ** 2 + (maxy - miny) ** 2) ** 0.5
            abs_tolerance = float(diagonal) * float(relative_tolerance)

            log.trace(
                "  Geometry bounds: minx={}, miny={}, maxx={}, maxy={}".format(
                    minx, miny, maxx, maxy
                )
            )
            log.trace("  Geometry diagonal length: {:.2f}".format(float(diagonal)))
            log.trace(
                "  Relative tolerance: {:.4f}% of diagonal".format(
                    float(relative_tolerance) * 100
                )
            )
            log.trace(
                "  Absolute tolerance: {:.2f} meters".format(float(abs_tolerance))
            )
        except Exception as e:
            log.trace(f"  Error calculating bounds/tolerance: {str(e)}")
            return geom

        # For single polygons, handle exterior and interior rings separately
        try:
            # Get exterior coordinates and ensure they're float
            exterior_coords = []
            for i, (x, y) in enumerate(geom.exterior.coords):
                try:
                    fx, fy = float(x), float(y)
                    exterior_coords.append((fx, fy))
                except (ValueError, TypeError) as e:
                    log.debug(
                        f"  Error converting exterior coordinate {i}: (x={x}, y={y}), Error: {str(e)}"
                    )
                    return geom

            exterior_coords = np.array(exterior_coords, dtype=np.float64)
            simplified_exterior = shapely.LineString(exterior_coords).simplify(
                abs_tolerance, preserve_topology=True
            )
        except Exception as e:
            log.debug(f"  Error processing exterior ring: {str(e)}")
            return geom

        # Only proceed if the simplified exterior is valid
        if simplified_exterior.is_valid and not simplified_exterior.is_empty:
            # Handle interior rings (holes)
            simplified_interiors = []
            for ring_idx, interior in enumerate(geom.interiors):
                try:
                    # Get interior coordinates and ensure they're float
                    interior_coords = []
                    for i, (x, y) in enumerate(interior.coords):
                        try:
                            fx, fy = float(x), float(y)
                            interior_coords.append((fx, fy))
                        except (ValueError, TypeError) as e:
                            log.debug(
                                f"    Error converting interior {ring_idx} coordinate {i}: (x={x}, y={y}), Error: {str(e)}"
                            )
                            continue

                    if interior_coords:
                        interior_coords = np.array(interior_coords, dtype=np.float64)
                        simplified_interior = shapely.LineString(
                            interior_coords
                        ).simplify(abs_tolerance, preserve_topology=True)
                        if (
                            simplified_interior.is_valid
                            and not simplified_interior.is_empty
                        ):
                            simplified_interiors.append(simplified_interior)
                except Exception as e:
                    log.debug(f"  Error processing interior ring {ring_idx}: {str(e)}")
                    continue

            # Create new polygon with simplified exterior and interiors
            try:
                new_poly = Polygon(
                    simplified_exterior,
                    holes=[interior for interior in simplified_interiors],
                )
                if new_poly.is_valid:
                    # Transform back if we transformed to meters
                    if to_meter_proj:
                        try:
                            new_poly = transform(
                                lambda x, y: (x, y), new_poly
                            )  # Transform back to original CRS
                        except Exception as e:
                            log.error(f"  Transform back from meters failed: {str(e)}")
                            return geom
                    return new_poly
                else:
                    log.warning(
                        "Created polygon is invalid - the simplified geometry failed validation. Returning original geometry."
                    )
                    return geom
            except Exception as e:
                log.error(f"  Failed to create simplified polygon: {str(e)}")
    except Exception as e:
        log.error(f"  Simplification error: {str(e)}")

    # If anything fails, return original geometry
    return geom


def process_spatial_file(
    input_path: Union[str, Path],
    resource_format: str,
    output_csv_path: Optional[Union[str, Path]] = None,
    tolerance: float = 0.001,  # Now represents a relative tolerance (0.1%)
    task_logger: Optional[logging.Logger] = None,
) -> Tuple[bool, Optional[str]]:
    """
    Simplifies and converts a spatial file (e.g., zipped Shapefile, GeoJSON) to CSV format.

    This function reads a spatial file, simplifies its geometries using a relative tolerance,
    and exports the attributes and simplified geometries to a CSV file. If the input is a zipped
    Shapefile, it will be unzipped and the .shp file will be processed. The function supports
    logging via an optional logger.

    Args:
        input_path (Union[str, Path]): Path to the input spatial file. Can be a zipped Shapefile,
            a .shp, or a GeoJSON file.
        resource_format (str): The format of the spatial file (e.g., "SHP", "QGIS", "GEOJSON").
        output_csv_path (Optional[Union[str, Path]], optional): Path to the output CSV file.
            If not provided, defaults to the input path with a .csv extension.
        tolerance (float, optional): Relative simplification tolerance as a fraction of the
            geometry's diagonal (e.g., 0.001 means 0.1% of the geometry size). Default is 0.001.
        task_logger (Optional[logging.Logger], optional): Logger to use for logging progress and
            errors. If not provided, a module-level logger is used.

    Returns:
        Tuple[bool, Optional[str]]:
            - success (bool): True if the conversion was successful, False otherwise.
            - error_message (Optional[str]): Error message if failed, or None if successful.

    Notes:
        - The function will attempt to transform coordinates to meters for simplification if
          the source CRS is geographic (e.g., WGS84).
        - If the input is a zipped Shapefile, only the first .shp file found will be processed.
        - The output CSV will contain all attribute columns plus a "geometry" column with WKT.
        - If any features cannot be processed, they will be skipped and a warning will be logged.
    """
    # Use the provided logger or fall back to the module logger
    log = task_logger if task_logger is not None else logger

    zip_temp_dir = None
    try:
        input_path = Path(input_path)
        if not input_path.exists():
            return False, f"Input file does not exist: {input_path}"

        if not output_csv_path:
            output_csv_path = input_path.with_suffix(".csv")
        else:
            output_csv_path = Path(output_csv_path)

        log.debug(f"Processing spatial file: {input_path}")
        # Step 0: Check if the input file is a Zipped Shapefile
        # If it is, we need to unzip it and process the .shp file
        if resource_format.upper() == "SHP" or resource_format.upper() == "QGIS":
            # Create a temporary directory for the unzipped files
            zip_temp_dir = input_path.parent / f"temp_{uuid.uuid4()}"
            zip_temp_dir.mkdir(exist_ok=True)

            # Unzip the file
            with zipfile.ZipFile(input_path, "r") as zip_ref:
                zip_ref.extractall(zip_temp_dir)

            # After extracting, find the .shp file in the temp dir
            shp_files = [
                f for f in os.listdir(zip_temp_dir) if f.lower().endswith(".shp")
            ]

            num_shp_files = len(shp_files)
            if num_shp_files > 1:
                shapefile_path = zip_temp_dir / shp_files[0]
                input_path = shapefile_path
                log.warning(
                    f"Found {num_shp_files} .shp files in the zipped Shapefile. Using FIRST unzipped shapefile: {shapefile_path}"
                )
            elif num_shp_files == 1:
                shapefile_path = zip_temp_dir / shp_files[0]
                input_path = shapefile_path
                log.debug(f"Using unzipped shapefile: {shapefile_path}")
            else:
                return False, "No .shp file found in the zipped Shapefile"

        # Step 1: Read spatial features using Fiona
        log.debug(f"Reading spatial features from {input_path}")
        with fiona.open(input_path) as src:
            features = list(src)
            # Get CRS information
            src_crs = src.crs
            log.info(f"Source CRS: {src_crs}")

            # Log schema information
            log.info(f"Feature schema: {src.schema}")

            # Setup transformation to meters if needed
            to_meter_proj = None
            if src_crs:
                try:
                    src_proj = pyproj.CRS(src_crs)
                    log.info(f"Source projection: {src_proj.to_string()}")
                    if not src_proj.is_projected:
                        bounds = src.bounds
                        log.info(f"Source bounds: {bounds}")
                        utm_zone = int((float(bounds[0]) + 180) / 6) + 1
                        utm_crs = pyproj.CRS(f"+proj=utm +zone={utm_zone} +datum=WGS84")
                        project = pyproj.Transformer.from_crs(
                            src_proj, utm_crs, always_xy=True
                        ).transform
                        to_meter_proj = project
                        log.info(
                            f"Setting up transformation to UTM zone {utm_zone} for meter-based simplification"
                        )
                        log.info(f"Target UTM projection: {utm_crs.to_string()}")
                except Exception as e:
                    log.warning(f"Could not setup coordinate transformation: {str(e)}")

        if not features:
            return False, "No features found in the input file"

        log.info(f"Found {len(features)} features")
        log.info(
            "Using relative tolerance of {:.4f}% of geometry size".format(
                float(tolerance) * 100
            )
        )

        # Step 2: Parse and simplify geometries
        simplified_geoms = []
        valid_attributes = []
        error_count = 0
        total_reduction = 0

        for i, feat in enumerate(features):
            try:
                # Create geometry and simplify
                log.trace(f"Feature {i} simplification:")
                log.trace(f"  Raw geometry: {feat['geometry']}")

                # Convert GeoJSON geometry to Shapely geometry
                try:
                    original_geom = shape(feat["geometry"])
                    log.trace(
                        f"  Geometry type: {original_geom.geom_type}  Is valid: {original_geom.is_valid}  Is empty: {original_geom.is_empty}"
                    )
                except Exception as e:
                    log.error(
                        f"Could not create Shapely geometry for feature {i}: {str(e)}"
                    )
                    continue

                # Convert to WKT for vertex counting
                try:
                    original_wkt = dumps(original_geom)
                    vertex_count = len(original_wkt.split(","))
                    log.trace(f"  Original WKT (first 50 chars): {original_wkt[:50]}")
                    log.trace(f"  Original vertices: {vertex_count}")
                except Exception as e:
                    log.warning(
                        f"Could not convert geometry to WKT for feature {i}: {str(e)}"
                    )
                    continue

                # Handle polygon geometries specially
                if isinstance(original_geom, (Polygon, MultiPolygon)):
                    simplified = simplify_polygon(
                        original_geom, tolerance, log, to_meter_proj
                    )
                else:
                    # For non-polygon geometries, try direct simplification
                    try:
                        if to_meter_proj:
                            original_geom = transform(to_meter_proj, original_geom)
                            log.trace("  Transformed to meters")

                        # Calculate absolute tolerance based on geometry size
                        minx, miny, maxx, maxy = original_geom.bounds
                        diagonal = ((maxx - minx) ** 2 + (maxy - miny) ** 2) ** 0.5
                        abs_tolerance = float(diagonal) * float(tolerance)

                        log.trace(
                            "  Geometry bounds: minx={}, miny={}, maxx={}, maxy={}".format(
                                minx, miny, maxx, maxy
                            )
                        )
                        log.trace(
                            "  Geometry diagonal length: {:.2f}".format(float(diagonal))
                        )
                        log.trace(
                            "  Absolute tolerance: {:.2f} meters".format(
                                float(abs_tolerance)
                            )
                        )

                        # Get coordinates and ensure they're float
                        coords = []
                        for i, (x, y) in enumerate(original_geom.coords):
                            try:
                                fx, fy = float(x), float(y)
                                coords.append((fx, fy))
                            except (ValueError, TypeError) as e:
                                log.warning(
                                    f"    Error converting coordinate {i}: (x={x}, y={y}), Error: {str(e)}"
                                )
                                continue

                        if coords:
                            log.trace(f"  Processed {len(coords)} coordinates")
                            coords = np.array(coords, dtype=np.float64)
                            original_geom = type(original_geom)(coords)
                            simplified = original_geom.simplify(
                                abs_tolerance, preserve_topology=True
                            )
                            if to_meter_proj:
                                simplified = transform(lambda x, y: (x, y), simplified)
                                log.trace("  Transformed back from meters")
                        else:
                            log.trace(
                                "  No valid coordinates found, using original geometry"
                            )
                            simplified = original_geom
                    except Exception as e:
                        log.warning(f"  Simplification failed: {str(e)}")
                        simplified = original_geom

                # Convert simplified geometry to WKT
                try:
                    simplified_wkt = dumps(simplified)
                    simplified_vertex_count = len(simplified_wkt.split(","))
                    log.trace(f"  Simplified vertices: {simplified_vertex_count}")

                    if vertex_count > 0:  # Avoid division by zero
                        reduction = (1 - simplified_vertex_count / vertex_count) * 100
                        total_reduction += reduction
                        log.trace("  Reduction: {:.1f}%".format(float(reduction)))

                    simplified_geoms.append(simplified_wkt)
                    valid_attributes.append(feat["properties"])
                except Exception as e:
                    error_count += 1
                    log.warning(
                        f"Error converting simplified geometry to WKT for feature {i}: {str(e)}"
                    )
                    # On error, store original unsimplified geometry
                    try:
                        log.warning("Storing original unsimplified geometry")
                        simplified_geoms.append(original_wkt)
                        valid_attributes.append(feat["properties"])
                    except Exception as e2:
                        log.warning(f"Could not store original geometry: {str(e2)}")

            except Exception as e:
                error_count += 1
                log.warning(f"Error processing feature {i}: {str(e)}")
                continue

        if error_count > 0:
            log.warning(
                f"Failed to process {error_count} out of {len(features)} features"
            )

        if not simplified_geoms:
            return False, "No features could be processed"

        avg_reduction = (
            total_reduction / len(simplified_geoms) if simplified_geoms else 0
        )
        log.info(
            "Average vertex reduction across all features: {:.1f}%".format(
                float(avg_reduction)
            )
        )

        # Step 3: Create DataFrame
        df = pd.DataFrame(valid_attributes)
        df["geometry"] = simplified_geoms

        # Step 4: Write to CSV
        df.to_csv(output_csv_path, index=False)

        return True, None

    except Exception as e:
        return False, f"Error converting spatial file to CSV: {str(e)}"

    finally:
        if zip_temp_dir:
            shutil.rmtree(zip_temp_dir)
