# encoding: utf-8
# flake8: noqa: E501

from __future__ import annotations

import logging
from jinja2 import DictLoader, Environment

log = logging.getLogger(__name__)


class FormulaProcessor:
    def __init__(
        self,
        scheming_yaml,
        package,
        resource,
        resource_fields_stats,
        resource_fields_freqs,
        logger,
    ):
        self.scheming_yaml = scheming_yaml
        self.package = package
        self.resource = resource
        self.resource_fields_stats = resource_fields_stats
        self.resource_fields_freqs = resource_fields_freqs
        self.logger = logger

    def process_formulae(
        self, entity_type: str, fields_key: str, formula_type: str = "formula"
    ):
        """
        Generic formula processor for both package and resource fields

        Args:
            entity_type: 'package' or 'resource'
            fields_key: Key in scheming_yaml for fields ('dataset_fields' or 'resource_fields')
            formula_type: Type of formula ('formula' or 'suggestion_formula')
        """
        formula_fields = [
            field for field in self.scheming_yaml[fields_key] if field.get(formula_type)
        ]

        if not formula_fields:
            return

        self.logger.info(
            f"Found {len(formula_fields)} {entity_type.upper()} field/s with {formula_type} in the scheming_yaml"
        )

        jinja2_formulae = {}
        for schema_field in formula_fields:
            field_name = schema_field["field_name"]
            template = schema_field[formula_type]
            jinja2_formulae[field_name] = template

            self.logger.debug(
                f'Jinja2 {formula_type} for {entity_type.upper()} field "{field_name}": {template}'
            )

        context = {
            "package": self.package,
            "resource": self.resource,
            "dpps": self.resource_fields_stats,
            "dppf": self.resource_fields_freqs,
        }
        self.logger.trace(f"Context: {context}")
        jinja2_env = create_jinja2_env(jinja2_formulae)

        updates = {}
        for schema_field in formula_fields:
            field_name = schema_field["field_name"]
            try:
                formula = jinja2_env.get_template(field_name)
                rendered_formula = formula.render(**context)
                updates[field_name] = rendered_formula

                self.logger.debug(
                    f'Evaluated jinja2 {formula_type} for {entity_type.upper()} field "{field_name}": {rendered_formula}'
                )
            except Exception as e:
                self.logger.error(
                    f'Error evaluating jinja2 {formula_type} for {entity_type.upper()} field "{field_name}": {str(e)}'
                )

        return updates


def create_jinja2_env(context):
    """Create a configured Jinja2 environment with all filters and globals."""
    env = Environment(loader=DictLoader(context))

    # Add filters
    filters = {
        "truncate_with_ellipsis": truncate_with_ellipsis,
        "format_number": format_number,
        "format_bytes": format_bytes,
        "format_date": format_date,
        "calculate_percentage": calculate_percentage,
        "get_unique_ratio": get_unique_ratio,
        "format_range": format_range,
        "format_coordinates": format_coordinates,
        "calculate_bbox_area": calculate_bbox_area,
    }
    env.filters.update(filters)

    # Add globals
    globals = {
        "spatial_extent_wkt": spatial_extent_wkt,
        "spatial_extent_feature_collection": spatial_extent_feature_collection,
        "get_frequency_top_values": get_frequency_top_values,
    }
    env.globals.update(globals)

    return env


# ------------------
# Jinja2 filters and functions
# IMPORTANT: Be sure to add the function to the filters dict in create_jinja2_env
def truncate_with_ellipsis(text, length=50, ellipsis="..."):
    """Truncate text to a specific length and append ellipsis."""
    if not text or len(text) <= length:
        return text
    return text[:length] + ellipsis


def format_number(value, decimals=2):
    """Format numbers with thousands separator and decimal places

    Example:
    {{ stats.population.sum | format_number }} -> 1,234,567.89
    """
    return f"{float(value):,.{decimals}f}"


def format_bytes(bytes):
    """Format byte sizes into human readable format

    Example:
    {{ stats.file_size.max | format_bytes }} -> 1.5 GB
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes < 1024:
            return f"{bytes:.1f} {unit}"
        bytes /= 1024


def format_date(value, format="%Y-%m-%d"):
    """Format dates in specified format

    Example:
    {{ stats.created_date.max | format_date("%B %d, %Y") }} -> January 1, 2024
    """
    return value.strftime(format)


def calculate_percentage(part, whole):
    """Calculate percentage

    Example:
    {{ calculate_percentage(stats.nullcount, stats.total_rows) }} -> 12.5
    """
    return (part / whole) * 100 if whole else 0


def get_unique_ratio(field):
    """Get ratio of unique values

    Example:
    {{ get_unique_ratio(stats.user_id) }} -> 0.95
    """
    return field.cardinality / field.total_rows if field.total_rows else 0


def format_range(min_val, max_val, separator=" to "):
    """Format a range of values

    Example:
    {{ format_range(stats.temperature.min, stats.temperature.max) }} -> "-10 to 35"
    """
    return f"{min_val}{separator}{max_val}"


def format_coordinates(lat, lon, precision=6):
    """Format coordinates nicely

    Example:
    {{ format_coordinates(stats.latitude.mean, stats.longitude.mean) }}
    -> "40.7128°N, 74.0060°W"
    """
    lat_dir = "N" if lat >= 0 else "S"
    lon_dir = "E" if lon >= 0 else "W"
    return f"{abs(lat):.{precision}f}°{lat_dir}, {abs(lon):.{precision}f}°{lon_dir}"


def calculate_bbox_area(min_lon, min_lat, max_lon, max_lat):
    """Calculate approximate area of bounding box in square kilometers

    Example:
    {{ calculate_bbox_area(bbox.min_lon, bbox.min_lat, bbox.max_lon, bbox.max_lat) }}
    -> 1234.56
    """
    from math import radians, cos

    earth_radius = 6371  # km
    width = abs(max_lon - min_lon) * cos(radians((min_lat + max_lat) / 2))
    height = abs(max_lat - min_lat)
    return width * height * (earth_radius**2)


# ------------------
# Jinja2 Functions
# IMPORTANT: Be sure to add the function to the globals dict in create_jinja2_env
def spatial_extent_wkt(
    min_lon: float, min_lat: float, max_lon: float, max_lat: float
) -> str:
    """Convert min/max WGS84 coordinates to WKT polygon format.

    Args:
        min_lon: Minimum longitude coordinate
        min_lat: Minimum latitude coordinate
        max_lon: Maximum longitude coordinate
        max_lat: Maximum latitude coordinate

    Returns:
        str: WKT polygon string representing the spatial extent

    Example:
        >>> spatial_extent_wkt(-180, -90, 180, 90)
        'POLYGON((-180 -90, -180 90, 180 90, 180 -90, -180 -90))'
    """
    # Create WKT polygon string from coordinates
    wkt = f"SRID=4326;POLYGON(({min_lon} {min_lat}, {min_lon} {max_lat}, {max_lon} {max_lat}, {max_lon} {min_lat}, {min_lon} {min_lat}))"
    return wkt


def spatial_extent_feature_collection(
    name: str, bbox: list[float], type: str = "calculated"
) -> str:
    """Convert a bounding box to a namedGeoJSON feature collection.

    Args:
        name: Name of the feature
        bbox: List of floats representing the bounding box [min_lon, min_lat, max_lon, max_lat]
        type: Type of the feature, defaults to "calculated"

    Returns:
        str: GeoJSON feature collection string

    Example:
        >>> spatial_extent_feature_collection("User Drawn Polygon 1", "draw", [-180, -90, 180, 90])
        '{"type": "FeatureCollection", "features": [{"type": "Feature", "properties":{"name":"User Drawn Polygon 1","type":"draw"}, "geometry": {"type": "Polygon", "coordinates": [[[-180, -90], [-180, 90], [180, 90], [180, -90], [-180, -90]]]}, "properties": {}}]}
    """
    return f'{{"type": "FeatureCollection", "features": [{{"type": "Feature", "properties": {{"name": "{name}", "type": "{type}"}}, "geometry": {{"type": "Polygon", "coordinates": [[{bbox[0]} {bbox[1]}, {bbox[0]} {bbox[3]}, {bbox[2]} {bbox[3]}, {bbox[2]} {bbox[1]}, {bbox[0]} {bbox[1]}]]}}, "properties": {{}}}}]}}'


def get_frequency_top_values(
    resource_fields_freqs: dict, field: str, count: int = 10
) -> list[dict]:
    """Get the top values for a field from the frequency data.

    Args:
        resource_fields_freqs: Dictionary containing frequency data for all fields
        field: The field to get the top values for
        count: The number of top values to return, defaults to 10

    Returns:
        List of dictionaries containing value, count and percentage for top values
    """
    if field not in resource_fields_freqs:
        return []

    # The data is already sorted by frequency in descending order from qsv frequency
    return resource_fields_freqs[field][:count]
