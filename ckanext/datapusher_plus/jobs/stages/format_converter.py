# -*- coding: utf-8 -*-
"""
Format Converter stage for the DataPusher Plus pipeline.

Handles conversion of various file formats to CSV.
"""

import os
import uuid
import subprocess
from typing import Optional

import ckanext.datapusher_plus.utils as utils
import ckanext.datapusher_plus.config as conf
import ckanext.datapusher_plus.spatial_helpers as sh
import ckanext.datapusher_plus.datastore_utils as dsu
from ckanext.datapusher_plus.jobs.stages.base import BaseStage
from ckanext.datapusher_plus.jobs.context import ProcessingContext


class FormatConverterStage(BaseStage):
    """
    Converts various file formats to CSV.

    Responsibilities:
    - Convert spreadsheets (XLS, XLSX, ODS, etc.) to CSV
    - Convert spatial formats (SHP, GEOJSON) to CSV
    - Normalize CSV/TSV/TAB files
    - Transcode to UTF-8
    """

    # Supported format types
    SPREADSHEET_EXTENSIONS = ["XLS", "XLSX", "ODS", "XLSM", "XLSB"]
    SPATIAL_FORMATS = ["SHP", "QGIS", "GEOJSON"]

    def __init__(self):
        super().__init__(name="FormatConverter")

    def process(self, context: ProcessingContext) -> ProcessingContext:
        """
        Convert file format to CSV.

        Args:
            context: Processing context

        Returns:
            Updated context with CSV file

        Raises:
            utils.JobError: If conversion fails
        """
        resource_format = context.resource.get("format", "").upper()

        # Check if file is a spreadsheet
        if resource_format in self.SPREADSHEET_EXTENSIONS:
            self._convert_spreadsheet(context, resource_format)
        # Check if file is a spatial format
        elif resource_format in self.SPATIAL_FORMATS:
            self._convert_spatial_format(context, resource_format)
        # Otherwise normalize as CSV/TSV/TAB
        else:
            self._normalize_csv(context, resource_format)

        return context

    def _convert_spreadsheet(
        self, context: ProcessingContext, file_format: str
    ) -> None:
        """
        Convert spreadsheet to CSV using qsv excel.

        Args:
            context: Processing context
            file_format: Spreadsheet format (XLS, XLSX, etc.)

        Raises:
            utils.JobError: If conversion fails
        """
        default_excel_sheet = conf.DEFAULT_EXCEL_SHEET
        context.logger.info(
            f"Converting {file_format} sheet {default_excel_sheet} to CSV..."
        )

        # Create hardlink with proper extension
        qsv_spreadsheet = os.path.join(
            context.temp_dir, "qsv_spreadsheet." + file_format
        )
        os.link(context.tmp, qsv_spreadsheet)

        # Run qsv excel to export to CSV
        qsv_excel_csv = os.path.join(context.temp_dir, "qsv_excel.csv")
        try:
            qsv_excel = context.qsv.excel(
                qsv_spreadsheet,
                sheet=default_excel_sheet,
                trim=True,
                output_file=qsv_excel_csv,
            )
        except utils.JobError as e:
            raise utils.JobError(
                f"Upload aborted. Cannot export spreadsheet(?) to CSV: {e}"
            )

        excel_export_msg = qsv_excel.stderr
        context.logger.info(f"{excel_export_msg}...")
        context.update_tmp(qsv_excel_csv)

    def _convert_spatial_format(
        self, context: ProcessingContext, resource_format: str
    ) -> None:
        """
        Convert spatial format to CSV.

        Args:
            context: Processing context
            resource_format: Spatial format (SHP, GEOJSON, etc.)

        Raises:
            utils.JobError: If conversion fails
        """
        context.logger.info("SHAPEFILE or GEOJSON file detected...")

        # Create unique spatial file
        qsv_spatial_file = os.path.join(
            context.temp_dir,
            f"qsv_spatial_{uuid.uuid4()}.{resource_format}",
        )
        os.link(context.tmp, qsv_spatial_file)
        qsv_spatial_csv = os.path.join(context.temp_dir, "qsv_spatial.csv")

        simplification_failed = False

        # Try spatial simplification if enabled
        if conf.AUTO_SPATIAL_SIMPLIFICATION:
            simplification_failed = not self._try_spatial_simplification(
                context, qsv_spatial_file, qsv_spatial_csv, resource_format
            )

        # Fallback to qsv geoconvert if simplification failed or disabled
        if not conf.AUTO_SPATIAL_SIMPLIFICATION or simplification_failed:
            self._geoconvert(context, qsv_spatial_file, resource_format)

    def _try_spatial_simplification(
        self,
        context: ProcessingContext,
        spatial_file: str,
        output_csv: str,
        resource_format: str,
    ) -> bool:
        """
        Try to convert and simplify spatial file.

        Args:
            context: Processing context
            spatial_file: Path to spatial file
            output_csv: Output CSV path
            resource_format: Spatial format

        Returns:
            True if successful, False otherwise
        """
        context.logger.info(
            f"Converting spatial file to CSV with a simplification relative "
            f"tolerance of {conf.SPATIAL_SIMPLIFICATION_RELATIVE_TOLERANCE}..."
        )

        try:
            success, error_message, bounds = sh.process_spatial_file(
                spatial_file,
                resource_format,
                output_csv_path=output_csv,
                tolerance=conf.SPATIAL_SIMPLIFICATION_RELATIVE_TOLERANCE,
                task_logger=context.logger,
            )

            if success:
                context.logger.info(
                    "Spatial file successfully simplified and converted to CSV"
                )
                context.update_tmp(output_csv)
                self._upload_simplified_resource(context, spatial_file, bounds)
                return True
            else:
                context.logger.warning(
                    f"Upload of simplified spatial file failed: {error_message}"
                )
                return False

        except Exception as e:
            context.logger.warning(f"Simplification and conversion failed: {str(e)}")
            context.logger.warning(
                f"Simplification failed. Using qsv geoconvert to convert to CSV, "
                f"truncating large columns to {conf.QSV_STATS_STRING_MAX_LENGTH} characters..."
            )
            return False

    def _upload_simplified_resource(
        self, context: ProcessingContext, spatial_file: str, bounds: Optional[tuple]
    ) -> None:
        """
        Upload simplified spatial resource to CKAN.

        Args:
            context: Processing context
            spatial_file: Path to simplified spatial file
            bounds: Bounding box coordinates (minx, miny, maxx, maxy)
        """
        resource = context.resource
        simplified_resource_name = (
            os.path.splitext(resource["name"])[0]
            + "_simplified"
            + os.path.splitext(resource["name"])[1]
        )

        existing_resource, existing_resource_id = dsu.resource_exists(
            resource["package_id"], simplified_resource_name
        )

        if existing_resource:
            context.logger.info("Simplified resource already exists. Replacing it...")
            dsu.delete_resource(existing_resource_id)
        else:
            context.logger.info("Simplified resource does not exist. Uploading it...")

        new_simplified_resource = {
            "package_id": resource["package_id"],
            "name": simplified_resource_name,
            "url": "",
            "format": resource["format"],
            "hash": "",
            "mimetype": resource["mimetype"],
            "mimetype_inner": resource["mimetype_inner"],
        }

        # Add bounds information if available
        if bounds:
            minx, miny, maxx, maxy = bounds
            new_simplified_resource.update(
                {
                    "dpp_spatial_extent": {
                        "type": "BoundingBox",
                        "coordinates": [[minx, miny], [maxx, maxy]],
                    }
                }
            )
            context.logger.info(
                f"Added dpp_spatial_extent to resource metadata: {bounds}"
            )

        dsu.upload_resource(new_simplified_resource, spatial_file)
        os.remove(spatial_file)

    def _geoconvert(
        self, context: ProcessingContext, spatial_file: str, resource_format: str
    ) -> None:
        """
        Convert spatial file using qsv geoconvert.

        Args:
            context: Processing context
            spatial_file: Path to spatial file
            resource_format: Spatial format

        Raises:
            utils.JobError: If geoconvert fails
        """
        context.logger.info("Converting spatial file to CSV using qsv geoconvert...")

        qsv_geoconvert_csv = os.path.join(context.temp_dir, "qsv_geoconvert.csv")
        try:
            context.qsv.geoconvert(
                context.tmp,
                resource_format,
                "csv",
                max_length=conf.QSV_STATS_STRING_MAX_LENGTH,
                output_file=qsv_geoconvert_csv,
            )
        except utils.JobError as e:
            raise utils.JobError(f"qsv geoconvert failed: {e}")

        context.update_tmp(qsv_geoconvert_csv)
        context.logger.info("Geoconverted successfully")

    def _normalize_csv(self, context: ProcessingContext, resource_format: str) -> None:
        """
        Normalize CSV/TSV/TAB and transcode to UTF-8.

        Args:
            context: Processing context
            resource_format: File format

        Raises:
            utils.JobError: If normalization fails
        """
        # Log appropriate message
        if resource_format == "CSV":
            context.logger.info(f"Normalizing/UTF-8 transcoding {resource_format}...")
        else:
            context.logger.info(
                f"Normalizing/UTF-8 transcoding {resource_format} to CSV..."
            )

        qsv_input_csv = os.path.join(context.temp_dir, "qsv_input.csv")
        qsv_input_utf_8_encoded_csv = os.path.join(
            context.temp_dir, "qsv_input_utf_8_encoded.csv"
        )

        # Detect file encoding
        encoding = self._detect_encoding(context)

        # Re-encode to UTF-8 if needed
        if encoding not in ("UTF-8", "ASCII"):
            context.logger.info(f"File is not UTF-8 encoded. Re-encoding from {encoding} to UTF-8")
            self._reencode_to_utf8(context, encoding, qsv_input_utf_8_encoded_csv)
            source_file = qsv_input_utf_8_encoded_csv
        else:
            source_file = context.tmp

        # Normalize using qsv input
        try:
            context.qsv.input(source_file, trim_headers=True, output_file=qsv_input_csv)
        except utils.JobError as e:
            raise utils.JobError(
                f"Job aborted as the file cannot be normalized/transcoded: {e}."
            )

        context.update_tmp(qsv_input_csv)
        context.logger.info("Normalized & transcoded...")

    def _detect_encoding(self, context: ProcessingContext) -> str:
        """
        Detect file encoding using uchardet.

        Args:
            context: Processing context

        Returns:
            Detected encoding string

        Raises:
            utils.JobError: If encoding detection fails
        """
        try:
            file_encoding = subprocess.run(
                ["uchardet", context.tmp],
                check=True,
                capture_output=True,
                text=True,
            )
            encoding = file_encoding.stdout.strip()
            context.logger.info(f"Identified encoding of the file: {encoding}")
            return encoding
        except subprocess.CalledProcessError as e:
            raise utils.JobError(f"Failed to detect file encoding: {e}")

    def _reencode_to_utf8(
        self, context: ProcessingContext, from_encoding: str, output_file: str
    ) -> None:
        """
        Re-encode file to UTF-8 using iconv.

        Args:
            context: Processing context
            from_encoding: Source encoding
            output_file: Output file path

        Raises:
            utils.JobError: If re-encoding fails
        """
        try:
            cmd = subprocess.run(
                ["iconv", "-f", from_encoding, "-t", "UTF-8", context.tmp],
                capture_output=True,
                check=True,
            )
            with open(output_file, "wb") as f:
                f.write(cmd.stdout)
            context.logger.info("Successfully re-encoded to UTF-8")
        except subprocess.CalledProcessError as e:
            raise utils.JobError(
                f"Job aborted as the file cannot be re-encoded to UTF-8. {e.stderr}"
            )
