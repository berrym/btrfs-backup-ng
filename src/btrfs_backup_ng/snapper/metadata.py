"""Snapper metadata handling.

This module handles parsing and generating snapper's info.xml metadata files
that accompany each snapshot.
"""

import json
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

__all__ = [
    "SnapperMetadata",
    "parse_info_xml",
    "generate_info_xml",
    "load_backup_metadata",
    "save_backup_metadata",
]

# Snapper's date format in info.xml
SNAPPER_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


@dataclass
class SnapperMetadata:
    """Metadata for a snapper snapshot.

    Attributes:
        type: Snapshot type ('single', 'pre', or 'post')
        num: Snapshot number
        date: Snapshot creation date
        description: Human-readable description
        cleanup: Cleanup algorithm ('timeline', 'number', or empty)
        pre_num: For 'post' snapshots, the number of the paired 'pre' snapshot
        userdata: Additional user-defined key-value data
    """

    type: str
    num: int
    date: datetime
    description: str = ""
    cleanup: str = ""
    pre_num: Optional[int] = None
    userdata: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "type": self.type,
            "num": self.num,
            "date": self.date.strftime(SNAPPER_DATE_FORMAT),
            "description": self.description,
            "cleanup": self.cleanup,
            "pre_num": self.pre_num,
            "userdata": self.userdata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SnapperMetadata":
        """Create from dictionary."""
        date = data.get("date", "")
        if isinstance(date, str):
            date = datetime.strptime(date, SNAPPER_DATE_FORMAT)
        return cls(
            type=data.get("type", "single"),
            num=data.get("num", 0),
            date=date,
            description=data.get("description", ""),
            cleanup=data.get("cleanup", ""),
            pre_num=data.get("pre_num"),
            userdata=data.get("userdata", {}),
        )


def parse_info_xml(path: Path | str) -> SnapperMetadata:
    """Parse a snapper info.xml file.

    Args:
        path: Path to the info.xml file

    Returns:
        SnapperMetadata object with parsed information

    Raises:
        FileNotFoundError: If the file doesn't exist
        ValueError: If the XML is malformed or missing required fields
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"info.xml not found: {path}")

    try:
        tree = ET.parse(path)
        root = tree.getroot()
    except ET.ParseError as e:
        raise ValueError(f"Failed to parse info.xml: {e}") from e

    if root.tag != "snapshot":
        raise ValueError(f"Expected <snapshot> root element, got <{root.tag}>")

    # Extract required fields
    type_elem = root.find("type")
    num_elem = root.find("num")
    date_elem = root.find("date")

    if type_elem is None or type_elem.text is None:
        raise ValueError("Missing <type> element in info.xml")
    if num_elem is None or num_elem.text is None:
        raise ValueError("Missing <num> element in info.xml")
    if date_elem is None or date_elem.text is None:
        raise ValueError("Missing <date> element in info.xml")

    try:
        num = int(num_elem.text)
    except ValueError as e:
        raise ValueError(f"Invalid snapshot number: {num_elem.text}") from e

    try:
        date = datetime.strptime(date_elem.text, SNAPPER_DATE_FORMAT)
    except ValueError as e:
        raise ValueError(f"Invalid date format: {date_elem.text}") from e

    # Extract optional fields
    description_elem = root.find("description")
    description = (
        description_elem.text
        if description_elem is not None and description_elem.text
        else ""
    )

    cleanup_elem = root.find("cleanup")
    cleanup = (
        cleanup_elem.text if cleanup_elem is not None and cleanup_elem.text else ""
    )

    pre_num_elem = root.find("pre_num")
    pre_num = None
    if pre_num_elem is not None and pre_num_elem.text:
        try:
            pre_num = int(pre_num_elem.text)
        except ValueError:
            pass  # Ignore invalid pre_num

    # Extract userdata
    userdata = {}
    userdata_elem = root.find("userdata")
    if userdata_elem is not None:
        for item in userdata_elem:
            if item.tag and item.text:
                userdata[item.tag] = item.text

    return SnapperMetadata(
        type=type_elem.text,
        num=num,
        date=date,
        description=description,
        cleanup=cleanup,
        pre_num=pre_num,
        userdata=userdata,
    )


def generate_info_xml(metadata: SnapperMetadata) -> str:
    """Generate snapper info.xml content from metadata.

    Args:
        metadata: SnapperMetadata object

    Returns:
        XML string suitable for writing to info.xml
    """
    lines = ['<?xml version="1.0"?>', "<snapshot>"]

    lines.append(f"  <type>{metadata.type}</type>")
    lines.append(f"  <num>{metadata.num}</num>")
    lines.append(f"  <date>{metadata.date.strftime(SNAPPER_DATE_FORMAT)}</date>")

    if metadata.description:
        # Escape XML special characters
        desc = metadata.description
        desc = desc.replace("&", "&amp;")
        desc = desc.replace("<", "&lt;")
        desc = desc.replace(">", "&gt;")
        lines.append(f"  <description>{desc}</description>")

    if metadata.cleanup:
        lines.append(f"  <cleanup>{metadata.cleanup}</cleanup>")

    if metadata.pre_num is not None:
        lines.append(f"  <pre_num>{metadata.pre_num}</pre_num>")

    if metadata.userdata:
        lines.append("  <userdata>")
        for key, value in metadata.userdata.items():
            value = value.replace("&", "&amp;")
            value = value.replace("<", "&lt;")
            value = value.replace(">", "&gt;")
            lines.append(f"    <{key}>{value}</{key}>")
        lines.append("  </userdata>")

    lines.append("</snapshot>")
    return "\n".join(lines)


@dataclass
class BackupMetadata:
    """Extended metadata stored with backups.

    This includes the original snapper metadata plus additional
    context needed for restoration.
    """

    snapper_config: str
    snapper_number: int
    snapper_type: str
    snapper_description: str
    snapper_cleanup: str
    snapper_pre_num: Optional[int]
    snapper_userdata: dict[str, str]
    snapper_date: str
    original_info_xml: str

    @classmethod
    def from_snapper_metadata(
        cls, config_name: str, metadata: SnapperMetadata, original_xml: str
    ) -> "BackupMetadata":
        """Create from snapper metadata."""
        return cls(
            snapper_config=config_name,
            snapper_number=metadata.num,
            snapper_type=metadata.type,
            snapper_description=metadata.description,
            snapper_cleanup=metadata.cleanup,
            snapper_pre_num=metadata.pre_num,
            snapper_userdata=metadata.userdata,
            snapper_date=metadata.date.strftime(SNAPPER_DATE_FORMAT),
            original_info_xml=original_xml,
        )

    def to_snapper_metadata(self) -> SnapperMetadata:
        """Convert back to SnapperMetadata."""
        return SnapperMetadata(
            type=self.snapper_type,
            num=self.snapper_number,
            date=datetime.strptime(self.snapper_date, SNAPPER_DATE_FORMAT),
            description=self.snapper_description,
            cleanup=self.snapper_cleanup,
            pre_num=self.snapper_pre_num,
            userdata=self.snapper_userdata,
        )


def save_backup_metadata(path: Path | str, metadata: BackupMetadata) -> None:
    """Save backup metadata to a JSON file.

    Args:
        path: Path to the .snapper-meta.json file
        metadata: BackupMetadata object to save
    """
    path = Path(path)
    data = asdict(metadata)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def load_backup_metadata(path: Path | str) -> BackupMetadata:
    """Load backup metadata from a JSON file.

    Args:
        path: Path to the .snapper-meta.json file

    Returns:
        BackupMetadata object

    Raises:
        FileNotFoundError: If the file doesn't exist
        ValueError: If the JSON is invalid
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Metadata file not found: {path}")

    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid metadata JSON: {e}") from e

    return BackupMetadata(
        snapper_config=data.get("snapper_config", ""),
        snapper_number=data.get("snapper_number", 0),
        snapper_type=data.get("snapper_type", "single"),
        snapper_description=data.get("snapper_description", ""),
        snapper_cleanup=data.get("snapper_cleanup", ""),
        snapper_pre_num=data.get("snapper_pre_num"),
        snapper_userdata=data.get("snapper_userdata", {}),
        snapper_date=data.get("snapper_date", ""),
        original_info_xml=data.get("original_info_xml", ""),
    )
