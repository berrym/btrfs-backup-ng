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
    "parse_info_xml_string",
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


def _parse_userdata(userdata_elems: list[ET.Element]) -> dict[str, str]:
    """Parse snapper's <userdata> element(s) into a {name: value} dict.

    Snapper (verified on 0.13.0) writes ONE ``<userdata>`` element PER entry, each
    holding a ``<key>NAME</key><value>VALUE</value>`` pair -- so a two-entry snapshot
    has two sibling ``<userdata>`` blocks. Iterate every block (not just the first)
    and pair key/value within it, so multiple entries all survive. The old code used
    ``root.find("userdata")`` (first block only) AND the child tag as the dict key,
    silently dropping every entry after the first -- multi-entry userdata data loss.

    Also handles a single block containing several key/value pairs, and a legacy
    ``<name>value</name>`` child, for robustness.
    """
    userdata: dict[str, str] = {}
    for userdata_elem in userdata_elems:
        pending_key: Optional[str] = None
        for item in userdata_elem:
            if item.tag == "key":
                pending_key = item.text or ""
            elif item.tag == "value":
                if pending_key is not None:
                    userdata[pending_key] = item.text or ""
                    pending_key = None
                # A <value> with no preceding <key> is malformed; ignore it.
            elif item.tag and item.text:
                # Legacy / alternative <name>value</name> form.
                userdata[item.tag] = item.text
    return userdata


def _snapper_metadata_from_root(root: ET.Element) -> SnapperMetadata:
    """Build a SnapperMetadata from a parsed <snapshot> element.

    Shared by ``parse_info_xml`` (file) and ``parse_info_xml_string`` (a sidecar's
    stored ``original_info_xml``) so both read snapper's format identically.
    """
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

    return SnapperMetadata(
        type=type_elem.text,
        num=num,
        date=date,
        description=description,
        cleanup=cleanup,
        pre_num=pre_num,
        userdata=_parse_userdata(root.findall("userdata")),
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

    return _snapper_metadata_from_root(root)


def parse_info_xml_string(xml_content: str) -> SnapperMetadata:
    """Parse snapper info.xml from a string.

    Used to reconstruct metadata from a sidecar's stored ``original_info_xml`` --
    snapper's own XML, the authoritative source on restore (independent of the
    parsed-dict field, which older sidecars may have stored mis-parsed).

    Raises:
        ValueError: If the XML is malformed or missing required fields
    """
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        raise ValueError(f"Failed to parse info.xml: {e}") from e

    return _snapper_metadata_from_root(root)


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

        def _esc(s: str) -> str:
            return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

        # Snapper writes one <userdata> block PER entry, each a <key>/<value> pair.
        for key, value in metadata.userdata.items():
            lines.append("  <userdata>")
            lines.append(f"    <key>{_esc(key)}</key>")
            lines.append(f"    <value>{_esc(value)}</value>")
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

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BackupMetadata":
        """Create from a JSON-decoded dict.

        Single construction site shared by ``load_backup_metadata`` (local file)
        and the raw+ssh enumeration path (a ``.snapper-meta.json`` cat'd back over
        ssh), so the two never drift.
        """
        return cls(
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


def save_backup_metadata(path: Path | str, metadata: BackupMetadata) -> None:
    """Save backup metadata to a JSON file.

    Args:
        path: Path to the .snapper-meta.json file
        metadata: BackupMetadata object to save
    """
    from btrfs_backup_ng import __util__

    path = Path(path)
    data = asdict(metadata)
    # Write atomically (temp -> fsync -> os.replace) so a crash mid-write can never leave a
    # torn .snapper-meta.json that load_backup_metadata would reject -- fulfilling the R7
    # deferral for this sidecar, now that R11 reads it back on restore. R11/R7.
    __util__.atomic_write_bytes(  # type: ignore[attr-defined]
        path, json.dumps(data, indent=2), mode=0o600
    )


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

    return BackupMetadata.from_dict(data)
