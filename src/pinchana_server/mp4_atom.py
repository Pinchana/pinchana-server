"""MP4 Top-Level Atom/Box Parser for fast-start moov verification."""

import struct
from pathlib import Path
from typing import Dict, Any, List, Optional


def inspect_mp4_atoms(file_path: Path, max_read_bytes: int = 100 * 1024 * 1024) -> Dict[str, Any]:
    """
    Parse top-level MP4 boxes using bounded binary reads.

    Handles:
    - Standard 32-bit size boxes.
    - Extended 64-bit size boxes (when size == 1).
    - Zero-size boxes extending to EOF (when size == 0).
    - Truncated and corrupt box headers.

    Returns dict containing:
    - 'is_faststart': True if 'moov' atom is found before 'mdat' atom.
    - 'moov_offset': Byte offset of 'moov' atom (or None).
    - 'mdat_offset': Byte offset of 'mdat' atom (or None).
    - 'atoms': List of top-level atom dicts.
    """
    if not file_path.is_file():
        return {"is_faststart": False, "moov_offset": None, "mdat_offset": None, "atoms": []}

    atoms: List[Dict[str, Any]] = []
    moov_offset: Optional[int] = None
    mdat_offset: Optional[int] = None

    file_size = file_path.stat().st_size

    try:
        with open(file_path, "rb") as f:
            curr_pos = 0
            while curr_pos < file_size and curr_pos < max_read_bytes:
                header = f.read(8)
                if len(header) < 8:
                    break  # Truncated header

                size_32, box_type_bytes = struct.unpack(">I4s", header)
                try:
                    box_type = box_type_bytes.decode("ascii", errors="replace")
                except Exception:
                    box_type = "unknown"

                box_header_size = 8
                box_total_size = size_32

                if size_32 == 1:
                    # 64-bit extended size
                    large_size_bytes = f.read(8)
                    if len(large_size_bytes) < 8:
                        break  # Truncated extended size header
                    box_total_size = struct.unpack(">Q", large_size_bytes)[0]
                    box_header_size = 16
                elif size_32 == 0:
                    # Box extends to EOF
                    box_total_size = file_size - curr_pos

                if box_total_size < box_header_size:
                    # Corrupt box size
                    break

                if box_type == "moov" and moov_offset is None:
                    moov_offset = curr_pos
                elif box_type == "mdat" and mdat_offset is None:
                    mdat_offset = curr_pos

                atoms.append({
                    "type": box_type,
                    "offset": curr_pos,
                    "size": box_total_size,
                    "header_size": box_header_size,
                })

                # If both moov and mdat are found, we can stop scanning
                if moov_offset is not None and mdat_offset is not None:
                    break

                # Advance file pointer to next top-level box
                next_pos = curr_pos + box_total_size
                if next_pos <= curr_pos:
                    break  # Prevent infinite loop on 0 or invalid size

                curr_pos = next_pos
                f.seek(curr_pos)
    except Exception:
        pass  # Graceful handling for corrupt or unreadable files

    is_faststart = False
    if moov_offset is not None and mdat_offset is not None:
        is_faststart = moov_offset < mdat_offset

    return {
        "is_faststart": is_faststart,
        "moov_offset": moov_offset,
        "mdat_offset": mdat_offset,
        "atoms": atoms,
    }
