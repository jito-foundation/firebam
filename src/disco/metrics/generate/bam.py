import xml.etree.ElementTree as ET
from pathlib import Path

def merge_bam(xml_data: str) -> str:
    """Merge FireBAM's metrics_bam.xml into the metrics.xml text.

    Top-level enums and tiles not in metrics.xml are appended to the
    root.  The metrics of a tile that metrics.xml already defines are
    appended to the end of that tile, so upstream metric offsets are
    unchanged."""
    root = ET.fromstring(xml_data)
    bam = ET.fromstring(Path('metrics_bam.xml').read_text())
    for ele in bam:
        # parse_metrics would silently drop other kinds, and keep only the
        # last of two enums with one name.
        assert ele.tag in ('enum', 'tile'), f'unexpected element <{ele.tag}> in metrics_bam.xml'
        if ele.tag == 'enum':
            assert root.find(f"enum[@name='{ele.attrib['name']}']") is None, f"BAM enum {ele.attrib['name']} redefines an upstream enum"
        tile = None
        if ele.tag == 'tile':
            tile = root.find(f"tile[@name='{ele.attrib['name']}']")
        if tile is not None:
            tile.extend(list(ele))
        else:
            root.append(ele)
    return ET.tostring(root, encoding='unicode')
