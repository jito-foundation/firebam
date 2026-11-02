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
            # A clash would generate two metrics with one name, caught only
            # as a redeclared enumerator when compiling the generated header.
            names = {metric.attrib.get('name') for metric in tile}
            for metric in ele:
                assert metric.attrib.get('name') not in names, f"BAM metric {ele.attrib['name']}.{metric.attrib.get('name')} redefines an upstream metric"
            tile.extend(list(ele))
        else:
            root.append(ele)
    merged = ET.tostring(root, encoding='unicode')
    lint_bam(merged)
    return merged

def lint_bam(merged: str) -> None:
    """The Makefile lints metrics.xml alone; hold the BAM metrics to the
    same NAMING RULES by linting the merged schema."""
    import lint_metrics
    errors = lint_metrics.lint(merged) + lint_metrics.lint_writers(merged, Path('metrics.xml').resolve().parents[3])
    for e in errors: print(e)
    if errors: raise SystemExit(f'{len(errors)} metrics naming violation(s) in metrics.xml merged with metrics_bam.xml')
