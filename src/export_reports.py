from __future__ import annotations

import csv
import os
import tempfile
import zipfile
from contextlib import contextmanager
from datetime import datetime
from html import escape
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple


def _excel_col_name(col_idx: int) -> str:
    n = col_idx
    letters = []
    while n > 0:
        n, rem = divmod(n - 1, 26)
        letters.append(chr(65 + rem))
    return "".join(reversed(letters))


def _xml_cell(ref: str, value) -> str:
    if value is None:
        return f'<c r="{ref}"/>'
    if isinstance(value, bool):
        return f'<c r="{ref}" t="n"><v>{1 if value else 0}</v></c>'
    if isinstance(value, (int, float)):
        return f'<c r="{ref}" t="n"><v>{value}</v></c>'
    text = str(value)
    text = escape(text)
    return f'<c r="{ref}" t="inlineStr"><is><t xml:space="preserve">{text}</t></is></c>'


class SimpleXlsxBuilder:
    def __init__(self):
        self.tmpdir = Path(tempfile.mkdtemp(prefix="recon_xlsx_"))
        self.sheets: List[Tuple[str, Path]] = []

    def add_sheet(self, name: str, headers: Sequence[str], rows: Iterable[Sequence]):
        safe_name = name[:31]
        idx = len(self.sheets) + 1
        sheet_path = self.tmpdir / f"sheet{idx}.xml"
        with sheet_path.open("w", encoding="utf-8") as f:
            f.write('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>')
            f.write('<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">')
            f.write("<sheetData>")

            row_num = 1
            f.write(f'<row r="{row_num}">')
            for c, h in enumerate(headers, start=1):
                ref = f"{_excel_col_name(c)}{row_num}"
                f.write(_xml_cell(ref, h))
            f.write("</row>")

            for data_row in rows:
                row_num += 1
                f.write(f'<row r="{row_num}">')
                for c, val in enumerate(data_row, start=1):
                    ref = f"{_excel_col_name(c)}{row_num}"
                    f.write(_xml_cell(ref, val))
                f.write("</row>")

            f.write("</sheetData></worksheet>")
        self.sheets.append((safe_name, sheet_path))

    def build(self, output_file: Path) -> Path:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(output_file, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(
                "[Content_Types].xml",
                self._content_types_xml(),
            )
            zf.writestr(
                "_rels/.rels",
                """<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>
<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">
<Relationship Id=\"rId1\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument\" Target=\"xl/workbook.xml\"/>
</Relationships>""",
            )
            zf.writestr("xl/workbook.xml", self._workbook_xml())
            zf.writestr("xl/_rels/workbook.xml.rels", self._workbook_rels_xml())
            zf.writestr("xl/styles.xml", self._styles_xml())
            for i, (_, path) in enumerate(self.sheets, start=1):
                zf.write(path, f"xl/worksheets/sheet{i}.xml")
        return output_file

    def cleanup(self):
        for p in self.tmpdir.glob("*"):
            try:
                p.unlink()
            except Exception:
                pass
        try:
            self.tmpdir.rmdir()
        except Exception:
            pass

    def _content_types_xml(self) -> str:
        overrides = [
            '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>',
            '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>',
        ]
        for i in range(1, len(self.sheets) + 1):
            overrides.append(
                f'<Override PartName="/xl/worksheets/sheet{i}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
            )
        return (
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
            '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
            '<Default Extension="xml" ContentType="application/xml"/>'
            + "".join(overrides)
            + "</Types>"
        )

    def _workbook_xml(self) -> str:
        sheets = []
        for i, (name, _) in enumerate(self.sheets, start=1):
            sheets.append(f'<sheet name="{escape(name)}" sheetId="{i}" r:id="rId{i}"/>')
        return (
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
            '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
            'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
            f"<sheets>{''.join(sheets)}</sheets></workbook>"
        )

    def _workbook_rels_xml(self) -> str:
        rels = []
        for i in range(1, len(self.sheets) + 1):
            rels.append(
                f'<Relationship Id="rId{i}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{i}.xml"/>'
            )
        rels.append(
            f'<Relationship Id="rId{len(self.sheets)+1}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>'
        )
        return (
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            + "".join(rels)
            + "</Relationships>"
        )

    def _styles_xml(self) -> str:
        return """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts>
  <fills count="1"><fill><patternFill patternType="none"/></fill></fills>
  <borders count="1"><border/></borders>
  <cellStyleXfs count="1"><xf/></cellStyleXfs>
  <cellXfs count="1"><xf xfId="0"/></cellXfs>
</styleSheet>"""


def write_csv_file(path: Path, headers: Sequence[str], rows: Iterable[Sequence]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(list(headers))
        for row in rows:
            w.writerow(list(row))
