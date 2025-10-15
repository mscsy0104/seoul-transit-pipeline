import xml.etree.ElementTree as ET
from io import StringIO

declaration = """<?xml version="1.0" encoding="UTF-8"?>
<ksccPatternStation>
<list_total_count>294466</list_total_count>
<RESULT>
<CODE>INFO-000</CODE>
<MESSAGE>정상 처리되었습니다</MESSAGE>
</RESULT>"""
row = """<row>
<CRTR_DD>20251005</CRTR_DD>
<PRPS_PTRN>①[지하철] → ②[지하철] → ③[지하철] → ④[경기버스] → ⑤[지하철]</PRPS_PTRN>
<TNOPE>2</TNOPE>
<TNOPE_GNRL>2</TNOPE_GNRL>
<TNOPE_KID/>
<TNOPE_YOUTH/>
<TNOPE_ELDR/>
<TNOPE_PWDBS/>
</row>
<row>
<CRTR_DD>20251005</CRTR_DD>
<PRPS_PTRN>①[지하철] → ②[지하철] → ③[지하철] → ④[버스]</PRPS_PTRN>
<TNOPE>167</TNOPE>
<TNOPE_GNRL>156</TNOPE_GNRL>
<TNOPE_KID>8</TNOPE_KID>
<TNOPE_YOUTH>3</TNOPE_YOUTH>
<TNOPE_ELDR/>
<TNOPE_PWDBS/>
</row>"""

xml_text = declaration + row

try:
    xml_file = StringIO(xml_text)
    print(xml_file)
    tree = ET.parse(xml_file)
    print(tree)
    root = tree.getroot()
    print(root)
    print(root.tag)
except Exception as e:
    print(e)


