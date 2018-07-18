# import requests
#
# res = requests.get("http://www.lazada.vn/tan-trang-nha-cua-moi/?skus[]=SK300HLADC4BVNAMZ&skus["
#                    "]=BL200HLAEYJ8VNAMZ&skus[]=SK300HL64RPBANVN&skus["
#                    "]=BL200HLADD1YVNAMZ&wt_dp_l=vn.Display%20Local.ADX.["
#                    "AA]_HL_May%20khoan%20bua%20Banner%20300x250..&utm_source=ADX&utm_medium=Display%20Local"
#                    "&utm_campaign=[AA]_HL_May%20khoan%20bua%20Banner%20300x600_&utm_content=")
# if res.ok:
#     print(res.text)
# else:
#     print(res.status_code)
#     print(res.text)
import urllib2

import html2text


def force_to_unicode(text):
    """If text is unicode, it is returned as is. If it's str, convert it to Unicode using UTF-8 encoding"""
    return text if isinstance(text, unicode) else text.decode('utf8')


# encoding="utf8"

with open("../data/a_text_file", "r") as f:
    url = unicode(f.read(), encoding="utf-8")
    print(url)
f = urllib2.urlopen(url)
print(f.headers['content-type'])
encoding = f.headers['content-type'].split("charset=")[-1]
content = f.read()
try:
    sss = unicode(content, encoding)
except LookupError:
    sss = force_to_unicode(content)
print(type(sss))

h = html2text.HTML2Text()
h.ignore_links = True
h.ignore_images = True
h.ignore_emphasis = True
h.ignore_tables = True
h.skip_internal_links = True
s = force_to_unicode(h.handle(sss))
print(s)
# with open("../data/html_plain.txt", "w") as f:
#     f.write(sss.encode("utf-8"))
# with open("../data/html.txt", "w") as f:
#     f.write(s.encode("utf-8"))
