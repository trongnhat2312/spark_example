from __future__ import print_function, unicode_literals

import urllib2
import html2text
from pyspark.sql import SparkSession


def force_to_unicode(text):
    """If text is unicode, it is returned as is. If it's str, convert it to Unicode using UTF-8 encoding"""
    return text if isinstance(text, unicode) else text.decode('utf8')
# encoding="utf8"


h = html2text.HTML2Text()
h.ignore_links = True
h.ignore_images = True
h.ignore_emphasis = True
h.ignore_tables = True
h.skip_internal_links = True

spark = SparkSession \
    .builder \
    .appName("Basic operation with structured data") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df = spark.read.csv("../data/urls_new.txt", sep="|", encoding="utf-8")

# s= df.take(2)
# print(type(s[1][1]))

# each element of urls in format [id, url]
urls = df.rdd
path = u"../data/ldp_new_new/"


def get_text_from(url):
    global plain_text
    plain_text = u''
    try:
        req = urllib2.Request(url[1])
        f = urllib2.urlopen(req, timeout=4.0)
        encode = f.headers['content-type'].split("charset=")[-1]
        content = f.read()
        try:
            html_text = content.decode(encode)
        except LookupError:
            html_text = content.decode("utf-8")
        except UnicodeDecodeError:
            html_text = u''
        plain_text = h.handle(html_text)
        f.close()
    except urllib2.HTTPError:
        print("404 not found " + url[0])
        plain_text = u''
    except IOError:
        print('cannot open ' + url[0])
        plain_text = u''
    except UnicodeEncodeError:
        plain_text = u''
    file_name = path + url[0] + u".txt"
    with open(file_name, 'w') as f:
        print(url[0])
        f.write(plain_text.encode("utf-8"))


urls.foreach(get_text_from)

# with open("../data/a_text_file", "r") as f:
#     url = unicode(f.read(), encoding="utf-8")
#     print(url)
# f = urllib2.urlopen(url)
# print(f.headers['content-type'])
# encoding = f.headers['content-type'].split("charset=")[-1]
# content = f.read()
# try:
#     sss = unicode(content, encoding)
# except LookupError:
#     sss = force_to_unicode(content)
# print(type(sss))
#
#
# h = html2text.HTML2Text()
# h.ignore_links = True
# h.ignore_images = True
# h.ignore_emphasis = True
# h.ignore_tables = True
# h.skip_internal_links = True
# s = force_to_unicode(h.handle(sss))
# print(s)
# # with open("../data/html_plain.txt", "w") as f:
# #     f.write(sss.encode("utf-8"))
# # with open("../data/html.txt", "w") as f:
# #     f.write(s.encode("utf-8"))
