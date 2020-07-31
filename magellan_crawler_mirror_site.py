import hashlib
import os
import re
import time
import traceback
from collections import defaultdict
from functools import lru_cache

import requests
from logzero import logger
from lxml import html
from retry import retry

from solution_fast_get_distribute import AMQPConnection
# init
from solution_fast_get_distribute import Worker

connection = AMQPConnection.connectAMQP()
channel = connection.channel()
channel.exchange_declare(exchange='doan_van_dieu_link', exchange_type='topic')
ng_urls = [
    "kakaku.com/search_results/",
    "kakaku.com/bb/",
    "kakaku.com/bb/internet_access/",
    "kakaku.com/bike/",
    "kakaku.com/drink/",
    "kakaku.com/electricity-gas/",
    "kakaku.com/energy/",
    "kakaku.com/food/",
    "kakaku.com/gas/",
    "kakaku.com/hobby/ss_0024_0005/",
    "kakaku.com/hobby/ss_0024_0006/",
    "kakaku.com/hobby/ss_0024_0007/",
    "kakaku.com/hobby/ss_0024_0008/",
    "kakaku.com/hobby/ss_0024_0009/",
    "kakaku.com/housing/",
    "kakaku.com/housing/ss_0049_0009/",
    "kakaku.com/kuruma/",
    "kakaku.com/kuruma/used/",
    "kakaku.com/lighting/",
    "kakaku.com/mobile_data/",
    "kakaku.com/mobile_data/world-wifi/",
    "kakaku.com/pet/",
    "kakaku.com/reform/",
    "kakaku.com/sougi/",
    "kakaku.com/taiyoukou/",
    "kakaku.com/used/",
    "kakaku.com/used/camera/",
    "kakaku.com/used/golf/",
    "kakaku.com/used/keitai/",
    "kakaku.com/used/pc/",
    "kakaku.com/used/watch/",
    "kakaku.com/ksearch/redirect/",
    "kakaku.com/auth/",
    "kakaku.com/pt/ard.asp",
]

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36"
}


class MirrorSite:
    def __init__(self, url, level=4, dir_path="/tmp/"):
        self.origin_url = url
        self.parent_url = url
        self.parent_url_non_protocol = self.remove_protocol(url)
        self.level = level
        self.requested_urls = defaultdict(int)
        self.ng_urls = ng_urls
        self.dir_path = dir_path

    def remove_protocol(self, url):
        return url.replace("https://", "").replace("http://", "")

    def get_parent_url(self):
        try:
            res = requests.get(
                "https://" + self.parent_url_non_protocol, headers=headers
            )
            # url = "https://{}/".format(self.parent_url_non_protocol)
            # return url
            if res.text.find("<title>403 Error") >= 0:
                raise Exception("403 Error")
            return res.url
        except:
            pass

        try:
            res = requests.get(
                "http://" + self.parent_url_non_protocol, headers=headers
            )
            # url = "http://{}/".format(self.parent_url_non_protocol)
            # return url
            return res.url
        except:
            pass

        raise ValueError("Specify web site: {}".format(self.parent_url))

    def run(self):
        self.parent_url = self.get_parent_url()
        self.parent_url_non_protocol = self.remove_protocol(self.parent_url)

        for level_i in range(self.level + 1):
            self.crawl_count = 0
            self.requested_urls = defaultdict(int)
            for content, url in self.recursive(self.parent_url, level_i):
                self.save(content, url)
        return self.crawl_count

    def recursive(self, url, level):
        if level < 0:
            return

        if self.requested_urls.get(url, -1) >= level:
            return

        if self.crawl_count > 10000:
            return

        logger.info("{}\t{}\t{}".format(url, level, self.crawl_count))
        self.requested_urls[url] = level

        try:
            content = self.get_html(url)
        except:
            traceback.print_exc()
            logger.warning("Can not get page: {}".format(url))
            return

        yield content, url
        self.crawl_count += 1

        for children_url in self.get_children(content):
            yield from self.recursive(children_url, level - 1)

    @lru_cache(maxsize=2048)
    @retry(tries=1, delay=5)
    def get_html(self, url):
        """
        前回のクロール結果を使うともっと高速化できるようになる(タイムスタンプの概念が欲しい。リライト対策)
        """

        # キャッシュに無い場合はローカルファイルをチェックする
        filepath = self.existed_filepath(url)
        if filepath is not None:
            return open(filepath).read()

        time.sleep(1)
        # content = requests.get(url, timeout=(60, 60), verify=False).text
        res = requests.get(url, timeout=(60, 60), headers=headers)
        try:
            res.encoding = res.apparent_encoding
            content = res.text
            content = content.encode(res.encoding)
        except:
            content = res.text
        return html.make_links_absolute(content, base_url=url)

    def get_children(self, content):
        if content is None:
            logger.warning("content is None")
            return
        try:
            dom = html.document_fromstring(content)
        except:
            # print(type(content), len(content))
            traceback.print_exc()
            return
        # print(content)
        # print(type(content), len(content))
        # print(type(dom.xpath('//a')))
        # print(dom.xpath('//a'))
        for atag in dom.xpath("//a"):
            href = atag.attrib.get("href", None)
            if href is None:
                continue
            # urlの#以降を除去
            href = re.sub("#.*", "", href)

            if self.is_skip_url(href):
                continue

            yield href

    def is_skip_url(self, url):
        # htmlファイルでないリンクは無視
        if url[-4:] in {".mp3", ".png", ".jpg", ".css", ".gif", ".svg", ".txt", ".xml"}:
            return True
        if url[-5:] in {".jpeg"}:
            return True

        if self.remove_protocol(url).find(self.parent_url_non_protocol) != 0:
            return True

        # is_ng = sum([
        #     url.find(ng_url) >= 0
        #     for ng_url in self.ng_urls
        # ])
        # if is_ng > 0:
        #     return True
        for ng_url in self.ng_urls:
            if url.find(ng_url) >= 0:
                logger.info("ignore url: {}".format(url))
                return True

        return False

    def existed_filepath(self, url):
        # FIXME: def saveと合わせてリファクタリング
        filepath = self.dir_path + "/" + self.remove_protocol(url)
        directory = "/".join(filepath.split("/")[0:-1])
        filename = filepath.split("/")[-1].replace(".html", "") + ".html"
        if filename == ".html":
            filename = "index.html"

        output_fillepath = "{}/{}".format(directory, filename)
        if os.path.isfile(output_fillepath):
            return output_fillepath

        output_fillepath = (
                self.dir_path
                + "/"
                + self.parent_url_non_protocol
                + "/longname_dir/"
                + hashlib.sha1(self.remove_protocol(url).encode()).hexdigest()
        )
        if os.path.isfile(output_fillepath):
            return output_fillepath

        output_fillepath = (
                "{}/{}".format(directory, hashlib.sha1(filename.encode()).hexdigest())
                + ".html"
        )
        if os.path.isfile(output_fillepath):
            return output_fillepath

        return

    def save(self, content, url):
        try:
            filepath = self.dir_path + "/" + self.remove_protocol(url)
            directory = "/".join(filepath.split("/")[0:-1])
            filename = filepath.split("/")[-1].replace(".html", "") + ".html"
            if filename == ".html":
                filename = "index.html"
            os.makedirs(directory, exist_ok=True)
        except OSError:
            filepath = (
                    self.dir_path
                    + "/"
                    + self.parent_url_non_protocol
                    + "/longname_dir/"
                    + hashlib.sha1(self.remove_protocol(url).encode()).hexdigest()
            )
            directory = "/".join(filepath.split("/")[0:-1])
            filename = filepath.split("/")[-1].replace(".html", "") + ".html"
            if filename == ".html":
                filename = "index.html"
            os.makedirs(directory, exist_ok=True)

        if isinstance(content, bytes):
            try:
                content = content.decode()
            except:
                logger.info("bytes content: {}".format(url))
                return
        if content is None:
            return

        try:
            output_fillepath = "{}/{}".format(directory, filename)
            if os.path.isfile(output_fillepath):
                return

            with open(output_fillepath, "w") as f:
                f.write(content)
        except OSError:
            output_fillepath = (
                    "{}/{}".format(directory, hashlib.sha1(filename.encode()).hexdigest())
                    + ".html"
            )
            if os.path.isfile(output_fillepath):
                return

            with open(output_fillepath, "w") as f:
                f.write(content)

    # SOLUTION HERE
    def recursive_ver_2(self, url):
        logger.info("{}".format(url))
        try:
            content = self.get_html(url)
        except:
            traceback.print_exc()
            logger.warning("Can not get page: {}".format(url))

        yield content, url
        # self.crawl_count += 1
        my_set_url_unique = set();
        for children_url in self.get_children(content):
            my_set_url_unique.add(children_url)
        for url_need_crawl in my_set_url_unique:
            # Publish to Broker
            channel.basic_publish(exchange='link', routing_key='worker', body=url_need_crawl)
            # content2 = yield from self.recursive(children_url, level - 1)
            # logger.info(content2)

    def run_v2(self):
        channel.basic_publish(exchange='doan_van_dieu_link', routing_key='worker', body=self.parent_url)
        Worker.WorkerCrawl(20)

if __name__ == "__main__":
    site = "https://venture-finance.jp/"
    site = "https://venture-finance.jp/archives/6904"
    site = "https://www.y-cashing.com/"
    site = "https://ryota.site/"
    site = "https://l-lovefashion.com/"
    site = "https://blog.gepuro.net/"
    site = "https://veramagazine.jp/"
    site = "http://venture-finance.jp/"
    level = 4
    MirrorSite(site, level,"/tmp/doan_van_dieu").run_v2()

#####################
######### RABBIT MANAGER
####### host
######  username
######  password