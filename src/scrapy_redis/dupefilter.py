import hashlib   # 用于生成请求指纹
import json
import logging
import time

from scrapy.dupefilters import BaseDupeFilter  # 导入 Scrapy 的 BaseDupeFilter 基类, 用于实现去重过滤器
from scrapy.utils.python import to_unicode   # 用于将 Python 对象转换为 Unicode 字符串
from w3lib.url import canonicalize_url  # 用于规范化 URL

from . import defaults  # 从 scrapy_redis.defaults 中导入默认值
from .connection import get_redis_from_settings  # 从 scrapy_redis.connection 中导入 get_redis_from_settings 函数, 用于从设置中获取 Redis 实例

# 获取日志记录器实例
logger = logging.getLogger(__name__)

# TODO: 将类名重命名为 RedisDupeFilter。
class RFPDupeFilter(BaseDupeFilter):
    """基于 Redis 的请求去重过滤器。

    该类可以与 Scrapy 默认的调度器一起使用。
    """

    logger = logger

    def __init__(self, server, key, debug=False):
        """初始化去重过滤器。

        参数
        ----------
        server : redis.StrictRedis
            Redis 服务器实例。
        key : str
            存储指纹的 Redis 键。
        debug : bool, optional
            是否记录过滤的请求。

        """
        self.server = server  # Redis 服务器实例
        self.key = key  # Redis 中存储指纹的键
        self.debug = debug  # 是否启用调试日志
        self.logdupes = True  # 是否记录去重日志

    @classmethod  # 类方法, 用于创建实例, 类方法的第一个参数必须是 cls,作用是将类本身作为第一个参数传入
    def from_settings(cls, settings):
        """根据设置返回实例。

        默认使用键 ``dupefilter:<timestamp>``。当使用
        ``scrapy_redis.scheduler.Scheduler`` 类时，此方法不会被使用，
        因为它需要将爬虫名称传递到键中。

        参数
        ----------
        settings : scrapy.settings.Settings

        返回
        -------
        RFPDupeFilter
            RFPDupeFilter 实例。
        """
        server = get_redis_from_settings(settings)  # 从设置中获取 Redis 实例
        # 创建一次性键，支持作为独立去重过滤器使用 Scrapy 的默认调度器
        key = defaults.DUPEFILTER_KEY % {"timestamp": int(time.time())}
        debug = settings.getbool("DUPEFILTER_DEBUG")  # 从设置中获取调试标志
        return cls(server, key=key, debug=debug)

    @classmethod
    def from_crawler(cls, crawler):
        """根据爬虫返回实例。

        参数
        ----------
        crawler : scrapy.crawler.Crawler

        返回
        -------
        RFPDupeFilter
            RFPDupeFilter 实例。
        """
        return cls.from_settings(crawler.settings)  # 从爬虫的设置中创建实例

    def request_seen(self, request):
        """检查请求是否已被看到。

        参数
        ----------
        request : scrapy.http.Request

        返回
        -------
        bool
            如果请求已经看到则返回 True，否则返回 False。
        """
        fp = self.request_fingerprint(request)  # 生成请求指纹
        # 将指纹添加到 Redis 集合中。如果指纹已存在，则返回 0
        added = self.server.sadd(self.key, fp)
        return added == 0  # 如果返回 0，说明请求已经存在

    def request_fingerprint(self, request):
        """为给定的请求返回指纹。

        参数
        ----------
        request : scrapy.http.Request

        返回
        -------
        str
            请求的指纹。
        """
        # 构建请求的指纹数据
        fingerprint_data = {
            "method": to_unicode(request.method),  # 请求方法（如 GET、POST）
            "url": canonicalize_url(request.url),  # 规范化 URL
            "body": (request.body or b"").hex(),  # 请求体的十六进制表示
        }
        # 将指纹数据转换为 JSON 字符串，并排序键
        fingerprint_json = json.dumps(fingerprint_data, sort_keys=True)
        # 生成 SHA1 指纹
        return hashlib.sha1(fingerprint_json.encode()).hexdigest()

    @classmethod
    def from_spider(cls, spider):
        """从爬虫返回实例。

        参数
        ----------
        spider : scrapy.spiders.Spider

        返回
        -------
        RFPDupeFilter
            RFPDupeFilter 实例。
        """
        settings = spider.settings
        server = get_redis_from_settings(settings)  # 从设置中获取 Redis 实例
        dupefilter_key = settings.get(
            "SCHEDULER_DUPEFILTER_KEY", defaults.SCHEDULER_DUPEFILTER_KEY
        )  # 获取去重过滤器的 Redis 键
        key = dupefilter_key % {"spider": spider.name}  # 以爬虫名称格式化键
        debug = settings.getbool("DUPEFILTER_DEBUG")  # 从设置中获取调试标志
        return cls(server, key=key, debug=debug)  # 创建实例并返回

    def close(self, reason=""):
        """关闭时删除数据。由 Scrapy 的调度器调用。

        参数
        ----------
        reason : str, optional
            关闭的原因。
        """
        self.clear()  # 清除 Redis 中存储的指纹数据

    def clear(self):
        """清除指纹数据。"""
        self.server.delete(self.key)  # 从 Redis 中删除键

    def log(self, request, spider):
        """记录给定请求的日志。

        参数
        ----------
        request : scrapy.http.Request
        spider : scrapy.spiders.Spider
        """
        if self.debug:
            msg = "Filtered duplicate request: %(request)s"
            # 记录调试日志
            self.logger.debug(msg, {"request": request}, extra={"spider": spider})
        elif self.logdupes:
            msg = (
                "Filtered duplicate request %(request)s"
                " - no more duplicates will be shown"
                " (see DUPEFILTER_DEBUG to show all duplicates)"
            )
            # 记录日志并禁用进一步的去重日志
            self.logger.debug(msg, {"request": request}, extra={"spider": spider})
            self.logdupes = False
