import importlib

from scrapy.utils.misc import load_object

from . import connection, defaults


# TODO: add SCRAPY_JOB support.
class Scheduler:
    """基于 Redis 的调度器，用于管理请求队列和去重过滤器。

    配置参数
    --------
    SCHEDULER_PERSIST : bool (默认值: False)
        是否在关闭时保留 Redis 队列数据。
    SCHEDULER_FLUSH_ON_START : bool (默认值: False)
        是否在启动时清空 Redis 队列数据。
    SCHEDULER_IDLE_BEFORE_CLOSE : int (默认值: 0)
        在没有消息接收时，关闭前等待的秒数。
    SCHEDULER_QUEUE_KEY : str
        调度器 Redis 队列的键。
    SCHEDULER_QUEUE_CLASS : str
        调度器队列类的路径。
    SCHEDULER_DUPEFILTER_KEY : str
        调度器去重过滤器的 Redis 键。
    SCHEDULER_DUPEFILTER_CLASS : str
        调度器去重过滤器类的路径。
    SCHEDULER_SERIALIZER : str
        调度器序列化器的路径。

    """

    def __init__(
        self,
        server,
        persist=False,
        flush_on_start=False,
        queue_key=defaults.SCHEDULER_QUEUE_KEY,
        queue_cls=defaults.SCHEDULER_QUEUE_CLASS,
        dupefilter=None,
        dupefilter_key=defaults.SCHEDULER_DUPEFILTER_KEY,
        dupefilter_cls=defaults.SCHEDULER_DUPEFILTER_CLASS,
        idle_before_close=0,
        serializer=None,
    ):
        """初始化调度器。

        参数
        ----------
        server : Redis
            Redis 服务器实例。
        persist : bool
            是否在关闭时保留请求数据。默认为 False。
        flush_on_start : bool
            是否在启动时清空请求数据。默认为 False。
        queue_key : str
            请求队列的键。
        queue_cls : str
            队列类的可导入路径。
        dupefilter : Dupefilter
            自定义去重过滤器实例。
        dupefilter_key : str
            去重过滤器的键。
        dupefilter_cls : str
            去重过滤器类的可导入路径。
        idle_before_close : int
            在放弃前的超时时间。

        """
        if idle_before_close < 0:
            raise TypeError("idle_before_close 不能为负数")

        self.server = server
        self.persist = persist
        self.flush_on_start = flush_on_start
        self.queue_key = queue_key
        self.queue_cls = queue_cls
        self.df = dupefilter
        self.dupefilter_cls = dupefilter_cls
        self.dupefilter_key = dupefilter_key
        self.idle_before_close = idle_before_close
        self.serializer = serializer
        self.stats = None  # 用于存储 Scrapy 的统计信息

    def __len__(self):
        """返回队列的长度"""
        return len(self.queue)

    @classmethod
    def from_settings(cls, settings):
        """根据 Scrapy 配置生成 Scheduler 实例"""
        kwargs = {
            "persist": settings.getbool("SCHEDULER_PERSIST"),
            "flush_on_start": settings.getbool("SCHEDULER_FLUSH_ON_START"),
            "idle_before_close": settings.getint("SCHEDULER_IDLE_BEFORE_CLOSE"),
        }

        # 如果某些配置缺失，则使用默认值。
        optional = {
            "queue_key": "SCHEDULER_QUEUE_KEY",
            "queue_cls": "SCHEDULER_QUEUE_CLASS",
            "dupefilter_key": "SCHEDULER_DUPEFILTER_KEY",
            "dupefilter_cls": "DUPEFILTER_CLASS",
            "serializer": "SCHEDULER_SERIALIZER",
        }
        for name, setting_name in optional.items():
            val = settings.get(setting_name)
            if val:
                kwargs[name] = val

        # 加载去重过滤器类
        dupefilter_cls = load_object(kwargs["dupefilter_cls"])
        if not hasattr(dupefilter_cls, "from_spider"):
            kwargs["dupefilter"] = dupefilter_cls.from_settings(settings)

        # 如果序列化器是字符串路径，则导入相应的模块。
        if isinstance(kwargs.get("serializer"), str):
            kwargs["serializer"] = importlib.import_module(kwargs["serializer"])

        # 从配置中获取 Redis 连接
        server = connection.from_settings(settings)
        # 确保 Redis 连接正常
        server.ping()

        return cls(server=server, **kwargs)

    @classmethod
    def from_crawler(cls, crawler):
        """根据 Scrapy 的 Crawler 实例生成 Scheduler 实例"""
        instance = cls.from_settings(crawler.settings)
        # 为 Scheduler 实例添加统计信息支持
        instance.stats = crawler.stats
        return instance

    def open(self, spider):
        """在爬虫启动时打开 Scheduler"""
        self.spider = spider

        try:
            # 初始化请求队列
            self.queue = load_object(self.queue_cls)(
                server=self.server,
                spider=spider,
                key=self.queue_key % {"spider": spider.name},
                serializer=self.serializer,
            )
        except TypeError as e:
            raise ValueError(
                f"无法实例化队列类 '{self.queue_cls}': {e}"
            )

        # 如果没有自定义的去重过滤器，则从蜘蛛实例中获取
        if not self.df:
            self.df = load_object(self.dupefilter_cls).from_spider(spider)

        # 如果配置为在启动时清空队列，则执行清空操作
        if self.flush_on_start:
            self.flush()

        # 如果队列中已有请求，则记录日志并继续执行这些请求
        if len(self.queue):
            spider.log(f"恢复爬取（{len(self.queue)} 个请求已调度）")

    def close(self, reason):
        """在爬虫关闭时执行的操作"""
        # 如果不保留数据，则清空队列和去重过滤器
        if not self.persist:
            self.flush()

    def flush(self):
        """清空队列和去重过滤器的数据"""
        self.df.clear()
        self.queue.clear()

    def enqueue_request(self, request):
        """将请求加入队列"""
        # 如果请求不需要过滤且已存在于去重过滤器中，则跳过该请求
        if not request.dont_filter and self.df.request_seen(request):
            self.df.log(request, self.spider)
            return False
        # 更新统计信息
        if self.stats:
            self.stats.inc_value("scheduler/enqueued/redis", spider=self.spider)
        # 将请求推入队列
        self.queue.push(request)
        return True

    def next_request(self):
        """从队列中获取下一个请求"""
        block_pop_timeout = self.idle_before_close
        request = self.queue.pop(block_pop_timeout)
        # 更新统计信息
        if request and self.stats:
            self.stats.inc_value("scheduler/dequeued/redis", spider=self.spider)
        return request

    def has_pending_requests(self):
        """判断队列中是否有待处理的请求"""
        return len(self) > 0
