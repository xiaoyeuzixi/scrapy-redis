try:
    from scrapy.utils.request import request_from_dict
except ImportError:
    from scrapy.utils.reqser import request_to_dict, request_from_dict # type: ignore

from . import picklecompat


class Base:
    """每个爬虫的基础队列类"""

    def __init__(self, server, spider, key, serializer=None):
        """初始化每个爬虫的 Redis 队列。

        参数
        ----------
        server : StrictRedis
            Redis 客户端实例。
        spider : Spider
            Scrapy 爬虫实例。
        key: str
            Redis 中用于存放和获取消息的键。
        serializer : object
            具有 ``loads`` 和 ``dumps`` 方法的序列化对象。

        """
        if serializer is None:
            # 向后兼容。
            # TODO: 弃用 pickle。
            serializer = picklecompat
        if not hasattr(serializer, "loads"):
            raise TypeError(
                f"serializer 没有实现 'loads' 函数: {serializer}"
            )
        if not hasattr(serializer, "dumps"):
            raise TypeError(
                f"serializer 没有实现 'dumps' 函数: {serializer}"
            )

        self.server = server    # Redis 客户端实例。
        self.spider = spider    # Scrapy 爬虫实例。
        self.key = key % {"spider": spider.name}  # Redis 中用于存放和获取消息的键。
        self.serializer = serializer  # 序列化对象。

    def _encode_request(self, request):
        """编码请求对象"""
        try:
            obj = request.to_dict(spider=self.spider)
        except AttributeError:
            obj = request_to_dict(request, self.spider)
        return self.serializer.dumps(obj)

    def _decode_request(self, encoded_request):
        """解码之前编码的请求"""
        obj = self.serializer.loads(encoded_request)
        return request_from_dict(obj, spider=self.spider)
    

    def __len__(self):
        """返回队列的长度"""
        raise NotImplementedError

    def push(self, request):
        """推送一个请求"""
        raise NotImplementedError

    def pop(self, timeout=0):
        """弹出一个请求"""
        raise NotImplementedError

    def clear(self):
        """清空队列/栈"""
        self.server.delete(self.key)




class FifoQueue(Base):
    """每个爬虫的 FIFO 队列"""

    def __len__(self):
        """返回队列的长度"""
        return self.server.llen(self.key)

    def push(self, request):
        """推送一个请求"""
        self.server.lpush(self.key, self._encode_request(request))

    def pop(self, timeout=0):
        """弹出一个请求"""
        if (timeout > 0):
            data = self.server.brpop(self.key, timeout)
            if isinstance(data, tuple):
                data = data[1]
        else:
            data = self.server.rpop(self.key)
        if data:
            return self._decode_request(data)



class PriorityQueue(Base):
    """使用 Redis 的排序集合实现的每个爬虫的优先级队列抽象"""

    def __len__(self):
        """返回队列的长度"""
        return self.server.zcard(self.key)

    def push(self, request):
        """推送一个请求"""
        data = self._encode_request(request)
        score = -request.priority
        # 不使用 zadd 方法，因为参数的顺序取决于类是 Redis 还是 StrictRedis，
        # 并且使用 kwargs 选项只接受字符串而非字节。
        self.server.execute_command("ZADD", self.key, score, data)

    def pop(self, timeout=0):
        """
        弹出一个请求
        timeout 在此队列类中不支持
        """
        # 使用 multi/exec 实现的原子范围/删除操作
        pipe = self.server.pipeline()
        pipe.multi()
        pipe.zrange(self.key, 0, 0).zremrangebyrank(self.key, 0, 0)
        results, count = pipe.execute()
        if results:
            return self._decode_request(results[0])



class LifoQueue(Base):
    """每个爬虫的 LIFO 队列。"""

    def __len__(self):
        """返回栈的长度"""
        return self.server.llen(self.key)

    def push(self, request):
        """推送一个请求"""
        self.server.lpush(self.key, self._encode_request(request))

    def pop(self, timeout=0):
        """弹出一个请求"""
        if timeout > 0:
            data = self.server.blpop(self.key, timeout)
            if isinstance(data, tuple):
                data = data[1]
        else:
            data = self.server.lpop(self.key)

        if data:
            return self._decode_request(data)



# TODO: 弃用这些名称。
SpiderQueue = FifoQueue
SpiderStack = LifoQueue
SpiderPriorityQueue = PriorityQueue

