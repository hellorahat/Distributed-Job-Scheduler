import redis

r = redis.Redis(
    host="localhost",
    port=6379,
    decode_responses=True
)


def get_redis():
    return r
