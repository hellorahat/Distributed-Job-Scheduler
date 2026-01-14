from redis import Redis

r = Redis(
    host="localhost",
    port=6379,
    decode_responses=True
)


def get_redis() -> Redis:
    return r
