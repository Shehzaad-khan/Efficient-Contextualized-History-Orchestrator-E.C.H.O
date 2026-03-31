import redis
import os
from dotenv import load_dotenv

load_dotenv(r"C:\E.C.H.O\.env")

r = redis.Redis.from_url(
    os.environ["REDIS_URL"],
    decode_responses=True
)
r.set('foo', 'bar')
value = r.get('foo')
print(value)  