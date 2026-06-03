import redis
import os
from dotenv import load_dotenv

from backend.security import validate_redis_tls_url

load_dotenv(r"C:\E.C.H.O\.env")

r = redis.Redis.from_url(
    validate_redis_tls_url(os.environ["REDIS_URL"]),
    decode_responses=True
)
r.set('foo', 'bar')
value = r.get('foo')
print(value)  
