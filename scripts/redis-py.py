import sys
from pathlib import Path

# Add project root (C:\E.C.H.O) to search path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

import os
import redis
from dotenv import load_dotenv

from ste.security import validate_redis_tls_url

load_dotenv(PROJECT_ROOT / ".env")

r = redis.Redis.from_url(
    validate_redis_tls_url(os.environ["REDIS_URL"]),
    decode_responses=True
)
r.set('foo', 'bar')
value = r.get('foo')
print(value)  
