import os
import redis as redis_client
import dotenv

dotenv.load_dotenv()

# Get Redis connection parameters from environment variables
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_USERNAME = os.getenv("REDIS_USERNAME")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")


redis = redis_client.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    username=REDIS_USERNAME,
    password=REDIS_PASSWORD,
    decode_responses=True,
)


async def connect_redis():
    """Connect to Redis if not already connected."""
    try:
        redis.ping()
    except redis_client.ConnectionError:
        # Connection will be established on first use
        pass
