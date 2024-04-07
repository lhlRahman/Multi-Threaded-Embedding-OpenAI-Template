import pymongo
import openai
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from threading import Semaphore

uri = "your conntection uri"

client = pymongo.MongoClient(uri)
db  = client.sample_mflix
collection  = db.movies


openai.api_key = 'your key'



class RateLimiter:
    def __init__(self, calls_per_minute):
        self.calls_per_minute = calls_per_minute
        self.semaphore = Semaphore(calls_per_minute)
        self.time_reset = time.time() + 60

    def wait_and_acquire(self):
        with self.semaphore:
            current_time = time.time()
            if current_time >= self.time_reset:
                self.semaphore = Semaphore(self.calls_per_minute)  # Reset the semaphore for the new minute
                self.time_reset = current_time + 60
            time_to_wait = self.time_reset - current_time
            # Calculate sleep time to distribute remaining calls over the time left in the minute
            sleep_time = time_to_wait / self.semaphore._value if self.semaphore._value > 0 else time_to_wait
            time.sleep(sleep_time)

def get_embedding(text, rate_limiter):
    rate_limiter.wait_and_acquire()
    response = openai.embeddings.create(
        input=text,
        model="text-embedding-ada-002",
    )
    if response and response.data:
        embedding_data = response.data[0].embedding
        if embedding_data:
            return embedding_data
        else:
            raise Exception("No data found in response")
    else:
        raise Exception("Failed to get embedding")

def update_movie(movie, rate_limiter):
    try:
        movie['plot_embedding'] = get_embedding(movie['plot'], rate_limiter)
        collection.replace_one({'_id': movie['_id']}, movie)
        print(f"Updated: {movie['title']}")
    except Exception as e:
        print(f"Failed to update {movie['title']}: {e}")

# Initialize the rate limiter with 3000 requests per minute limit
rate_limiter = RateLimiter(3000)

# Retrieve movies that require updating
movies = list(collection.find({'plot': {'$exists': True}, 'plot_embedding': {'$exists': False}}))
print(len(movies))

# Update movies using a ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=100) as executor:
    futures = [executor.submit(update_movie, movie, rate_limiter) for movie in movies]
    for future in as_completed(futures):
        future.result()

print('All Done!')
