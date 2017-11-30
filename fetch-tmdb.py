from pymongo import MongoClient
import time
import json
import asyncio
import aiohttp


URL = 'http://api.themoviedb.org/3/'
CONFIG_URL = 'http://api.themoviedb.org/3/configuration'
EXAMPLE = ('https://api.themoviedb.org/3/configuration?'
           'api_key=78ef9d5bb5afba0881ccda78508a961a')
EXAMPLE2 = ('https://api.themoviedb.org/3/movie/32612'
            '?api_key=78ef9d5bb5afba0881ccda78508a961a')
API_KEY = '78ef9d5bb5afba0881ccda78508a961a'
MOVIE = 'movie/'
THUMBNAIL = 'w92'
LARGE = 'w500'
BASE_URL = 'http://image.tmdb.org/t/p/'
KEY_APPEND = '?api_key=' + API_KEY


def updateMovie(data, base_url, movie_id):
    client = MongoClient()
    db = client.movie_db
    movies = db.movies
    movie = movies.find_one({'_id': movie_id})
    updated = False
    if movie is not None:
        updated = True
        movie['posterPath'] = data['poster_path']
        movie['backdropPath'] = data['backdrop_path']
        movie['posterThumbnailHref'] = (base_url + THUMBNAIL
                                        + data['poster_path'])
        movie['posterHref'] = base_url + LARGE + data['poster_path']
        movies.save(movie)
    return updated


async def fetch(pid, session, base_url, movie_id, tmdb_id):
    start = time.time()
    print('Fetch async {} started.'.format(pid))
    req_url = URL + MOVIE + tmdb_id + KEY_APPEND
    # response = await aiohttp.request('GET', req_url)
    async with session.get(req_url) as response:
        text = await response.text()
        data = json.loads(text)
        # print('Response: status: {} text: {}'.format(response.status, text))
        updated = False
        if response.status == 200:
            updated = updateMovie(data, base_url, movie_id)
        else: 
            await asyncio.sleep(3)
        end_time = time.time()
        print('{} fetched in {:0.2f} sec ({})'.format(pid,
              end_time-start, str(updated)))


async def main(cursor, step):
    reqs = []
    async with aiohttp.ClientSession() as session:
        for index in range(1, step+1):
            if cursor.alive:
                movie = cursor.next()
                reqs.append(fetch(index, session, BASE_URL,
                                  movie['_id'], movie['tmdbId']))
                index += 1
        await asyncio.wait(reqs)
    await asyncio.sleep(5)


client = MongoClient()
db = client.movie_db
movies = db.movies
cursor = movies.find({'posterPath': {'$not': {'$exists': True}}})

STEP_SIZE = 20
loop = 1
while cursor.alive:
    print('Loop {} through event loop.'.format(loop))
    ioloop = asyncio.get_event_loop()
    ioloop.run_until_complete(main(cursor, STEP_SIZE))
    loop += 1
ioloop.close()
