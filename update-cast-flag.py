from pymongo import UpdateMany, MongoClient

client = MongoClient()
# client = MongoClient('localhost', 27017)
# client = MongoClient('mongodb://localhost:27017')

db = client.movie_db

movies = db.movies

# Clear cast data flag
movies.bulk_write([UpdateMany({}, {'$set': {'hasCastData': False}})])

# Set cast data flag

for movie in movies.find():
    print('movie {0} {1}'.format(movie['title'], movie['_id']))
    if len(movie['cast']) > 0:
        movie['hasCastData'] = True
        movies.save(movie)
