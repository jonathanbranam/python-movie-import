import csv
from pymongo import UpdateOne, MongoClient

client = MongoClient()
# client = MongoClient('localhost', 27017)
# client = MongoClient('mongodb://localhost:27017')

db = client.movie_db

movies = db.movies
movies.remove()

count = 0
BULK_INSERT_COUNT = 1000
BULK_UPDATE_COUNT = 1000

with open('../downloads/ml-20m/movies.csv') as movieFile:
    movieReader = csv.reader(movieFile)
    next(movieReader, None)
    movieList = []
    for row in movieReader:
        genres = []
        if row[2] != '(no genres listed)':
            genres = row[2].split('|')

        movie_data = {
            'ml_id': row[0],
            'ml_title': row[1],
            'ml_genres': genres
        }
        movieList.append(movie_data)
        count += 1
        if len(movieList) >= BULK_INSERT_COUNT:
            print('Inserting {0} ({1}) movies...'
                  .format(len(movieList), count))
            movies.insert_many(movieList)
            movieList = []
        # print(movie_data)
    if len(movieList) > 0:
        print('Inserting {0} ({1}) movies...'
              .format(len(movieList), count))
        movies.insert_many(movieList)

print('Imported {0} total movies...'.format(count))

update_count = 0
with open('../downloads/ml-20m/links.csv') as linkFile:
    linkReader = csv.reader(linkFile)
    next(linkReader, None)
    linkList = []
    updateList = []
    for row in linkReader:
        movie = movies.find_one({'ml_id': row[0]})
        if movie is not None:
            update_count += 1
            # movie['imdb_id'] = row[1]
            # movie['tmdb_id'] = row[2]
            # movies.save(movie)
            updateList.append(UpdateOne(
                {'_id': movie['_id']},
                {'$set': {
                    'imdb_id': row[1],
                    'tmdb_id': row[2]
                }}
            ))

        if len(updateList) >= BULK_UPDATE_COUNT:
            print('Updating {0} ({1} / {2}) movies...'
                  .format(len(updateList), update_count, count))
            # print(updateList)
            movies.bulk_write(updateList, ordered=False)
            updateList = []
    if len(updateList) > 0:
        print('Updating {0} ({1} / {2}) movies...'
              .format(len(updateList), update_count, count))
        movies.bulk_write(updateList, ordered=False)


print('Updated {0} total movies...'.format(update_count))
