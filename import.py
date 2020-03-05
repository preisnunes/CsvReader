from csvReader import CsvReader

columnsNamesToExtract = [
	'director_name',
	'actor_2_name',
	'actor_1_name',
	'actor_3_name',
	'gross',
	'genres',
	'movie_title',
	'num_voted_users',
    'imdb_score'
]

csvReader = CsvReader('dataset.csv', columnsNamesToExtract)
csvReader.setChunkSize(2)
csvIterator = iter(csvReader)


for movies in csvIterator:
    for movie in movies:
        print(movie['movie_title'])
    print("\n")

