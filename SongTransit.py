import glob
import os
import psycopg2
import json



downloaded_song_paths = glob.glob("C:\\Users\\user\\Downloads\\*_ESD_.json")
destination_dir = os.path.dirname(os.path.abspath(__file__)) + "\\SongData\\"


def get_DB_info():
	with open("secret.json") as secret_json:
		data = json.load(secret_json)
		secret_json.close()
		return {"dbname":data["dbname"], "user":data["user"], "host":data["host"], "password":data["password"]}

def move_songs_to_DB():
	db_info = get_DB_info()

	try:
		dbConn = psycopg2.connect("dbname=" + db_info["dbname"] 
								+ " user=" + db_info["user"] 
								+ " host=" + db_info["host"] 
								+ " password=" + db_info["password"] + "")
	except Exception as e:
		print "Unable to connect to Database"
		return

	cur = dbConn.cursor()

	# cur.execute("""SELECT * from songs"")
	# rows = cur.fetchall()

	song_paths = glob.glob("./SongData/*.json")
	for cur_song_path in song_paths:
		with open(cur_song_path) as song_json:
			cur_song_data = json.load(song_json)
			song_name = cur_song_data["file_metadata"]["name"].replace("'", "''") # Changed to "''" to escape '
			song_artist = cur_song_data["file_metadata"]["artist"].replace("'", "''")
			song_genre = cur_song_data["file_metadata"]["genre"].replace("'", "''")

			cur.execute("""SELECT COUNT(*) 
					FROM songs
					WHERE title='{}' and artist='{}' and genre='{}';"""
					.format(song_name, song_artist, song_genre))
			row = cur.fetchone() # Since only count it being returned, fetch one

			if int(row[0]) == 0: # If Empty
				# Insert song data into the DB
				pass



# Move songs from Downloads (where MASV-Map puts them) to this repository's "SongData" directory
def transfer_songs():
	for song_path in downloaded_song_paths:
		song_file_name = song_path.replace("C:\\Users\\user\\Downloads\\", "")
		destination_path = destination_dir + song_file_name
		os.rename(song_path, destination_path)
		print("Moved song: " + song_file_name)

move_songs_to_DB()