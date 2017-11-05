import glob
import os
import psycopg2,psycopg2.extras
import json
import timeit

try:
    # Python2
    from cStringIO import StringIO 
except ImportError:
    # Python3
    from io import StringIO


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
		print("Unable to connect to Database")
		return

	curs = dbConn.cursor()

	# curs.execute("""SELECT * from songs"")
	# rows = curs.fetchall()

	removals = []

	song_paths = glob.glob("./SongData/*.json")
	for cur_song_path in song_paths:
		with open(cur_song_path) as song_json:
			start_time = timeit.default_timer()

			cur_song_data = json.load(song_json)
			song_name = cur_song_data["file_metadata"]["name"].replace("'", "''") # Changed to "''" to escape '
			song_artist = cur_song_data["file_metadata"]["artist"].replace("'", "''")
			song_genre = cur_song_data["file_metadata"]["genre"].replace("'", "''")


			# Check if the current song exists in Database
			curs.execute("""SELECT COUNT(*) 
					FROM songs
					WHERE title=%s and artist=%s and genre=%s;""",
					(song_name, song_artist, song_genre))
			count = int(curs.fetchone()[0]) # Since only count is being returned, fetch one

			if count == 0: # If song does not already exist in DB
				# Insert song data into the DB
				song_id = db_insert_song(curs, song_name, song_artist, song_genre)
				print("Inserted song -> " + song_name)

				rec_freq = cur_song_data["record_metadata"]["frequency"]
				rec_down_scale = cur_song_data["record_metadata"]["down_scale"]
				rec_force_stop = cur_song_data["forced_stop"]
				rec_id = db_insert_recording(curs, rec_freq, rec_down_scale, rec_force_stop, song_id)
				print("Inserted recording freq -> " + str(rec_freq))

				rec_vis_data = cur_song_data["song_visual_data"]
				db_insert_spectrum(curs, rec_vis_data, rec_id)
				print("Inserted spectrum!")

				dbConn.commit() # After all records have been inserted, commit transation
				print("Transaction COMMITTED!")
			else:
				# Maybe remove the .json?
				print("Song already complete -> " + song_name + " -- removing file")
				removals.append(cur_song_path)

			elapsed = timeit.default_timer() - start_time
			print("Time elapsed -> " + str(elapsed))
			print("	------")

	print("Transition complete ~ closing DB cursor and connection")
	# Close communicatio with the Database
	curs.close()
	dbConn.close()

	print("Removing {} files that already exist in the DB".format(len(removals)))
	remove_files(removals)



# Run SQL commands to insert a song record into the DB
# Return the ID of record after insertion
def db_insert_song(curs, title, artist, genre):
	curs.execute("""INSERT INTO songs (title, genre, artist)
					VALUES (%s, %s, %s)
					RETURNING pk_song""", (title, genre, artist))
	rec_id = int(curs.fetchone()[0])
	return rec_id

# Run SQL commands to insert a recording record into the DB
# With the songID as it's parent
# Return the ID of the record after insertion
def db_insert_recording(curs, frequency, down_scale, forced_stop, song_id):
	# Always insert a 's' type
	curs.execute("""INSERT INTO recordings (frequency, down_scale, forced_stop, song_id)
					VALUES (%s, %s, %s, %s)
					RETURNING pk_recording""", (frequency, down_scale, forced_stop, song_id)) 
	rec_id = int(curs.fetchone()[0])
	return rec_id

# Run SQL commands to insert a spectrum record into the DB
# With the recordingID as it's parent
# Return nothing, since mass-insertion is being done
def db_insert_spectrum(curs, visual_data, recording_id):
	st = timeit.default_timer()

	f = StringIO()
	# visual_data is a 2D array (a nx63 matrix)
	values_list = []
	for rowIndex, rowData in enumerate(visual_data):
		items = []
		for colIndex, colData in enumerate(rowData): 
			value = (rowIndex, colIndex, colData, recording_id)
			items.append('\t'.join(map(str, value)) + '\n')
		f.writelines(items)
	f.seek(0)

	e = timeit.default_timer() - st
	print(" Loop-iteration time: " + str(e))

	curs.copy_from(f, 'spectrums', columns=('row', 'col', 'value', 'recording_id'))

# os.remove each file path in list
def remove_files(r_list):
	for file_path in r_list:
		os.remove(file_path)

# Move songs from Downloads (where MASV-Map puts them) to this repository's "SongData" directory
def transfer_songs():
	for song_path in downloaded_song_paths:
		# Note: Currently Windows path 
		song_file_name = song_path.replace("C:\\Users\\user\\Downloads\\", "")
		destination_path = destination_dir + song_file_name
		os.rename(song_path, destination_path)
		print("Moved song: " + song_file_name)

move_songs_to_DB()