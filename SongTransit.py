import glob
import os

downloaded_song_paths = glob.glob("C:\\Users\\user\\Downloads\\*_ESD_.json")
destination_dir = os.path.dirname(os.path.abspath(__file__)) + "\\SongData\\"

for song_path in downloaded_song_paths:
	song_file_name = song_path.replace("C:\\Users\\user\\Downloads\\", "")
	destination_path = destination_dir + song_file_name
	os.rename(song_path, destination_path)
	print("Moved song: " + song_file_name)
