import requests
from base64 import b64encode
import pandas as pd
import json
import pyodbc
import configparser
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import date

from utils.get_env import get_env_variable

config = configparser.ConfigParser()
config.read('C:\Spot_Proj\configurations.ini')

client_id = get_env_variable("client_id")
client_secret = get_env_variable("client_secret")
base_url = config.get('spotApi','baseurl')
todays_date = date.today()

# Gets the token and returns the Bearer Token
def get_spotify_access_token(client_id, client_secret):
    """
        Retrieves the access token. 

        Args: client_id (string): Unique identifier of my application. 
              client_secret (string): The key used to autorize WEB api or SDK calls. 

        Returns: Acess token (response.json): Access token used to preform get requests. 
    """
    url = f'{base_url}api/token'
    # Builds the authorization token string
    headers = {
        #https://developer.spotify.com/dashboard/c2e4643825c345e280e849f65ac0b58c/settings
        # If key has been comprimised then regenerate immediatly by clicking "ROTATE" in the app overview page.
        "Authorization": "Basic " + b64encode(f"{client_id}:{client_secret}".encode()).decode(),
    }
    data = {"grant_type": "client_credentials" }

    # Sends the post request to get the token string
    response = requests.post(url, headers=headers, data=data)
    # Parse the json response to just grab the access token token
    access_token = response.json().get("access_token")
  #  print('***Retrieved Access Token***',access_token)
    return access_token

def get_artist_id(access_token, artist_name):
    """
        Function returns the artist ID given the artist name. Necessary to preform following requests.

        Args: access_token (json): Spotify token used to retrieve artist information.
              artist_name (pd.DataFrame): DataFrame of artist names.

        Fetch: artist_id (pd.DataFrame): DataFrame of artist IDs that correspond to the name. 
    """
    # For each records in the df search for the id. 
    for i in artist_name:
        url = f"https://api.spotify.com/v1/search?q={i}&type=artist"
        headers = {"Authorization": f"Bearer {access_token}",}
        # Send the get request to return the id
        response = requests.get(url, headers=headers)
        # Parse the json object to just gather names. 
        artist_id = response.json()["artists"]["items"][0]["id"]
    #print('***Retrieved Artist ID***',artist_id)
    return artist_id

def get_sev_artist_info(access_token, artist_id):
    """
        Gathers Information about the artist

        Args: access_token (json): Spotify token used to retrieve artist information.
              artist_id (pd.DataFrame): DataFrame of artist IDs.

        Fetch: artist_df (pd.DataFrame): DataFrame of artist info.
    """

    

def get_artist_info(access_token, artist_id):
    """
        Gathers Information about the artist

        Args: access_token (json): Spotify token used to retrieve artist information.
              artist_id (pd.DataFrame): DataFrame of artist IDs.

        Fetch: artist_df (pd.DataFrame): DataFrame of artist info.
    """
    # Info to be saved in DBO.ARTIST
    artist_df = pd.DataFrame(columns = ['ID','SpotifyUrl','Followers','Image','Name','Popularity','Type', 'URI'])

    for i in artist_id['ID']:
        print('***i***', i)
        url = f"https://api.spotify.com/v1/artists/{i}"
        headers = {
            "Authorization": f"Bearer {access_token}",
        }
        print('***URL***',url)
        print('***Headers***', headers)
        response = requests.get(url, headers=headers)
        print('***Response***', response)
        track = response.json()
        print(track)
        #print('***API_Data***', API_Data)
        #for track in API_Data:
        #print(track)
        art_row = {'ID' : track['id'],
               'SpotifyUrl' : track['external_urls']['spotify'],
               'Followers' : track['followers']['total'],
               'Image' : track['images'][2]['url'],
               'Name' : track['name'],
               'Popularity' : track['popularity'],
               'Type' : track['type'],
               'URI' : track['uri'],
               'PostDate' : todays_date}
        artist_df.loc[len(artist_df)] = art_row

    print('***Artist Info DF***',artist_df.head(5))
  #  print('***Retrieved Artist Dataframe**')
    #print('Artist ID',artist_df)
  #  print('***Artist DF***', artist_df)
    return artist_df
    # Print json data using loop
   # print(f"Total Number of Followers: {API_Data['followers']['total']}")
        

def get_artist_top_tracks(access_token, artist_id):
    
    url = f"https://api.spotify.com/v1/artists/{artist_id}/top-tracks?country=US"
    headers = {
        "Authorization": f"Bearer {access_token}",
    }

    response = requests.get(url, headers=headers)
    #print(response.json())
    top_tracks = response.json()["tracks"]
 #   print('***Retrieved Top Tracks***')
    return top_tracks


def get_all_tracks(artist_id):
    auth_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(auth_manager=auth_manager)
    # Get all albums
    albums = []
    results = sp.artist_albums(artist_id, album_type='album,single,compilation', limit=50)
    while results:
        albums.extend(results['items'])
        results = sp.next(results) if results['next'] else None

    # Get all track IDs
    track_ids = []
    track_names = []
    album_names = []
    release_dates = []
    popularity = []
    image = []

    for album in albums:
        album_id = album['id']
        album_name = album['name']
        album_tracks = sp.album_tracks(album_id)
        release_dates.append(album['release_date'])


        for track in album_tracks['items']:
            track_ids.append(track['id'])
            track_names.append(track['name'])
            popularity.append(track['popularity'])
            image.append(track['album']["images"][-1]["url"])


    # Store in DataFrame
    track_df = pd.DataFrame({
        'Artist ID' : artist_id,
        'Album ID' : album_id,
        'Album' : album_name,
        'Track ID': track_ids,
        'Track Name': track_names,
        'Album Name': album_names,
        'Release Date': release_dates,
        'Popularity' : popularity,
        'Image' : image
    })

  #  print(track_df.head(20))
 #   print(f"Retrieved {len(track_df)} tracks for Nicki Minaj.")
 #   print(track_df.head())
   # print('***Track DF***', track_df)
  #  print('***Track ID***', track_df['Track ID'].values)
    return track_df





def get_artist_tracks(access_token, track_id):
    url = f"https://api.spotify.com/v1/tracks/{track_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
    }
   # print('***URL***', url)
    response = requests.get(url, headers=headers)
    tracks = response.json()
   # print(tracks)

    return tracks


def connect_db():
    connectionString = 'DRIVER=SQL Server;SERVER=ABBSCPU\SQLEXPRESS;DATABASE=Master;TrustServerCertificate=yes;'
    conn = pyodbc.connect(connectionString)
    print('***Connect to Database***')
    return conn

def import_artist(dd,conn):
    cursor = conn.cursor()

    for index, row in dd.iterrows():
        cursor.execute("INSERT INTO MASTER.DBO.ARTIST (ID, ARTIST_NAME, SPOTIFY_URL, FOLLOWERS, IMAGE, POPULARITY, TYPE, URI, POST_DATE)  VALUES (?,?,?,?,?,?,?,?,?)", row.ID, row.Name, row.SpotifyURL, row.Followers, row.Image, row.Popularity, row.Type, row.URI, row.PostDate)

    conn.commit()
    cursor.close()

def import_song(df,conn):
   cursor = conn.cursor()

   for index, row in df.iterrows():
        cursor.execute("INSERT INTO MASTER.DBO.SONGS (ID, SONG, POPULARITY, IMAGE, RELEASE_DATE, TRACK_ID, ALBUM_NAME)  VALUES (?,?,?,?,?,?,?)", row.ID, row.Song, row.Popularity, row.Image, row.ReleaseDate, row.TrackID, row.AlbumName)
   print('***Import Data to Database***')
   conn.commit()
   cursor.close()

def main():
    # Replace with your Spotify API credentials
    artist_name = { 'Name' : ['Flo Milli','Cardi B','KARRAHBOOO','Rapsody','GloRilla','Ice Spice','Doja Cat','Megan Thee Stallion','Sexyy Red','Nicki Minaj','Doechii']}
    artist_df = pd.DataFrame(artist_name)

    access_token = get_spotify_access_token(client_id, client_secret)


    artist_id_df = pd.DataFrame(columns = ['ID'])
    # Adds Artist ids to a dataframe
    
    for row in artist_df.iterrows():
        artist_id = get_artist_id(access_token, row)
        artist_id_df.loc[len(artist_id_df)] = artist_id
    
    connect = connect_db()

    artist_info_df = get_artist_info(access_token, artist_id_df)
    print('***Artist Info:****',artist_info_df.head(5))
    import_song(artist_info_df,connect)

    for i in artist_info_df['ID']:
        track_df = get_artist_tracks(access_token, i)
        import_song(track_df, connect)

    '''
    dd = pd.concat([artist_df,artist_id_df],axis=1)
    df = pd.DataFrame(columns = ['ID','Song','Popularity','Image','ReleaseDate','TrackID','AlbumName'])
 
    for i, rows in artist_info_df.iterrows():
       # print(rows['ID'])       
        #top_tracks = get_artist_top_tracks(access_token, rows['ID'])
        broad_track = get_all_tracks(rows['ID'])
        for i in broad_track['Track ID']:
            f = get_artist_tracks(access_token, i)
           # print(f['artists'])
      #  print(f"Top tracks for {artist_name}:")
       # for i, track in enumerate(all_tracks, start=1):
        #    print(track)
          #  print(track['name'])
            new_row = {'ID' : rows['ID'],
                       'TrackID' : i,
                       'Song' : f['name'], 
                       'AlbumName': f['album']['name'],
                       'Popularity' : f['popularity'],
                       'Image' : f['album']["images"][-1]["url"],
                       'ReleaseDate' : f['album']["release_date"]}
            df.loc[len(df)] = new_row
            
          #  print(f"{i}. {f['name']} - {f['popularity']}% ")'
          '''

if __name__ == "__main__":
    main()