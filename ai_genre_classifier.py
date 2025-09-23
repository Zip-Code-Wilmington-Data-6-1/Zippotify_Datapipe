# import openai  # Uncomment when you want to use OpenAI
import time
import json
from database import SessionLocal
from sqlalchemy import text
from models import DimGenre, DimSongGenre
import os
from dotenv import load_dotenv

load_dotenv()

# You'll need to set OPENAI_API_KEY in your .env file and install openai
# openai.api_key = os.getenv("OPENAI_API_KEY")

def classify_genres_with_llm(song_title, artist_name):
    """Use LLM to classify song genres based on title and artist"""
    
    prompt = f"""
    Given the song title "{song_title}" by artist "{artist_name}", predict the most likely music genres.
    
    Return ONLY a JSON list of 1-3 most likely genres from this list:
    ["rock", "pop", "hip-hop", "rap", "country", "jazz", "blues", "electronic", "dance", 
     "classical", "folk", "metal", "punk", "reggae", "r&b", "soul", "funk", "indie", 
     "alternative", "acoustic", "soundtrack", "ambient", "house", "techno", "dubstep",
     "gospel", "latin", "world", "experimental", "instrumental"]
    
    Example response: ["rock", "alternative"]
    
    Response:
    """
    
    try:
        # Using OpenAI GPT (you'd need to uncomment and configure)
        # response = openai.ChatCompletion.create(
        #     model="gpt-3.5-turbo",
        #     messages=[{"role": "user", "content": prompt}],
        #     max_tokens=100,
        #     temperature=0.3
        # )
        # genres = json.loads(response.choices[0].message.content)
        
        # For demo - rule-based classification
        genres = rule_based_genre_classification(song_title, artist_name)
        return genres
        
    except Exception as e:
        print(f"Error classifying {song_title}: {e}")
        return []

def rule_based_genre_classification(song_title, artist_name=None):
    """Rule-based genre classification with improved diversity"""
    genres = []
    
    # Convert to lowercase for matching
    title_lower = song_title.lower()
    artist_lower = artist_name.lower() if artist_name else ""
    text = f"{title_lower} {artist_lower}".strip()
    
    # Enhanced genre keywords and patterns for better diversity
    # Note: Order matters! More specific genres first, pop keywords made much more restrictive
    genre_keywords = {
        # Specific genres first to catch before pop
        "hip-hop": ["rap", "hip hop", "feat.", "nigga", "gangsta", "trap", "mc ", "dj ", "crew", "posse", "freestyle", "cipher", "spittin", "bars", "rhyme"],
        "metal": ["metal", "heavy", "death", "black", "thrash", "doom", "brutal", "headbang", "scream", "growl", "shred"],
        "electronic": ["electronic", "techno", "house", "dance", "remix", " mix", "beat", "synth", "edm", "trance", "dubstep", "rave", "club"],
        "rock": ["rock", "guitar", "riff", "band", "grunge", "alternative", "garage", "indie rock", "punk rock", "hard rock"],
        "jazz": ["jazz", "blues", "swing", "bebop", "smooth", "fusion", "improvisation", "saxophone", "trumpet", "bebop"],
        "classical": ["classical", "symphony", "orchestra", "piano", "violin", "concerto", "sonata", "chamber", "baroque", "mozart", "beethoven"],
        "r&b": ["r&b", "soul", "motown", "rhythm", "groove", "smooth", "neo soul", "rnb", "soulful"],
        "country": ["country", "nashville", "cowboy", "rural", "honky", "bluegrass", "fiddle", "banjo", "americana", "western"],
        "reggae": ["reggae", "jamaica", "rasta", "ska", "dancehall", "caribbean", "island", "dub", "marley"],
        "folk": ["folk", "acoustic", "traditional", "singer songwriter", "storytelling", "campfire", "unplugged", "bluegrass"],
        "gospel": ["gospel", "church", "praise", "worship", "jesus", "god", "spiritual", "choir", "hymn", "christian", "holy"],
        "latin": ["latin music", "salsa", "mariachi", "cumbia", "reggaeton", "bachata", "merengue", "tropical", "latino", "spanish music", "música"],
        "funk": ["funk", "groove", "bass", "slap", "tight", "pocket", "rhythm section", "funky", "groovy"],
        "punk": ["punk", "hardcore", "riot", "anarchy", "diy", "fast", "aggressive", "rebellious", "mosh", "slam"],
        "indie": ["indie", "independent", "underground", "lo-fi", "bedroom", "artsy", "experimental", "alternative"],
        "ambient": ["ambient", "chill", "meditation", "atmospheric", "ethereal", "drone", "soundscape", "relaxing"],
        "soundtrack": ["soundtrack", "theme", "score", "movie", "film", "cinema", "television", "tv", "game", "main title"],
        "country": ["country", "nashville", "cowboy", "rural", "honky", "bluegrass", "fiddle", "banjo", "americana", "western"],
        # Pop keywords made MUCH more restrictive - only obvious pop indicators
        "pop": ["pop music", "pop song", "pop hit", "top 40", "billboard", "mainstream pop", "radio pop"]
    }
    
    # Known artists by genre - organized alphabetically within each genre
    artist_genres = {
        # === POP ARTISTS ===
        "adele": ["pop", "soul"],
        "ariana grande": ["pop", "r&b"],
        "backstreet boys": ["pop"],
        "billie eilish": ["pop", "alternative"],
        "britney spears": ["pop"],
        "bruno mars": ["pop", "funk"],
        "camila cabello": ["pop", "latin"],
        "christina aguilera": ["pop"],
        "coldplay": ["rock", "pop"],
        "demi lovato": ["pop"],
        "doja cat": ["pop", "hip-hop"],
        "dua lipa": ["pop", "dance"],
        "ed sheeran": ["pop", "folk"],
        "halsey": ["pop", "alternative"],
        "imagine dragons": ["rock", "pop"],
        "john mayer": ["pop", "rock"],
        "jonas brothers": ["pop"],
        "justin bieber": ["pop", "r&b"],
        "katy perry": ["pop"],
        "kesha": ["pop"],
        "lady gaga": ["pop", "dance"],
        "lana del rey": ["pop", "indie"],
        "lorde": ["pop", "alternative"],
        "madonna": ["pop"],
        "maroon 5": ["pop", "rock"],
        "miley cyrus": ["pop"],
        "nsync": ["pop"],
        "olivia rodrigo": ["pop", "rock"],
        "one direction": ["pop"],
        "onerepublic": ["pop", "rock"],
        "pink": ["pop", "rock"],
        "post malone": ["hip-hop", "pop"],
        "rihanna": ["pop", "r&b"],
        "selena gomez": ["pop"],
        "shawn mendes": ["pop", "rock"],
        "sia": ["pop", "electronic"],
        "taylor swift": ["pop", "country"],
        "the chainsmokers": ["electronic", "pop"],
        "the weeknd": ["pop", "r&b"],
        "we the kings": ["pop", "rock"],
        
        # === ROCK ARTISTS ===
        "ac/dc": ["rock"],
        "ac dc": ["rock"],  # without slash
        "acdc": ["rock"],  # no spaces
        "aerosmith": ["rock"],
        "alice in chains": ["rock"],
        "arctic monkeys": ["rock"],
        "band of horses": ["indie", "rock"],
        "the beatles": ["rock", "pop"],
        "coldplay": ["rock", "pop"],
        "deep purple": ["rock"],
        "the eagles": ["rock", "country"],
        "eagles": ["rock", "country"],  # without "the"
        "fall out boy": ["rock", "pop"],
        "fleetwood mac": ["rock", "pop"],
        "florence + the machine": ["rock", "indie"],
        "foo fighters": ["rock"],
        "green day": ["rock", "punk"],
        "guns n' roses": ["rock"],
        "guns n roses": ["rock", "metal"],  # without apostrophe
        "guns and roses": ["rock", "metal"],  # spelled out
        "gnr": ["rock", "metal"],  # abbreviated
        "jethro tull": ["rock"],
        "kings of leon": ["rock"],
        "led zeppelin": ["rock"],
        "led zep": ["rock"],  # abbreviated
        "zeppelin": ["rock"],  # short form
        "linkin park": ["rock", "metal"],
        "mumford & sons": ["folk", "rock"],
        "muse": ["rock"],
        "my chemical romance": ["rock", "alternative"],
        "mcr": ["rock", "alternative"],  # abbreviation
        "nirvana": ["rock"],
        "panic! at the disco": ["rock", "pop"],
        "panic at the disco": ["rock", "pop"],  # without exclamation
        "pearl jam": ["rock"],
        "pink floyd": ["rock"],
        "the pink floyd": ["rock"],  # early name variation
        "queen": ["rock"],
        "radiohead": ["rock", "alternative"],
        "red hot chili peppers": ["rock"],
        "the rolling stones": ["rock"],
        "the stones": ["rock"],  # nickname
        "stones": ["rock"],  # short
        "soundgarden": ["rock"],
        "stone temple pilots": ["rock"],
        "the strokes": ["rock"],
        "system of a down": ["rock", "metal"],
        "twenty one pilots": ["pop", "rock"],
        "21 pilots": ["pop", "rock"],  # alternate spelling
        "twenty øne piløts": ["pop", "rock"],  # stylized
        "two door cinema club": ["indie", "rock"],
        "uriah heep": ["rock"],
        "the white stripes": ["rock"],
        "the who": ["rock"],
        
        # Beatles variations
        "the fabs": ["rock", "pop"],  # nickname
        "fab four": ["rock", "pop"],  # nickname
        
        # === HIP-HOP/RAP ARTISTS ===
        "21 savage": ["hip-hop"],
        "a$ap rocky": ["hip-hop"],
        "a tribe called quest": ["hip-hop"],
        "atcq": ["hip-hop"],  # abbreviation
        "afrika bambaataa": ["hip-hop", "electronic"],
        "beastie boys": ["hip-hop", "rock"],
        "biggie": ["hip-hop"],
        "black eyed peas": ["hip-hop", "pop"],
        "the black eyed peas": ["hip-hop", "pop"],
        "bone thugs-n-harmony": ["hip-hop"],
        "bone thugs n harmony": ["hip-hop"],  # without hyphens
        "btnh": ["hip-hop"],  # abbreviation
        "cardi b": ["hip-hop"],
        "chance the rapper": ["hip-hop"],
        "childish gambino": ["hip-hop"],
        "cypress hill": ["hip-hop"],
        "de la soul": ["hip-hop"],
        "dr. dre": ["hip-hop"],
        "drake": ["hip-hop", "rap"],
        "eminem": ["hip-hop"],
        "future": ["hip-hop"],
        "grandmaster flash": ["hip-hop"],
        "ice cube": ["hip-hop"],
        "j cole": ["hip-hop", "rap"],
        "jay-z": ["hip-hop"],
        "kanye west": ["hip-hop"],
        "kendrick lamar": ["hip-hop", "rap"],
        "lauryn hill": ["r&b", "hip-hop"],
        "lil nas x": ["hip-hop"],
        "lil wayne": ["hip-hop", "rap"],
        "megan thee stallion": ["hip-hop", "rap"],
        "nas": ["hip-hop"],
        "nicki minaj": ["hip-hop"],
        "nwa": ["hip-hop"],
        "n.w.a": ["hip-hop"],
        "n.w.a.": ["hip-hop"],
        "outkast": ["hip-hop"],
        "out kast": ["hip-hop"],  # alternate spelling
        "prophet posse": ["hip-hop", "rap"],
        "public enemy": ["hip-hop"],
        "run-dmc": ["hip-hop"],
        "snoop dogg": ["hip-hop"],
        "travis scott": ["hip-hop"],
        "tupac": ["hip-hop"],
        "tyler the creator": ["hip-hop"],
        "wu-tang clan": ["hip-hop"],
        "wu tang clan": ["hip-hop"],  # without hyphens
        "wu-tang": ["hip-hop"],  # shortened
        "wutang": ["hip-hop"],  # no spaces/hyphens
        
        # === COUNTRY ARTISTS ===
        "alan jackson": ["country"],
        "blake shelton": ["country"],
        "brad paisley": ["country"],
        "brooks & dunn": ["country"],
        "brooks and dunn": ["country"],
        "carrie underwood": ["country"],
        "chris stapleton": ["country"],
        "the chicks": ["country"],  # renamed Dixie Chicks
        "dan + shay": ["country"],
        "dan and shay": ["country"],
        "dixie chicks": ["country"],
        "dolly parton": ["country"],
        "faith hill": ["country"],
        "florida georgia line": ["country"],
        "garth brooks": ["country"],
        "george strait": ["country"],
        "hank williams": ["country"],
        "jason aldean": ["country"],
        "johnny cash": ["country"],
        "the judds": ["country"],
        "kacey musgraves": ["country"],
        "keith urban": ["country"],
        "kenny chesney": ["country"],
        "lady antebellum": ["country"],
        "lady a": ["country"],  # renamed Lady Antebellum
        "little big town": ["country"],
        "luke bryan": ["country"],
        "maren morris": ["country"],
        "merle haggard": ["country"],
        "miranda lambert": ["country"],
        "old dominion": ["country"],
        "radney foster": ["country"],
        "rascal flatts": ["country"],
        "shania twain": ["country"],
        "sugarland": ["country"],
        "thomas rhett": ["country"],
        "tim mcgraw": ["country"],
        "toby keith": ["country"],
        "waylon jennings": ["country"],
        "willie nelson": ["country"],
        "zac brown band": ["country"],
        
        # Country variations
        "big & rich": ["country"],
        "big and rich": ["country"],
        "montgomery gentry": ["country"],
        
        # === ELECTRONIC/EDM ARTISTS ===
        "above & beyond": ["electronic", "dance"],
        "above and beyond": ["electronic", "dance"],
        "aphex twin": ["electronic"],
        "armin van buuren": ["electronic"],
        "avicii": ["electronic", "dance"],
        "boards of canada": ["electronic"],
        "calvin harris": ["electronic", "dance"],
        "chemical brothers": ["electronic"],
        "cosmic gate": ["electronic"],
        "daft punk": ["electronic"],
        "david guetta": ["electronic"],
        "deadmau5": ["electronic"],
        "diplo": ["electronic"],
        "disclosure": ["dance", "electronic"],
        "eric prydz": ["dance", "electronic"],
        "fatboy slim": ["electronic"],
        "flume": ["electronic"],
        "justice": ["electronic"],
        "kraftwerk": ["electronic"],
        "major lazer": ["electronic"],
        "marshmello": ["electronic", "dance"],
        "moby": ["electronic"],
        "odesza": ["electronic"],
        "porter robinson": ["electronic"],
        "prodigy": ["electronic"],
        "skream": ["electronic", "dubstep"],
        "skrillex": ["electronic", "dubstep"],
        "swedish house mafia": ["electronic", "dance"],
        "shm": ["electronic", "dance"],  # abbreviation
        "tiesto": ["electronic"],
        "underworld": ["electronic"],
        "zedd": ["electronic", "dance"],
        
        # Electronic collaborations
        "diplo & skrillex": ["electronic"],
        "skrillex & diplo": ["electronic"],
        "jack u": ["electronic"],  # Skrillex & Diplo duo
        "jack ü": ["electronic"],  # stylized version
        "armin van buuren feat.": ["electronic"],
        "tiesto feat.": ["electronic"],
        "calvin harris feat.": ["electronic", "dance"],
        "david guetta feat.": ["electronic", "dance"],
        "major lazer feat. mo": ["electronic"],
        
        # === JAZZ ARTISTS ===
        "billie holiday": ["jazz"],
        "charlie parker": ["jazz"],
        "chick corea & return to forever": ["jazz"],
        "dave brubeck quartet": ["jazz"],
        "duke ellington": ["jazz"],
        "ella fitzgerald": ["jazz"],
        "john coltrane": ["jazz"],
        "john coltrane quartet": ["jazz"],
        "louis armstrong": ["jazz"],
        "mahavishnu orchestra": ["jazz"],
        "miles davis": ["jazz", "blues"],
        "miles davis quintet": ["jazz"],
        "mjq": ["jazz"],  # Modern Jazz Quartet abbreviation
        "modern jazz quartet": ["jazz"],
        "return to forever": ["jazz"],
        "thelonious monk": ["jazz"],
        "weather report": ["jazz"],
        "davis coltrane": ["jazz"],  # appears to be an error in original
        
        # === R&B/SOUL ARTISTS ===
        "alicia keys": ["r&b"],
        "anderson .paak": ["r&b", "hip-hop"],
        "aretha franklin": ["r&b", "soul"],
        "beyoncé": ["pop", "r&b"],
        "boyz ii men": ["r&b"],
        "boyz 2 men": ["r&b"],  # alternate spelling
        "chris brown": ["r&b", "pop"],
        "d'angelo": ["r&b", "soul"],
        "destiny's child": ["r&b", "pop"],
        "destinys child": ["r&b", "pop"],  # without apostrophe
        "dream": ["r&b", "pop"],
        "en vogue": ["r&b"],
        "erykah badu": ["r&b", "soul"],
        "frank ocean": ["r&b"],
        "janet jackson": ["r&b", "pop"],
        "john legend": ["r&b", "soul"],
        "marvin gaye": ["r&b", "soul"],
        "mary j. blige": ["r&b"],
        "maxwell": ["r&b", "soul"],
        "michael jackson": ["r&b", "pop"],
        "prince": ["r&b", "pop"],
        "stevie wonder": ["r&b", "soul"],
        "sza": ["r&b", "pop"],
        "usher": ["r&b", "pop"],
        "whitney houston": ["r&b", "pop"],
        
        # R&B groups with variations
        "3lw": ["r&b", "pop"],
        "702": ["r&b"],
        "s.w.v.": ["r&b"],
        "swv": ["r&b"],
        "t.l.c.": ["r&b", "pop"],
        "tlc": ["r&b", "pop"],
        "total": ["r&b"],
        "xscape": ["r&b"],
        
        # === REGGAE ARTISTS ===
        "bob marley": ["reggae"],
        "bob marley & the wailers": ["reggae"],
        "bob marley and the wailers": ["reggae"],  # spelled out
        "bunny wailer": ["reggae"],
        "burning spear": ["reggae"],
        "cornel campbell": ["reggae"],
        "damian marley": ["reggae"],
        "inner circle": ["reggae"],
        "jimmy cliff": ["reggae"],
        "lee perry": ["reggae"],
        "marley": ["reggae"],  # just surname
        "melody makers": ["reggae"],
        "peter tosh": ["reggae"],
        "steel pulse": ["reggae"],
        "stephen marley": ["reggae"],
        "third world": ["reggae"],
        "toots and the maytals": ["reggae"],
        "the wailers": ["reggae"],  # just the band
        "wailers": ["reggae"],  # without "the"
        "ziggy marley": ["reggae"],
        "ziggy marley & the melody makers": ["reggae"],
        
        # === METAL ARTISTS ===
        "anthrax": ["metal"],
        "black sabbath": ["metal"],
        "children of bodom": ["metal"],
        "dream theater": ["metal"],
        "gojira": ["metal"],
        "iron maiden": ["metal"],
        "judas priest": ["metal"],
        "korn": ["metal"],
        "limp bizkit": ["metal"],
        "mastodon": ["metal"],
        "megadeth": ["metal"],
        "metallica": ["rock", "metal"],
        "opeth": ["metal"],
        "pantera": ["metal"],
        "rage against the machine": ["metal"],
        "rammstein": ["metal"],
        "slayer": ["metal"],
        "tool": ["metal"],
        
        # === PUNK ARTISTS ===
        "bad religion": ["punk"],
        "black flag": ["punk"],
        "the clash": ["punk"],
        "dead kennedys": ["punk"],
        "minor threat": ["punk"],
        "nofx": ["punk"],
        "the offspring": ["punk"],
        "pennywise": ["punk"],
        "the ramones": ["punk"],
        "rancid": ["punk"],
        "sex pistols": ["punk"],
        "social distortion": ["punk"],
        
        # === FOLK ARTISTS ===
        "ani difranco": ["folk"],
        "bob dylan": ["folk"],
        "bon iver": ["folk"],
        "carole king": ["folk"],
        "cat stevens": ["folk"],
        "crosby stills nash": ["folk", "rock"],
        "crosby, stills & nash": ["folk", "rock"],
        "csn": ["folk", "rock"],  # abbreviation
        "crosby stills nash & young": ["folk", "rock"],
        "csny": ["folk", "rock"],  # with Young
        "denison witmer": ["indie", "folk"],
        "fleet foxes": ["folk"],
        "garfunkel and simon": ["folk"],  # reversed
        "iron & wine": ["folk"],
        "james taylor": ["folk"],
        "joni mitchell": ["folk"],
        "leonard cohen": ["folk"],
        "the lumineers": ["folk", "indie"],
        "the mamas & the papas": ["folk", "pop"],
        "mamas and papas": ["folk", "pop"],  # without "the"
        "neil young": ["folk"],
        "nick drake": ["folk"],
        "of monsters and men": ["indie", "folk"],
        "peter paul and mary": ["folk"],
        "peter, paul & mary": ["folk"],  # with commas
        "simon and garfunkel": ["folk"],
        "simon & garfunkel": ["folk"],  # with ampersand
        "sufjan stevens": ["folk"],
        "suzanne vega": ["folk"],
        "tracy chapman": ["folk"],
        
        # === INDIE ARTISTS ===
        "arcade fire": ["indie"],
        "beach house": ["indie"],
        "death cab for cutie": ["indie"],
        "foster the people": ["indie", "pop"],
        "franz ferdinand": ["indie"],
        "grizzly bear": ["indie"],
        "interpol": ["indie"],
        "mac demarco": ["indie"],
        "mgmt": ["indie"],
        "modest mouse": ["indie"],
        "the national": ["indie"],
        "the shins": ["indie"],
        "the strokes": ["indie", "rock"],
        "tame impala": ["indie"],
        "vampire weekend": ["indie"],
        "yeah yeah yeahs": ["indie"],
        
        # === ALTERNATIVE ARTISTS ===
        "alice in chains": ["alternative"],
        "blind melon": ["alternative"],
        "bush": ["alternative"],
        "collective soul": ["alternative"],
        "jane's addiction": ["alternative"],
        "live": ["alternative"],
        "nirvana": ["alternative", "rock"],
        "pearl jam": ["alternative", "rock"],
        "smashing pumpkins": ["alternative"],
        "stone temple pilots": ["alternative"],
        
        # === FUNK ARTISTS ===
        "chic": ["funk"],
        "earth wind & fire": ["funk", "soul"],
        "james brown": ["funk", "soul"],
        "jamiroquai": ["funk"],
        "kool and the gang": ["funk"],
        "parliament-funkadelic": ["funk"],
        "red hot chili peppers": ["funk", "rock"],
        "sly and the family stone": ["funk"],
        "tower of power": ["funk"],
        
        # === GOSPEL ARTISTS ===
        "bebe winans": ["gospel"],
        "casting crowns": ["gospel"],
        "cece winans": ["gospel"],
        "chris tomlin": ["gospel"],
        "donnie mcclurkin": ["gospel"],
        "fred hammond": ["gospel"],
        "hillsong": ["gospel"],
        "israel houghton": ["gospel"],
        "kirk franklin": ["gospel"],
        "mahalia jackson": ["gospel"],
        "mary mary": ["gospel"],
        "paul baloche": ["gospel"],
        "skillet": ["gospel", "rock"],
        "switchfoot": ["gospel", "rock"],
        "yolanda adams": ["gospel"],
        
        # === LATIN ARTISTS ===
        "bad bunny": ["latin", "reggaeton"],
        "buena vista social club": ["latin"],
        "carlos santana": ["latin", "rock"],
        "daddy yankee": ["latin", "reggaeton"],
        "enrique iglesias": ["latin", "pop"],
        "gipsy kings": ["latin"],
        "j balvin": ["latin", "reggaeton"],
        "jennifer lopez": ["latin", "pop"],
        "jesse & joy": ["latin"],
        "mana": ["latin", "rock"],
        "manu chao": ["latin"],
        "ozuna": ["latin", "reggaeton"],
        "ricky martin": ["latin", "pop"],
        "selena": ["latin"],
        "shakira": ["latin", "pop"],
        
        # === CLASSICAL ARTISTS ===
        "bach": ["classical"],
        "beethoven": ["classical"],
        "chopin": ["classical"],
        "ludovico einaudi": ["classical", "instrumental"],
        "max richter": ["ambient", "classical"],
        "mozart": ["classical"],
        "nils frahm": ["ambient", "classical"],
        "ólafur arnalds": ["ambient", "classical"],
        "ravi shankar": ["world", "classical"],
        "tchaikovsky": ["classical"],
        "vivaldi": ["classical"],
        "yo-yo ma": ["world", "classical"],
        
        # === BLUES ARTISTS ===
        "b.b. king": ["blues"],
        "howlin' wolf": ["blues"],
        "muddy waters": ["blues"],
        "robert johnson": ["blues"],
        "stevie ray vaughan": ["blues"],
        
        # === AMBIENT/EXPERIMENTAL ARTISTS ===
        "brian eno": ["ambient"],
        "stars of the lid": ["ambient"],
        "tim hecker": ["ambient"],
        "william basinski": ["ambient"],
        
        # === WORLD MUSIC ARTISTS ===
        "ali farka toure": ["world"],
        "cesaria evora": ["world"],
        "ladysmith black mambazo": ["world"],
        "nusrat fateh ali khan": ["world"],
        "youssou n'dour": ["world"],
        
        # === SOUNDTRACK COMPOSERS ===
        "alan silvestri": ["soundtrack"],
        "danny elfman": ["soundtrack"],
        "ennio morricone": ["soundtrack"],
        "hans zimmer": ["soundtrack"],
        "howard shore": ["soundtrack"],
        "james horner": ["soundtrack"],
        "john williams": ["soundtrack", "classical"],
        "thomas newman": ["soundtrack"],
        
        # === COLLABORATIONS & FEATURED ARTISTS ===
        "aerosmith & run-dmc": ["rock", "hip-hop"],
        "beyonce feat. jay-z": ["r&b", "pop"],
        "the carters": ["hip-hop", "r&b"],  # Jay-Z & Beyoncé duo
        "david bowie & queen": ["rock", "pop"],
        "dr. dre feat. eminem": ["hip-hop"],
        "drake feat. lil wayne": ["hip-hop"],
        "elton john & kiki dee": ["pop", "rock"],
        "eminem feat. dr. dre": ["hip-hop"],
        "eminem feat. rihanna": ["hip-hop", "pop"],
        "jay-z feat. beyonce": ["hip-hop", "r&b"],
        "jay-z feat. kanye west": ["hip-hop"],
        "jay z & beyonce": ["hip-hop", "r&b"],
        "kanye west feat. jay-z": ["hip-hop"],
        "lady gaga & tony bennett": ["pop", "jazz"],
        "lil wayne feat. drake": ["hip-hop"],
        "lil wayne feat. drake": ["hip-hop"],
        "queen & david bowie": ["rock", "pop"],
        "rihanna feat. eminem": ["pop", "hip-hop"],
        "run-dmc & aerosmith": ["hip-hop", "rock"],
        "tony bennett & lady gaga": ["jazz", "pop"],
        "under pressure": ["rock"],  # Famous Queen & Bowie collab
        "walk this way": ["rock", "hip-hop"],  # Aerosmith & Run-DMC
    }
    
    # Check artist-based classification first (only if artist is provided)
    if artist_name and artist_lower in artist_genres:
        genres.extend(artist_genres[artist_lower])
    
    # Check keyword-based classification (works with title only or title + artist)
    for genre, keywords in genre_keywords.items():
        if any(keyword in text for keyword in keywords):
            if genre not in genres:
                genres.append(genre)
    
   
    return genres[:3]  # Limit to 3 genres max

def process_songs_with_ai(batch_size=100):
    """Process songs using AI/rule-based genre classification"""
    session = SessionLocal()
    
    try:
        # Get songs without genres
        songs_query = session.execute(text('''
            SELECT ds.song_id, ds.song_title, da.artist_name
            FROM dim_song ds
            JOIN dim_song_artist dsa ON ds.song_id = dsa.song_id
            JOIN dim_artist da ON dsa.artist_id = da.artist_id
            LEFT JOIN dim_song_genre dsg ON ds.song_id = dsg.song_id
            WHERE dsg.song_id IS NULL
            LIMIT :batch_size
        '''), {"batch_size": batch_size}).fetchall()
        
        print(f"Processing {len(songs_query)} songs with AI classification...")
        
        for song_id, song_title, artist_name in songs_query:
            print(f"Classifying: '{song_title}' by {artist_name}")
            
            # Get genres from AI/rules
            predicted_genres = classify_genres_with_llm(song_title, artist_name)
            
            # Save to database
            for genre_name in predicted_genres:
                # Get or create genre
                genre = session.query(DimGenre).filter_by(genre_name=genre_name).first()
                if not genre:
                    genre = DimGenre(genre_name=genre_name)
                    session.add(genre)
                    session.commit()
                
                # Link song to genre
                existing = session.query(DimSongGenre).filter_by(
                    song_id=song_id, genre_id=genre.genre_id
                ).first()
                if not existing:
                    link = DimSongGenre(song_id=song_id, genre_id=genre.genre_id)
                    session.add(link)
            
            session.commit()
            print(f"  -> Assigned genres: {predicted_genres}")
            
            # No delay needed for local processing - let's go fast!
            # time.sleep(0.1)
        
        print(f"Completed processing {len(songs_query)} songs!")
        
    except Exception as e:
        print(f"Error: {e}")
        session.rollback()
    finally:
        session.close()

def process_songs_without_artists(batch_size=1000):
    """Process songs that don't have artist relationships using title-only classification"""
    session = SessionLocal()
    
    try:
        # Get songs without artist relationships and without genres
        songs_query = session.execute(text('''
            SELECT ds.song_id, ds.song_title
            FROM dim_song ds
            LEFT JOIN dim_song_artist dsa ON ds.song_id = dsa.song_id
            LEFT JOIN dim_song_genre dsg ON ds.song_id = dsg.song_id
            WHERE dsa.song_id IS NULL AND dsg.song_id IS NULL
            LIMIT :batch_size
        '''), {"batch_size": batch_size}).fetchall()
        
        print(f"Processing {len(songs_query)} songs WITHOUT artists using title-only classification...")
        
        for song_id, song_title in songs_query:
            print(f"Classifying (title-only): '{song_title}'")
            
            # Get genres from title-only rules
            predicted_genres = rule_based_genre_classification(song_title, artist_name=None)
            
            # Save to database
            for genre_name in predicted_genres:
                # Get or create genre
                genre = session.query(DimGenre).filter_by(genre_name=genre_name).first()
                if not genre:
                    genre = DimGenre(genre_name=genre_name)
                    session.add(genre)
                    session.commit()
                
                # Link song to genre
                existing = session.query(DimSongGenre).filter_by(
                    song_id=song_id, genre_id=genre.genre_id
                ).first()
                if not existing:
                    link = DimSongGenre(song_id=song_id, genre_id=genre.genre_id)
                    session.add(link)
            
            session.commit()
            print(f"  -> Assigned genres: {predicted_genres}")
        
        print(f"Completed processing {len(songs_query)} songs without artists!")
        
    except Exception as e:
        print(f"Error: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    # Process ALL songs in the database
    total_processed = 0
    batch_size = 1000
    
    while True:
        # Count songs without genres before processing
        session = SessionLocal()
        try:
            count_query = session.execute(text('''
                SELECT COUNT(*)
                FROM dim_song ds
                JOIN dim_song_artist dsa ON ds.song_id = dsa.song_id
                JOIN dim_artist da ON dsa.artist_id = da.artist_id
                LEFT JOIN dim_song_genre dsg ON ds.song_id = dsg.song_id
                WHERE dsg.song_id IS NULL
            ''')).scalar()
            
            print(f"Songs remaining to process: {count_query}")
            
            if count_query == 0:
                print("No more songs with artists to process!")
                break
                
        finally:
            session.close()
        
        # Process next batch
        print(f"\n--- Processing batch {total_processed // batch_size + 1} ---")
        process_songs_with_ai(batch_size=batch_size)
        total_processed += min(batch_size, count_query)
        
        print(f"Total songs processed so far: {total_processed}")
    
    # Also process songs without artists
    print("\n--- Now processing songs without artist relationships ---")
    total_no_artist = 0
    
    while True:
        # Count songs without artists and without genres
        session = SessionLocal()
        try:
            count_query = session.execute(text('''
                SELECT COUNT(*)
                FROM dim_song ds
                LEFT JOIN dim_song_artist dsa ON ds.song_id = dsa.song_id
                LEFT JOIN dim_song_genre dsg ON ds.song_id = dsg.song_id
                WHERE dsa.song_id IS NULL AND dsg.song_id IS NULL
            ''')).scalar()
            
            print(f"Songs without artists remaining to process: {count_query}")
            
            if count_query == 0:
                print("No more songs without artists to process!")
                break
                
        finally:
            session.close()
        
        # Process next batch of songs without artists
        print(f"\n--- Processing no-artist batch {total_no_artist // batch_size + 1} ---")
        process_songs_without_artists(batch_size=batch_size)
        total_no_artist += min(batch_size, count_query)
        
        print(f"Total songs without artists processed: {total_no_artist}")
    
    print(f"\n🎉 COMPLETED! Total songs processed:")
    print(f"  - With artists: {total_processed}")
    print(f"  - Without artists: {total_no_artist}")
    print(f"  - Grand total: {total_processed + total_no_artist}")