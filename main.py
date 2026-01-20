import json
import time
import random
import psycopg2
import threading
from datetime import datetime, timezone
from faker import Faker

# PostgreSQL Config
PG_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'dbname': 'votes_db',
    'user': 'admin',
    'password': 'admin'
}

# General Settings
random.seed(21)
fake = Faker()
TOTAL_VOTERS = 50_000
BATCH_SIZE = 10_000
NUM_THREADS = 4  # number of parallel threads for voter generation

# Districts
# — Full district → region map (146 districts, based on Uganda Bureau of Statistics)
district_to_region = {
    # Central (27)
    "Buikwe": "Central", "Bukomansimbi": "Central", "Butambala": "Central", "Buvuma": "Central",
    "Gomba": "Central", "Kalangala": "Central", "Kalungu": "Central", "Kampala": "Central",
    "Kasanda": "Central", "Kayunga": "Central", "Kiboga": "Central", "Kyankwanzi": "Central",
    "Kyotera": "Central", "Luweero": "Central", "Lwengo": "Central", "Lyantonde": "Central",
    "Masaka": "Central", "Mityana": "Central", "Mpigi": "Central", "Mubende": "Central",
    "Mukono": "Central", "Nakaseke": "Central", "Nakasongola": "Central", "Rakai": "Central",
    "Sembabule": "Central", "Wakiso": "Central",

    # Eastern (40)
    "Amuria": "Eastern", "Budaka": "Eastern", "Bududa": "Eastern", "Bugiri": "Eastern",
    "Bugweri": "Eastern", "Bukedea": "Eastern", "Bukwo": "Eastern", "Bulambuli": "Eastern",
    "Busia": "Eastern", "Butaleja": "Eastern", "Butebo": "Eastern", "Buyende": "Eastern",
    "Iganga": "Eastern", "Jinja": "Eastern", "Kaberamaido": "Eastern", "Kalaki": "Eastern",
    "Kaliro": "Eastern", "Kamuli": "Eastern", "Kapchorwa": "Eastern", "Kapelebyong": "Eastern",
    "Katakwi": "Eastern", "Kibuku": "Eastern", "Kumi": "Eastern", "Kween": "Eastern",
    "Luuka": "Eastern", "Manafwa": "Eastern", "Mayuge": "Eastern", "Mbale": "Eastern",
    "Namayingo": "Eastern", "Namisindwa": "Eastern", "Namutumba": "Eastern", "Ngora": "Eastern",
    "Pallisa": "Eastern", "Serere": "Eastern", "Sironko": "Eastern", "Soroti": "Eastern",
    "Tororo": "Eastern", "Butebo": "Eastern",

    # Northern (41)
    "Abim": "Northern", "Adjumani": "Northern", "Agago": "Northern", "Alebtong": "Northern",
    "Amolatar": "Northern", "Amudat": "Northern", "Amuru": "Northern", "Apac": "Northern",
    "Arua": "Northern", "Dokolo": "Northern", "Gulu": "Northern", "Kaabong": "Northern",
    "Karenga": "Northern", "Kitgum": "Northern", "Koboko": "Northern", "Kole": "Northern",
    "Kotido": "Northern", "Kwania": "Northern", "Lamwo": "Northern", "Lira": "Northern",
    "Madi-Okollo": "Northern", "Maracha": "Northern", "Moroto": "Northern", "Moyo": "Northern",
    "Nabilatuk": "Northern", "Nakapiripirit": "Northern", "Napak": "Northern", "Nebbi": "Northern",
    "Nwoya": "Northern", "Obongi": "Northern", "Omoro": "Northern", "Otuke": "Northern",
    "Oyam": "Northern", "Pader": "Northern", "Pakwach": "Northern", "Terego": "Northern",
    "Yumbe": "Northern", "Zombo": "Northern",

    # Western (38)
    "Buhweju": "Western", "Buliisa": "Western", "Bundibugyo": "Western", "Bunyangabu": "Western",
    "Bushenyi": "Western", "Hoima": "Western", "Ibanda": "Western", "Isingiro": "Western",
    "Kabale": "Western", "Kabarole": "Western", "Kagadi": "Western", "Kakumiro": "Western",
    "Kamwenge": "Western", "Kanungu": "Western", "Kasese": "Western", "Kazo": "Western",
    "Kibaale": "Western", "Kikuube": "Western", "Kiruhura": "Western", "Kiryandongo": "Western",
    "Kisoro": "Western", "Kitagwenda": "Western", "Kyegegwa": "Western", "Kyenjojo": "Western",
    "Masindi": "Western", "Mbarara": "Western", "Mitooma": "Western", "Ntoroko": "Western",
    "Ntungamo": "Western", "Rubanda": "Western", "Rubirizi": "Western", "Rukiga": "Western",
    "Rukungiri": "Western", "Rwampara": "Western", "Sheema": "Western"
}

# — Create a districts list from that dictionary (so random.choice works)
districts = list(district_to_region.keys())


# Lock for safe threaded inserts
vote_counts_lock = threading.Lock()

# --- Database Setup Functions ---

def create_tables(conn, cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS candidates (
            candidate_id VARCHAR(255) PRIMARY KEY,
            candidate_name VARCHAR(255),
            party_affiliation VARCHAR(255),
            biography TEXT,
            campaign_platforms TEXT,
            slogan TEXT,
            photo_url TEXT
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS voters (
            voter_id VARCHAR(255) PRIMARY KEY,
            voter_name VARCHAR(255),
            date_of_birth DATE,
            gender VARCHAR(255),
            nationality VARCHAR(255),
            registration_number VARCHAR(255),
            address_street VARCHAR(255),
            address_city VARCHAR(255),
            address_state VARCHAR(255),
            address_country VARCHAR(255),
            address_postcode VARCHAR(255),
            email VARCHAR(255),
            phone_number VARCHAR(255),
            picture TEXT,
            registered_age INTEGER
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS votes_stream (
            id SERIAL PRIMARY KEY,
            district TEXT,
            candidate_id TEXT,
            candidate_name TEXT,
            slogan TEXT,
            timestamp TIMESTAMP
        );
    """)
    conn.commit()
    print("✅ Tables created successfully!")

def insert_candidates(conn, cur):
    candidates_data = [
        {
            "candidate_id": "c1",
            "candidate_name": "Yoweri Museveni",
            "party_affiliation": "National Resistance Movement",
            "biography": "President of Uganda since 1986",
            "campaign_platforms": "Stability, security, infrastructure",
            "slogan": "Settle for the Best, Museveni is the Best",
            "photo_url": "/Users/smartlyfe/Desktop/RealtimeVotingEngineering/images/IMG_NRM_4183.jpg"
        },
        {
            "candidate_id": "c2",
            "candidate_name": "Robert Kyagulanyi Ssentamu",
            "party_affiliation": "National Unity Platform",
            "biography": "Opposition leader and youth icon",
            "campaign_platforms": "Anti-corruption, youth empowerment",
            "slogan": "A New Uganda Now",
            "photo_url": "/Users/smartlyfe/Desktop/RealtimeVotingEngineering/images/IMG_NUP_4181.jpg"
        },
        {
            "candidate_id": "c3",
            "candidate_name": "Mugisha Muntu",
            "party_affiliation": "Alliance for National Transformation",
            "biography": "Veteran opposition leader",
            "campaign_platforms": "Democracy and good governance",
            "slogan": "Change you can trust",
            "photo_url": "/Users/smartlyfe/Desktop/RealtimeVotingEngineering/images/IMG_ANT_4179.jpg"
        },
        {
            "candidate_id": "c4",
            "candidate_name": "James Nathan Nandala Mafabi",
            "party_affiliation": "Forum for Democratic Change",
            "biography": "FDC leader and presidential candidate",
            "campaign_platforms": "Economic reform and jobs",
            "slogan": "Fixing the economy; Money in our Pockets",
            "photo_url": "/Users/smartlyfe/Desktop/RealtimeVotingEngineering/images/IMG_FDC_4804 2.jpg"
        },
        {
            "candidate_id": "c5",
            "candidate_name": "Robert Kasibante",
            "party_affiliation": "National Peasants Party",
            "biography": "NPP presidential candidate",
            "campaign_platforms": "Agriculture and rural growth",
            "slogan": "Skills for the Future",
            "photo_url": "/Users/smartlyfe/Desktop/RealtimeVotingEngineering/images/IMG_NPP_4806 2.jpg"
        },
        {
            "candidate_id": "c6",
            "candidate_name": "Mubarak Munyagwa Sserunga",
            "party_affiliation": "Common Man’s Party",
            "biography": "CMP presidential candidate",
            "campaign_platforms": "Grassroots empowerment",
            "slogan": "United we stand",
            "photo_url": "/Users/smartlyfe/Desktop/RealtimeVotingEngineering/images/IMG_CMP4809 2.jpg"
        },
        {
            "candidate_id": "c7",
            "candidate_name": "Elton Joseph Mabirizi",
            "party_affiliation": "Conservative Party",
            "biography": "CP candidate focusing on federalism",
            "campaign_platforms": "Federalism and decentralisation",
            "slogan": "Federalism is the answer",
            "photo_url": "/Users/smartlyfe/Desktop/RealtimeVotingEngineering/images/IMG_CP_4808 2.jpg"
        },
        {
            "candidate_id": "c8",
            "candidate_name": "Bulira Frank Kabinga",
            "party_affiliation": "Revolutionary People’s Party",
            "biography": "RPP presidential candidate",
            "campaign_platforms": "Mass mobilisation and reform",
            "slogan": "Federal for a new Uganda",
            "photo_url": "/Users/smartlyfe/Desktop/RealtimeVotingEngineering/images/IMG_RPP_4811.jpg"
        }
    ]

    for candidate in candidates_data:
        cur.execute("""
            INSERT INTO candidates 
            (candidate_id, candidate_name, party_affiliation, biography, campaign_platforms, slogan, photo_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (candidate_id) DO NOTHING
        """, (
            candidate['candidate_id'], candidate['candidate_name'], candidate['party_affiliation'],
            candidate['biography'], candidate['campaign_platforms'], candidate['slogan'], candidate['photo_url']
        ))
    conn.commit()
    print("✅ Candidates inserted successfully!")



def generate_voter_full():
    dob = fake.date_of_birth(minimum_age=18, maximum_age=90)
    age = (datetime.today().date() - dob).days // 365
    voter_id = str(fake.unique.random_int(min=1000000000, max=9999999999))
    return (
        voter_id,
        fake.name(),
        dob,
        random.choice(["Male", "Female", "Other"]),
        "Ugandan",
        fake.unique.bothify(text="REG#######"),
        fake.street_address(),
        fake.city(),
        fake.state(),
        "Uganda",
        fake.postcode(),
        fake.email(),
        fake.phone_number(),
        "https://via.placeholder.com/150",
        age
    )


def insert_voters_threaded(start_idx, end_idx):
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    for i in range(start_idx, end_idx, BATCH_SIZE):
        batch = [generate_voter_full() for _ in range(min(BATCH_SIZE, end_idx - i))]
        args_str = ",".join(cur.mogrify(
            "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", voter).decode("utf-8") for voter in batch)
        cur.execute(f"INSERT INTO voters (voter_id, voter_name, date_of_birth, gender, nationality, registration_number,"
                    f"address_street, address_city, address_state, address_country, address_postcode, email, phone_number, picture, registered_age)"
                    f" VALUES {args_str} ON CONFLICT (voter_id) DO NOTHING;")
        conn.commit()
        print(f"Thread {threading.current_thread().name}: Inserted {i + len(batch) - start_idx} / {end_idx - start_idx} voters")
    cur.close()
    conn.close()


def reset_database():
    """Reset the votes_stream table and clear in-memory vote counts."""
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE votes_stream;")
        conn.commit()
        cur.close()
        conn.close()
        print("🧹 Cleared previous votes from votes_stream table.")
    except Exception as e:
        print(f"⚠️ Error resetting votes: {e}")


# --- Main Execution ---
if __name__ == "__main__":
    print("🔧 Starting database initialization...")

    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cur = conn.cursor()

        create_tables(conn, cur)
        insert_candidates(conn, cur)

        cur.execute("SELECT COUNT(*) FROM voters")
        existing = cur.fetchone()[0]
        cur.close()
        conn.close()

        if existing < TOTAL_VOTERS:
            print(f"👥 Generating {TOTAL_VOTERS - existing} new voters...")
            voters_per_thread = (TOTAL_VOTERS - existing) // NUM_THREADS
            threads = []
            for i in range(NUM_THREADS):
                start_idx = i * voters_per_thread
                end_idx = (i + 1) * voters_per_thread if i != NUM_THREADS - 1 else TOTAL_VOTERS - existing
                t = threading.Thread(target=insert_voters_threaded, args=(start_idx, end_idx), name=f"VoterThread-{i+1}")
                threads.append(t)
                t.start()
            for t in threads:
                t.join()
            print("✅ All voters inserted successfully.")
        else:
            print("ℹ️ Voters already exist, skipping generation.")

    except Exception as e:
        print(f"⚠️ Error during setup: {e}")

    print("\n🧹 Resetting previous votes...")
    reset_database()
    print("✅ Setup complete. No votes produced — ready for voting.py")
