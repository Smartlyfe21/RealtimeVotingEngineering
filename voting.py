import json
import time
import random
import threading
from datetime import datetime, timezone
from faker import Faker
import psycopg2
from confluent_kafka import Producer, Consumer, KafkaException

# - PostgreSQL Config
PG_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'dbname': 'votes_db',
    'user': 'admin',
    'password': 'admin'
}

# - Kafka Config
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'vote-group',
    'auto.offset.reset': 'earliest'
}

TOPIC = 'votes'
random.seed(None)
fake = Faker()
TOTAL_VOTERS = 50_000
BATCH_SIZE = 10_000
NUM_THREADS = 4  # Number of threads for voter insertion

# - Districts
districts = [
    'Abim', 'Adjumani', 'Agago', 'Alebtong', 'Amolatar', 'Amuria', 'Amuru', 'Apac', 'Arua',
    'Budaka', 'Bududa', 'Bugiri', 'Bugweri', 'Buhweju', 'Buikwe', 'Bukedea', 'Bukomansimbi', 'Bukwo',
    'Bulambuli', 'Buliisa', 'Bundibugyo', 'Bunyangabu', 'Bushenyi', 'Busia', 'Butaleja', 'Butambala',
    'Buvuma', 'Buyende', 'Dokolo', 'Gomba', 'Gulu', 'Hoima', 'Ibanda', 'Iganga', 'Isingiro', 'Jinja',
    'Kaabong', 'Kabale', 'Kabarole', 'Kaberamaido', 'Kalangala', 'Kaliro', 'Kalungu', 'Kampala',
    'Kamuli', 'Kamwenge', 'Kanungu', 'Kapchorwa', 'Kasese', 'Katakwi', 'Kayunga', 'Kazo', 'Kibale',
    'Kiboga', 'Kikuube', 'Kiruhura', 'Kiryandongo', 'Kisoro', 'Kitgum', 'Koboko', 'Kotido', 'Kumi',
    'Kwania', 'Kween', 'Kyankwanzi', 'Kyegegwa', 'Kyenjojo', 'Kyotera', 'Lamwo', 'Lira', 'Luuka',
    'Luwero', 'Lwengo', 'Lyantonde', 'Madi-Okollo', 'Manafwa', 'Maracha', 'Masaka', 'Masindi', 'Mayuge',
    'Mbale', 'Mbarara', 'Mityana', 'Moyo', 'Mpigi', 'Mubende', 'Mukono', 'Nabilatuk', 'Nakaseke',
    'Nakapiripirit', 'Nakasongola', 'Namayingo', 'Namisindwa', 'Namutumba', 'Napak', 'Nebbi', 'Ngora',
    'Ntoroko', 'Ntungamo', 'Nwoya', 'Otuke', 'Oyam', 'Pader', 'Pakwach', 'Pallisa', 'Rakai', 'Rubanda',
    'Rubirizi', 'Rukiga', 'Rukungiri', 'Sembabule', 'Serere', 'Sheema', 'Sironko', 'Soroti', 'Tororo',
    'Wakiso', 'Yumbe', 'Zombo'
]

# - Thread Event & Vote Counts
stop_event = threading.Event()
vote_counts = {}
vote_counts_lock = threading.Lock()

# - Create Tables
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

# - Insert Candidates
def insert_candidates(conn, cur):
    candidates_data = [
        {
            "candidate_id": "c1",
            "candidate_name": "Yoweri Museveni",
            "party_affiliation": "National Resistance Movement",
            "biography": "President of Uganda for 40 years.",
            "campaign_platforms": "Stability, security, infrastructure development",
            "slogan": "Settle for the Best, Museveni is the Best",
            "photo_url": "/Users/smartlyfe/Desktop/RealtimeVotingEngineering/images/IMG_NRM_4183.jpg"  # NRM Yellow
        },
        {
            "candidate_id": "c2",
            "candidate_name": "Robert Kyagulanyi Ssentamu",
            "party_affiliation": "National Unity Platform",
            "biography": "Popular musician and youth leader.",
            "campaign_platforms": "Anti-corruption, youth empowerment, social reforms",
            "slogan": "A New Uganda Now",
            "photo_url": "/Users/smartlyfe/Desktop/RealtimeVotingEngineering/images/IMG_NUP_4181.jpg"  # NUP Red
        },
        {
            "candidate_id": "c3",
            "candidate_name": "Mugisha Muntu",
            "party_affiliation": "Alliance for National Transformation",
            "biography": "Veteran opposition leader.",
            "campaign_platforms": "Democracy, human rights, good governance",
            "slogan": "Change you can trust",
            "photo_url": "/Users/smartlyfe/Desktop/RealtimeVotingEngineering/images/IMG_ANT_4179.jpg"  # ANT Purple
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

# - Generate Fake Voter
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

# - Insert Voters Threaded
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


# - Produce Votes
# - Produce Votes
def produce_votes():
    import json
    import time
    import random
    from datetime import datetime, timezone
    from confluent_kafka import Producer
    from PIL import Image
    import psycopg2

    random.seed(None)  # ensure randomness each run

    total_votes = 80_000  # total votes for the election

    # --- Fetch candidates ---
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT candidate_id, candidate_name, slogan, photo_url, party_affiliation, biography 
        FROM candidates
    """)
    candidates_list = cur.fetchall()
    conn.close()

    # Randomly picked winner
    winner_candidate = random.choice(candidates_list)
    others = [c for c in candidates_list if c != winner_candidate]
    random.shuffle(others)

    #  Assigning exact vote counts ---
    winner_votes = random.randint(int(0.51 * total_votes), int(0.53 * total_votes))
    runner_votes = random.randint(int(0.37 * total_votes), int(0.39 * total_votes))
    last_votes = total_votes - winner_votes - runner_votes

    votes_distribution = [
        (winner_candidate, winner_votes),
        (others[0], runner_votes),
        (others[1], last_votes)
    ]

    # Build full votes list and shuffle
    votes_list = []
    for candidate, count in votes_distribution:
        votes_list.extend([candidate] * count)
    random.shuffle(votes_list)

    #  Produce votes to Kafka
    producer = Producer({'bootstrap.servers': 'localhost:29092'})
    votes_casted = {c[1]: 0 for c in candidates_list}

    for idx, candidate in enumerate(votes_list, start=1):
        vote = {
            'district': random.choice(districts),
            'candidate_id': candidate[0],
            'candidate_name': candidate[1],
            'slogan': candidate[2],
            'timestamp': datetime.now(timezone.utc).isoformat()
        }

        producer.produce(TOPIC, key=vote['district'], value=json.dumps(vote))
        votes_casted[candidate[1]] += 1

        if idx % 500 == 0:
            producer.flush()
        time.sleep(0.005)  # small delay for realistic pace

    producer.flush()

    #  Election results
    print(f"\n🏁 Election ended — {total_votes} votes cast.")
    sorted_votes = sorted(votes_casted.items(), key=lambda x: x[1], reverse=True)
    for idx, (name, count) in enumerate(sorted_votes, start=1):
        print(f"{['🏆 Winner','🥈 Runner-up','🥉 Third'][idx-1]}: {name} — {count} votes ({count/total_votes*100:.2f}%)")

    #  Display winner image
    winner_image_path = winner_candidate[3]
    try:
        Image.open(winner_image_path).show()
    except Exception as e:
        print(f"⚠️ Could not open winner image: {e}")

# - Consume Votes
def consume_votes():
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([TOPIC])

    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()

    print("✅ Consumer started. Listening for new votes...\n")

    try:
        while not stop_event.is_set():
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            vote = json.loads(msg.value().decode('utf-8'))
            cur.execute("""
                INSERT INTO votes_stream (district, candidate_id, candidate_name, slogan, timestamp)
                VALUES (%s, %s, %s, %s, %s)
            """, (vote['district'], vote['candidate_id'], vote['candidate_name'], vote['slogan'], vote['timestamp']))
            conn.commit()

            # Update live vote counts
            with vote_counts_lock:
                if vote['candidate_name'] not in vote_counts:
                    vote_counts[vote['candidate_name']] = {}
                if vote['district'] not in vote_counts[vote['candidate_name']]:
                    vote_counts[vote['candidate_name']][vote['district']] = 0
                vote_counts[vote['candidate_name']][vote['district']] += 1

            print(f"📥 Inserted vote: {vote}")
            with vote_counts_lock:
                print("📊 Live vote counts:")
                for cand, districts_count in vote_counts.items():
                    total = sum(districts_count.values())
                    print(f"  {cand}: {total} votes")
                print("\n")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        cur.close()
        conn.close()
        print("🛑 Consumer thread stopped.")

# - Main Execution
if __name__ == "__main__":
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cur = conn.cursor()
        create_tables(conn, cur)

        cur.execute("SELECT COUNT(*) FROM candidates")
        if cur.fetchone()[0] == 0:
            insert_candidates(conn, cur)

        cur.execute("SELECT COUNT(*) FROM voters")
        existing = cur.fetchone()[0]
        cur.close()
        conn.close()

        if existing < TOTAL_VOTERS:
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

    except Exception as e:
        print(f"⚠️ Error: {e}")

    # - Start Producer and Consumer Threads
    producer_thread = threading.Thread(target=produce_votes)
    consumer_thread = threading.Thread(target=consume_votes)

    producer_thread.start()
    consumer_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Stopping all threads...")
        stop_event.set()
        producer_thread.join()
        consumer_thread.join()
        print("✅ All threads stopped. Exiting program.")
