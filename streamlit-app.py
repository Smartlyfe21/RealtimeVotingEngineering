# streamlit-app.py
import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from datetime import datetime

# Try to import autorefresh, warn if missing
try:
    from streamlit_autorefresh import st_autorefresh
    has_autorefresh = True
except Exception:
    has_autorefresh = False


# Config / Constants

st.set_page_config(page_title="Realtime UG-Election Voting Dashboard", layout="wide")

# Candidate colors and images (use the ones from your main.py)
CANDIDATE_COLORS = {
    'Yoweri Museveni': 'yellow',
    'Robert Kyagulanyi Ssentamu': 'red',
    'Mugisha Muntu': 'purple'
}

CANDIDATE_IMAGES = {
    'Yoweri Museveni': '/Users/smartlyfe/Desktop/RealtimeVotingEngineering/images/IMG_NRM_4183.jpg',  # NRM candidate yellow
    'Robert Kyagulanyi Ssentamu': '/Users/smartlyfe/Desktop/RealtimeVotingEngineering/images/IMG_NUP_4181.jpg',  # NUP candidate red
    'Mugisha Muntu': '/Users/smartlyfe/Desktop/RealtimeVotingEngineering/images/IMG_ANT_4179.jpg'  # ANT candidate purple
}


# All districts (same list you used)
DISTRICTS = [
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

TARGET_VOTES = 50000

# PostgreSQL connection
DB_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'dbname': 'votes_db',
    'user': 'admin',
    'password': 'admin'
}

# Auto-refresh every 5 seconds
if has_autorefresh:
    st_autorefresh(interval=5000, key="votes_refresh")
else:
    st.sidebar.warning("For auto-refresh install `streamlit-autorefresh`: pip install streamlit-autorefresh")


# Data fetching helpers

@st.cache_data(ttl=4)
def fetch_votes():
    """
    Returns dataframe: district, candidate_name, total_votes
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("""
            SELECT district, candidate_name, COUNT(*) as total_votes
            FROM votes_stream
            GROUP BY district, candidate_name
            ORDER BY district, candidate_name;
        """)
        data = cur.fetchall()
        cur.close()
        conn.close()
        df = pd.DataFrame(data, columns=['district', 'candidate_name', 'total_votes'])
        return df
    except Exception as e:
        st.sidebar.error(f"Unable to fetch votes: {e}")
        return pd.DataFrame(columns=['district', 'candidate_name', 'total_votes'])

@st.cache_data(ttl=30)
def fetch_candidates():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        df = pd.read_sql("SELECT * FROM candidates ORDER BY candidate_id;", conn)
        conn.close()
        return df
    except Exception as e:
        st.sidebar.warning(f"Candidates table not available: {e}")
        # Fallback to minimal candidate info if DB not available
        fallback = pd.DataFrame([
            {"candidate_id":"c1","candidate_name":"Yoweri Museveni","party_affiliation":"National Resistance Movement","slogan":"Settle for the Best, Museveni is the Best","photo_url":CANDIDATE_IMAGES.get("Yoweri Museveni","")},
            {"candidate_id":"c2","candidate_name":"Robert Kyagulanyi Ssentamu","party_affiliation":"National Unity Platform","slogan":"A New Uganda Now","photo_url":CANDIDATE_IMAGES.get("Robert Kyagulanyi Ssentamu","")},
            {"candidate_id":"c3","candidate_name":"Mugisha Muntu","party_affiliation":"Alliance for National Transformation","slogan":"Change you can trust","photo_url":CANDIDATE_IMAGES.get("Mugisha Muntu","")}
        ])
        return fallback

@st.cache_data(ttl=10)
def fetch_voters_sample(limit=20):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        df = pd.read_sql(f"SELECT * FROM voters LIMIT {limit};", conn)
        conn.close()
        return df
    except Exception as e:
        st.sidebar.info("Voters table not found or inaccessible.")
        return pd.DataFrame()


# UI Layout / Header

st.title("Realtime UG-Election Voting Dashboard 🇺🇬")
st.caption(f"Last refresh: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")

cols = st.columns([1, 2])
with cols[0]:
    st.metric("🎯 Target Votes", TARGET_VOTES)
with cols[1]:
    # show top-level totals once below
    pass


# Fetch data

votes_df = fetch_votes()
candidates_df = fetch_candidates()
voters_df = fetch_voters_sample(20)

# Ensure all districts appear even if no votes yet
all_districts_df = pd.DataFrame({'district': DISTRICTS})
# pivot votes to have candidate columns per district
if not votes_df.empty:
    pivot = votes_df.pivot_table(index='district', columns='candidate_name', values='total_votes', aggfunc='sum').fillna(0)
else:
    pivot = pd.DataFrame(index=DISTRICTS, columns=list(CANDIDATE_IMAGES.keys())).fillna(0)

# Make sure pivot contains all candidates
for c in CANDIDATE_IMAGES.keys():
    if c not in pivot.columns:
        pivot[c] = 0
pivot = pivot.reindex(index=DISTRICTS).fillna(0)

# Flatten for charting / metrics
vote_sum_per_candidate = pivot.sum(axis=0).reset_index()
vote_sum_per_candidate.columns = ['candidate_name', 'total_votes']
total_votes = int(vote_sum_per_candidate['total_votes'].sum())


# Top metrics / progress

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Votes (so far)", total_votes)
col2.metric("Distinct Districts Reporting", int((pivot.sum(axis=1) > 0).sum()))
# Leading candidate
if total_votes > 0:
    leader = vote_sum_per_candidate.sort_values('total_votes', ascending=False).iloc[0]
    col3.metric("Leading Candidate", f"{leader['candidate_name']}", f"{int(leader['total_votes'])} votes")
else:
    col3.metric("Leading Candidate", "—", "0 votes")
col4.metric("Refresh interval (sec)", 5 if has_autorefresh else "manual")

st.progress(min(1.0, total_votes / TARGET_VOTES))


# Charts

# Pie chart - overall distribution
if total_votes > 0:
    vote_sum_for_plot = vote_sum_per_candidate.copy()
    vote_sum_for_plot['pct'] = vote_sum_for_plot['total_votes'] / total_votes * 100
else:
    vote_sum_for_plot = vote_sum_per_candidate.copy()
    vote_sum_for_plot['pct'] = 0

fig_pie = px.pie(
    vote_sum_for_plot,
    names='candidate_name',
    values='total_votes',
    color='candidate_name',
    color_discrete_map=CANDIDATE_COLORS,
    title="Total Vote Distribution"
)
st.plotly_chart(fig_pie, use_container_width=True)

# Bar chart per district (stacked)
fig_bar = px.bar(
    pivot.reset_index(),
    x='district',
    y=list(CANDIDATE_IMAGES.keys()),
    title="Votes per District (stacked)"
)
fig_bar.update_layout(xaxis={'categoryorder':'array', 'categoryarray':DISTRICTS})
st.plotly_chart(fig_bar, use_container_width=True)


# Candidate Cards (images + small stats)

st.subheader("🧑‍💼 Candidates")
cards = st.columns(len(CANDIDATE_IMAGES))
for i, (name, img_url) in enumerate(CANDIDATE_IMAGES.items()):
    votes_for = int(vote_sum_per_candidate.set_index('candidate_name').reindex([name]).fillna(0)['total_votes'].iloc[0])
    pct = (votes_for / total_votes * 100) if total_votes > 0 else 0
    with cards[i]:
        st.image(img_url, caption=f"{name}", width=180)
        st.markdown(f"**{name}**")
        st.markdown(f"Votes: **{votes_for:,}**")
        st.markdown(f"Percent: **{pct:.2f}%**")


# Detailed Tables

st.subheader("📋 Detailed Vote Results (by district & candidate)")
# Merge to ensure all districts displayed
votes_flat = votes_df.copy()
if votes_flat.empty:
    votes_flat = pd.DataFrame(columns=['district', 'candidate_name', 'total_votes'])
all_districts_df = pd.DataFrame({'district': DISTRICTS})
# Make sure each district + candidate appears - we'll show pivot flattened
detailed = pivot.reset_index().melt(id_vars='district', var_name='candidate_name', value_name='total_votes')
detailed = detailed.sort_values(['district', 'candidate_name']).reset_index(drop=True)
st.dataframe(detailed, use_container_width=True)

# Candidates table
st.subheader("🗳️ Candidates Table (from DB)")
st.dataframe(candidates_df)

# Voters sample
st.subheader("🧾 Sample Registered Voters (first 20 rows)")
if not voters_df.empty:
    st.dataframe(voters_df)
else:
    st.info("No voters sample available (voters table missing or empty).")


# Footer / Notes

st.markdown("---")
st.caption("This dashboard reads from `votes_stream` Postgres table (aggregated via consumer or Spark). If numbers seem low: check your producer speed or how many messages are in the Kafka topic. To reach the target of 50,000 votes, increase producer throughput or run the producer longer.")
