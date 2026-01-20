import os
import json
import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
import streamlit.components.v1 as components
from datetime import datetime
import plotly.io as pio

pio.templates.default = "plotly"

# Candidate Colors & Images
CANDIDATE_COLORS = {
    'Yoweri Museveni': '#FFFF00',
    'Robert Kyagulanyi Ssentamu': '#FF0000',
    'Mugisha Muntu': '#800080',
    'James Nathan Nandala Mafabi': '#ADD8E6',
    'Mubarak Munyagwa Sserunga': '#FFA500',
    'Elton Joseph Mabirizi': '#90EE90',
    'Bulira Frank Kabinga': '#A52A2A',
    'Robert Kasibante': '#000000'
}


CANDIDATE_IMAGES = {
    'Yoweri Museveni': '/Users/smartlyfe/Desktop/RealtimeVotingEngineering/images/IMG_NRM_4183.jpg',
    'Robert Kyagulanyi Ssentamu': '/Users/smartlyfe/Desktop/RealtimeVotingEngineering/images/IMG_NUP_4181.jpg',
    'Mugisha Muntu': '/Users/smartlyfe/Desktop/RealtimeVotingEngineering/images/IMG_ANT_4179.jpg',
    'James Nathan Nandala Mafabi': '/Users/smartlyfe/Desktop/RealtimeVotingEngineering/images/IMG_FDC_4804 2.jpg',
    'Mubarak Munyagwa Sserunga': '/Users/smartlyfe/Desktop/RealtimeVotingEngineering/images/IMG_CMP4809 2.jpg',
    'Elton Joseph Mabirizi': '/Users/smartlyfe/Desktop/RealtimeVotingEngineering/images/IMG_CP_4808 2.jpg',
    'Bulira Frank Kabinga': '/Users/smartlyfe/Desktop/RealtimeVotingEngineering/images/IMG_RPP_4811.jpg',
    'Robert Kasibante': '/Users/smartlyfe/Desktop/RealtimeVotingEngineering/images/IMG_NPP_4806 2.jpg'
}

#  District ID Mapping

DISTRICT_ID_MAP = {
    "UG101": "Kalangala", "UG102": "Kampala", "UG103": "Kiboga", "UG104": "Luwero",
    "UG105": "Masaka", "UG106": "Mpigi", "UG107": "Mubende", "UG108": "Mukono",
    "UG109": "Nakasongola", "UG110": "Rakai", "UG111": "Ssembabule", "UG112": "Kayunga",
    "UG113": "Wakiso", "UG114": "Lyantonde", "UG115": "Mityana", "UG116": "Nakaseke",
    "UG117": "Buikwe", "UG118": "Bukomansimbi", "UG119": "Butambala", "UG120": "Buvuma",
    "UG121": "Gomba", "UG122": "Kalungu", "UG123": "Kyankwanzi", "UG124": "Lwengo",
    "UG125": "Kyotera", "UG126": "Kassanda", "UG201": "Bugiri", "UG202": "Busia",
    "UG203": "Iganga", "UG204": "Jinja", "UG205": "Kamuli", "UG206": "Kapchorwa",
    "UG207": "Katakwi", "UG208": "Kumi", "UG209": "Mbale", "UG210": "Pallisa",
    "UG211": "Soroti", "UG212": "Tororo", "UG213": "Kaberamaido", "UG214": "Mayuge",
    "UG215": "Sironko", "UG216": "Amuria", "UG217": "Budaka", "UG218": "Bududa",
    "UG219": "Bukedea", "UG220": "Bukwo", "UG221": "Butaleja", "UG222": "Kaliro",
    "UG223": "Manafwa", "UG224": "Namutumba", "UG225": "Bulambuli", "UG226": "Buyende",
    "UG227": "Kibuku", "UG228": "Kween", "UG229": "Luuka", "UG230": "Namayingo",
    "UG231": "Ngora", "UG232": "Serere", "UG233": "Butebo", "UG234": "Namisindwa",
    "UG235": "Bugweri", "UG236": "Kapelebyong", "UG237": "Kalaki", "UG301": "Adjumani",
    "UG302": "Apac", "UG303": "Arua", "UG304": "Gulu", "UG305": "Kitgum",
    "UG306": "Kotido", "UG307": "Lira", "UG308": "Moroto", "UG309": "Moyo",
    "UG310": "Nebbi", "UG311": "Nakapiripirit", "UG312": "Pader", "UG313": "Yumbe",
    "UG314": "Abim", "UG315": "Amolatar", "UG316": "Amuru", "UG317": "Dokolo",
    "UG318": "Kaabong", "UG319": "Koboko", "UG320": "Maracha", "UG321": "Oyam",
    "UG322": "Agago", "UG323": "Alebtong", "UG324": "Amudat", "UG325": "Kole",
    "UG326": "Lamwo", "UG327": "Napak", "UG328": "Nwoya", "UG329": "Otuke",
    "UG330": "Zombo", "UG331": "Omoro", "UG332": "Pakwach", "UG333": "Kwania",
    "UG334": "Nabilatuk", "UG335": "Karenga", "UG336": "Madi Okollo", "UG337": "Obongi",
    "UG401": "Bundibugyo", "UG402": "Bushenyi", "UG403": "Hoima", "UG404": "Kabale",
    "UG405": "Kabarole", "UG406": "Kasese", "UG407": "Kibaale", "UG408": "Kisoro",
    "UG409": "Masindi", "UG410": "Mbarara", "UG411": "Ntungamo", "UG412": "Rukungiri",
    "UG413": "Kamwenge", "UG414": "Kanungu", "UG415": "Kyenjojo", "UG416": "Buliisa",
    "UG417": "Ibanda", "UG418": "Isingiro", "UG419": "Kiruhura", "UG420": "Buhweju",
    "UG421": "Kiryandongo", "UG422": "Kyegegwa", "UG423": "Mitooma", "UG424": "Ntoroko",
    "UG425": "Rubirizi", "UG426": "Sheema", "UG427": "Kagadi", "UG428": "Kakumiro",
    "UG429": "Rubanda", "UG430": "Bunyangabu", "UG431": "Rukiga", "UG432": "Kikuube",
    "UG433": "Kazo", "UG434": "Kitagwenda", "UG435": "Rwampara"
}

# Create reverse mapping (district name -> district code)
DISTRICT_NAME_TO_ID = {v: k for k, v in DISTRICT_ID_MAP.items()}

TARGET_VOTES = int(os.getenv("TARGET_VOTES", 500000))

#  District → Region Mapping

district_to_region = {
    "Buikwe": "Central", "Bukomansimbi": "Central", "Butambala": "Central", "Buvuma": "Central",
    "Gomba": "Central", "Kalangala": "Central", "Kalungu": "Central", "Kampala": "Central",
    "Kasanda": "Central", "Kayunga": "Central", "Kiboga": "Central", "Kyankwanzi": "Central",
    "Kyotera": "Central", "Luweero": "Central", "Lwengo": "Central", "Lyantonde": "Central",
    "Masaka": "Central", "Mityana": "Central", "Mpigi": "Central", "Mubende": "Central",
    "Mukono": "Central", "Nakaseke": "Central", "Nakasongola": "Central", "Rakai": "Central",
    "Sembabule": "Central", "Wakiso": "Central",

    "Amuria": "Eastern", "Budaka": "Eastern", "Bududa": "Eastern", "Bugiri": "Eastern",
    "Bugweri": "Eastern", "Bukedea": "Eastern", "Bukwo": "Eastern", "Bulambuli": "Eastern",
    "Busia": "Eastern", "Butaleja": "Eastern", "Butebo": "Eastern", "Buyende": "Eastern",
    "Iganga": "Eastern", "Jinja": "Eastern", "Kaberamaido": "Eastern", "Kaliro": "Eastern",
    "Kamuli": "Eastern", "Kapchorwa": "Eastern", "Kapelebyong": "Eastern", "Katakwi": "Eastern",
    "Kibuku": "Eastern", "Kumi": "Eastern", "Kween": "Eastern", "Luuka": "Eastern",
    "Manafwa": "Eastern", "Mayuge": "Eastern", "Mbale": "Eastern", "Namayingo": "Eastern",
    "Namisindwa": "Eastern", "Namutumba": "Eastern", "Ngora": "Eastern", "Pallisa": "Eastern",
    "Serere": "Eastern", "Sironko": "Eastern", "Soroti": "Eastern", "Tororo": "Eastern",

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

DISTRICTS = list(district_to_region.keys())

#  Auto Refresh

try:
    from streamlit_autorefresh import st_autorefresh
    has_autorefresh = True
except:
    has_autorefresh = False

st.set_page_config(page_title="Realtime UG Voting Dashboard", layout="wide")
if has_autorefresh:
    st_autorefresh(interval=15000, key="refresh")

# Fetch Votes

@st.cache_data(ttl=5)
def fetch_votes():
    try:
        conn = psycopg2.connect(host="localhost", port=5433,
                                dbname="votes_db", user="admin", password="admin")
        df = pd.read_sql("SELECT district, candidate_name, timestamp FROM votes_stream;", conn)
        conn.close()
        return df
    except Exception as e:
        st.sidebar.error(f"Unable to fetch votes: {e}")
        return pd.DataFrame()

votes_df = fetch_votes()

# Pivot vote counts
if not votes_df.empty:
    pivot = votes_df.groupby(["district", "candidate_name"]).size().unstack(fill_value=0)
else:
    pivot = pd.DataFrame(0, index=DISTRICTS, columns=list(CANDIDATE_COLORS.keys()))

pivot = pivot.reindex(DISTRICTS).fillna(0)

#  Totals & Metrics

totals = pivot.sum(axis=0).reset_index()
totals.columns = ["candidate_name", "total_votes"]
total_votes = int(totals["total_votes"].sum())

st.title("🇺🇬 Realtime UG 2026 Voting Dashboard")
c1, c2, c3, c4 = st.columns(4)
c1.metric("🎯 Target Votes", TARGET_VOTES)
c2.metric("📊 Total Votes (so far)", f"{total_votes:,}")
c3.metric("📍 Reporting Districts", int((pivot.sum(axis=1) > 0).sum()))
leader = totals.sort_values("total_votes", ascending=False).iloc[0]
c4.metric("🏆 Current Leader", leader["candidate_name"])
st.progress(min(total_votes / TARGET_VOTES, 1.0))

# Choropleth Map (Colored by Winning Candidate)

GEOJSON_PATH = "/Users/smartlyfe/Desktop/RealtimeVotingEngineering/data/ug_districts.geo.json"
ug_geo = None
try:
    with open(GEOJSON_PATH) as f:
        ug_geo = json.load(f)
except Exception as e:
    st.warning(f"Could not load GeoJSON: {e}")

if ug_geo:
    # Find the winning candidate for each district
    winning_candidate = pivot.idxmax(axis=1).reset_index()
    winning_candidate.columns = ["district", "winning_candidate"]

    # Get the vote count for the winning candidate
    winning_votes = pivot.max(axis=1).reset_index()
    winning_votes.columns = ["district", "votes"]

    # Merge the data
    map_data = winning_candidate.merge(winning_votes, on="district")

    # Map the winning candidate to their color
    map_data["color"] = map_data["winning_candidate"].map(CANDIDATE_COLORS)

    # Create the choropleth map using plotly graph objects for custom colors
    fig_map = go.Figure()

    for candidate in CANDIDATE_COLORS.keys():
        candidate_districts = map_data[map_data["winning_candidate"] == candidate]

        if len(candidate_districts) > 0:
            fig_map.add_trace(go.Choropleth(
                geojson=ug_geo,
                locations=candidate_districts["district"],
                z=[1] * len(candidate_districts),
                featureidkey="properties.name",
                colorscale=[[0, CANDIDATE_COLORS[candidate]], [1, CANDIDATE_COLORS[candidate]]],
                showscale=False,
                marker_line_color='white',
                marker_line_width=0.5,
                name=candidate,
                hovertemplate='<b>%{location}</b><br>Winner: ' + candidate + '<br>Votes: %{customdata}<extra></extra>',
                customdata=candidate_districts["votes"]
            ))

    fig_map.update_geos(fitbounds="locations", visible=False)
    fig_map.update_layout(
        title="📍 Districts Colored by Winning Candidate",
        height=600,
        showlegend=True,
        legend=dict(
            title="Winning Candidate",
            orientation="v",
            yanchor="middle",
            y=0.5,
            xanchor="left",
            x=1.02
        )
    )

    st.plotly_chart(fig_map, use_container_width=True)
else:
    st.warning("Map not loaded (geojson issue).")

#  Pie Chart

fig_pie = px.pie(
    totals, names="candidate_name", values="total_votes",
    color="candidate_name", color_discrete_map=CANDIDATE_COLORS,
    title="📊 Overall Vote Distribution"
)
st.plotly_chart(fig_pie, use_container_width=True)

#  District Stacked Bar

pivot_for_bar = pivot.reset_index()
pivot_for_bar = pivot_for_bar.rename(columns={'index': 'district'}) if 'index' in pivot_for_bar.columns else pivot_for_bar

fig_bar = px.bar(
    pivot_for_bar, x="district", y=list(CANDIDATE_COLORS.keys()),
    title="📈 Votes per District (Stacked)",
    barmode="stack", color_discrete_map=CANDIDATE_COLORS
)
st.plotly_chart(fig_bar, use_container_width=True)

#  Region Stacked Bar

# Create a copy to avoid modifying the original pivot
pivot_with_region = pivot.copy()
pivot_with_region["region"] = pivot_with_region.index.map(lambda d: district_to_region.get(d, "Unknown"))
region_sum = pivot_with_region.groupby("region").sum().reset_index()
region_cols = [c for c in region_sum.columns if c not in ["region"]]

fig_region = px.bar(
    region_sum, x="region", y=region_cols,
    title="📊 Votes by Region (Stacked)",
    barmode="stack", color_discrete_map=CANDIDATE_COLORS
)
st.plotly_chart(fig_region, use_container_width=True)

#  Time Trend

if not votes_df.empty:
    votes_df["timestamp"] = pd.to_datetime(votes_df["timestamp"])
    tt = votes_df.groupby([pd.Grouper(key="timestamp", freq="1Min"), "candidate_name"]).size().reset_index(name="votes")

    fig_trend = px.line(
        tt, x="timestamp", y="votes", color="candidate_name",
        title="📈 Vote Trend Over Time",
        color_discrete_map=CANDIDATE_COLORS
    )
    st.plotly_chart(fig_trend, use_container_width=True)

# Candidate Cards

st.subheader("🧑‍💼 Candidates Overview")
cards = st.columns(len(CANDIDATE_IMAGES))
for i, (nm, img) in enumerate(CANDIDATE_IMAGES.items()):
    v = int(totals.set_index("candidate_name").reindex([nm]).fillna(0)["total_votes"].iloc[0])
    pct = (v / total_votes * 100) if total_votes > 0 else 0
    with cards[i]:
        st.image(img, width=180)
        st.markdown(f"**{nm}**")
        st.markdown(f"Votes: **{v:,}**")
        st.markdown(f"Percent: **{pct:.2f}%**")

# Detailed Table

st.subheader("📋 Detailed Results by District")
detail = pivot.reset_index().melt(id_vars="district", var_name="candidate_name", value_name="votes")
st.dataframe(detail, use_container_width=True)

st.caption("Results from the `votes_stream` Postgres table. Refresh to see updates.")