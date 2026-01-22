# 🇺🇬 Realtime UG 2026 Voting Dashboard 🗳️

This project was inspired by the **upcoming 15th January 2026 Uganda general election**.  
It is a personal, educational, and technical demonstration of a realtime election voting system built using **Python, Apache Kafka, Spark Structured Streaming, PostgreSQL, and Streamlit**.  

It simulates votes, processes them in realtime, persists aggregated results, and visualizes them in an interactive dashboard.  

⚠️ **Disclaimer:** All data is fully synthetic and simulated. This project is NOT affiliated with any government, electoral commission, political party, or political process in Uganda or elsewhere, and it does NOT represent real election results, predictions, or outcomes.

---

## 🧠 Architecture Overview

The system follows a real-time streaming pipeline:



python main.py --> creates tables & sets up database
↓
voting.py --> simulates vote generation into Kafka
↓
Kafka Topic (raw votes)
↓
Spark Structured Streaming (processing & aggregations)
↓
PostgreSQL (persistent storage)
↓
Streamlit Dashboard (realtime visualization)

🛠️ Features
✔ Initializes database tables automatically via main.py
✔ Simulates voting data in realtime (voting.py)
✔ Streams data into Kafka
✔ Processes and aggregates votes with Spark Structured Streaming
✔ Stores results in PostgreSQL
✔ Displays realtime visualizations with Streamlit
✔ Shows per-candidate, per-district, and regional breakdowns
✔ Choropleth map of votes by district
✔ Time series and stacked charts for trends
✔ Candidate cards with photos, votes & percentages

📁 Project Structure
RealtimeVotingEngineering/
├── kafka-producer/         # Vote simulator code
│   └── produce_votes.py
├── spark-streaming/        # Spark streaming processor
│   └── spark-streaming.py
├── streamlit-app/          # Dashboard frontend
│   ├── streamlit-app.py
│   ├── data/               # GeoJSON & config
│   │   └── ug_districts.geo.json
│   └── images/             # Candidate photos & charts (pie, bar, map)
├── docker-compose.yml      # Orchestrates Kafka, Postgres, etc.
├── requirements.txt        # Python dependencies
├── main.py                 # Initializes DB & tables
├── voting.py               # Generates vote simulation
└── README.md               # Project documentation

1️⃣ Clone the Repository
git clone https://github.com/Smartlyfe21/RealtimeVotingEngineering.git
cd RealtimeVotingEngineering

2️⃣ Install Dependencies
pip install -r requirements.txt

3️⃣ Start Services
Start Kafka, Zookeeper, and PostgreSQL with Docker Compose:
docker compose up -d

4️⃣ Initialize Database Tables
python main.py

5️⃣ Start the Vote Simulation
python voting.py

6️⃣ Run the Spark Streaming Job
spark-submit spark-streaming/spark-streaming.py

📊 Dashboard Visuals
Candidates 🏛️
Voting included **all 136 districts across all regions**.
<table>
  <tr>
    <td align="center">
      <img src="https://github.com/Smartlyfe21/RealtimeVotingEngineering/raw/main/images/IMG_ANT_4179.jpg" width="80"/><br>44.93%
    </td>
    <td align="center">
      <img src="https://github.com/Smartlyfe21/RealtimeVotingEngineering/raw/main/images/IMG_CMP4809%202.jpg" width="80"/><br>47.77%
    </td>
    <td align="center">
      <img src="https://github.com/Smartlyfe21/RealtimeVotingEngineering/raw/main/images/IMG_CP_4808%202.jpg" width="80"/><br>0.70%
    </td>
    <td align="center">
      <img src="https://github.com/Smartlyfe21/RealtimeVotingEngineering/raw/main/images/IMG_FDC_4804%202.jpg" width="80"/><br>0.94%
    </td>
    <td align="center">
      <img src="https://github.com/Smartlyfe21/RealtimeVotingEngineering/raw/main/images/IMG_NPP_4806%202.jpg" width="80"/><br>1.30%
    </td>
    <td align="center">
      <img src="https://github.com/Smartlyfe21/RealtimeVotingEngineering/raw/main/images/IMG_NRM_4183.jpg" width="80"/><br>1.29%
    </td>
    <td align="center">
      <img src="https://github.com/Smartlyfe21/RealtimeVotingEngineering/raw/main/images/IMG_NUP_4181.jpg" width="80"/><br>1.62%
    </td>
    <td align="center">
      <img src="https://github.com/Smartlyfe21/RealtimeVotingEngineering/raw/main/images/IMG_RPP_4811.jpg" width="80"/><br>1.45%
    </td>
  </tr>
</table>


Charts & Maps 📈
<div style="display: flex; flex-wrap: wrap; gap: 20px; justify-content: center;"> <div style="text-align:center;"> <img src="https://github.com/Smartlyfe21/RealtimeVotingEngineering/raw/main/images/piechart.png" width="400"/><br> <strong>Vote Share Pie Chart</strong> </div> <div style="text-align:center;"> <img src="https://github.com/Smartlyfe21/RealtimeVotingEngineering/raw/main/images/region-barplot-3.png" width="400"/><br> <strong>Regional Bar Plot</strong> </div> <div style="text-align:center;"> <img src="https://github.com/Smartlyfe21/RealtimeVotingEngineering/raw/main/images/ugmap.png" width="400"/><br> <strong>District Votes Map</strong> </div> <div style="text-align:center;"> <img src="https://github.com/Smartlyfe21/RealtimeVotingEngineering/raw/main/images/Vote-trendplot-2.png" width="400"/><br> <strong>Vote Trends</strong> </div> <div style="text-align:center;"> <img src="https://github.com/Smartlyfe21/RealtimeVotingEngineering/raw/main/images/districtvote-4.png" width="400"/><br> <strong>District Vote Chart</strong> </div> </div>

📊 Results Preview
| Candidate                   | Votes   | Percent |
| --------------------------- | ------- | ------- |
| Yoweri Museveni             | 222,987 | 44.93%  |
| Robert Kyagulanyi Ssentamu  | 237,081 | 47.77%  |
| Mugisha Muntu               | 3,464   | 0.70%   |
| James Nathan Nandala Mafabi | 4,656   | 0.94%   |
| Mubarak Munyagwa Sserunga   | 6,452   | 1.30%   |
| Elton Joseph Mabirizi       | 6,406   | 1.29%   |
| Bulira Frank Kabinga        | 8,041   | 1.62%   |
| Robert Kasibante            | 7,221   | 1.45%   |

⚠️ Reminder: These results are fully synthetic and for demonstration only.

🧠 Notes & Tips
st.set_page_config() must appear as the first Streamlit call
District names/codes in the app must match GeoJSON
Adjust dashboard refresh settings for latency vs CPU usage


📄 License
This project is released under the MIT License
