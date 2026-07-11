import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import networkx as nx
import urllib.request
import json
import sys
import os
from pathlib import Path

# Add root directory to python path
sys.path.append(str(Path(__file__).resolve().parent))

# Import logic from existing src module
from src.recommendation_engine import recommend, load_data as load_recsys_data
from src.optimize_lineup import build_candidate_pool, optimise_xi, FORMATIONS

# Page configs
st.set_page_config(
    page_title="GoalData League Analytics",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom premium styling (Dark Theme / Glassmorphism vibes)
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700&display=swap');
html, body, [class*="css"] {
    font-family: 'Outfit', sans-serif;
}
.main-header {
    background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
    padding: 2.5rem;
    border-radius: 16px;
    color: #f8fafc;
    text-align: center;
    margin-bottom: 2rem;
    border: 1px solid #334155;
    box-shadow: 0 10px 30px rgba(0,0,0,0.25);
}
.main-header h1 {
    font-size: 2.8rem !important;
    font-weight: 700 !important;
    background: linear-gradient(to right, #38bdf8, #818cf8);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    margin-bottom: 0.5rem;
}
.main-header p {
    font-size: 1.1rem;
    color: #94a3b8;
}
.card {
    background-color: #1e293b;
    padding: 1.5rem;
    border-radius: 12px;
    border: 1px solid #334155;
    margin-bottom: 1.5rem;
    box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
}
.card h3 {
    margin-top: 0;
    color: #38bdf8;
}
.stat-val {
    font-size: 2rem;
    font-weight: bold;
    color: #f8fafc;
}
.stat-lbl {
    font-size: 0.85rem;
    color: #94a3b8;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}
</style>
""", unsafe_allow_html=True)

# Main App Header
st.markdown("""
<div class="main-header">
    <h1>GoalData League Analytics Dashboard</h1>
    <p>Advanced scouting, squad representation, integer linear programming squad selection & passing networks</p>
</div>
""", unsafe_allow_html=True)

# Cache data loading
@st.cache_data
def load_all_data():
    recsys_df = load_recsys_data()
    return recsys_df

try:
    df_recsys = load_all_data()
except Exception as e:
    st.error(f"Error loading dataset: {e}")
    st.warning("Please ensure that you have run the feature engineering and clustering scripts first.")
    st.stop()

# Sidebar config
st.sidebar.markdown("## Navigation & Config")
menu = st.sidebar.radio(
    "Choose Analytics Layer",
    ["Scouting & Player Recommender", "Tactical Clustering Analysis", "Squad Lineup Optimizer", "Passing Network & xG Graph"]
)

# Sidebar metadata
st.sidebar.markdown("---")
st.sidebar.markdown("### Dataset Overview")
st.sidebar.metric(label="Total Player-Seasons", value=f"{len(df_recsys):,}")
st.sidebar.metric(label="Unique Players", value=f"{df_recsys['player_name'].nunique():,}")
st.sidebar.metric(label="Seasons Covered", value=", ".join(sorted(df_recsys['season'].unique())))

# ----------------- TAB 1: SCOUTING & PLAYER RECOMMENDER -----------------
if menu == "Scouting & Player Recommender":
    st.markdown("## 🔍 Scouting & Player Similarity Engine")
    st.markdown("Find candidate replacements or comparable profiles using low-dimensional tactical embeddings.")

    col1, col2 = st.columns([1, 3])

    with col1:
        st.subheader("Query Configuration")
        # Search filter
        search_query = st.text_input("Search Player by Name", value="Luka Modric")
        
        # Filter matching players
        matching = df_recsys[df_recsys['player_name'].str.contains(search_query, case=False, na=False)]
        if matching.empty:
            st.error("No player found with that name.")
            player_list = sorted(df_recsys['player_name'].unique())
        else:
            player_list = sorted(matching['player_name'].unique())

        selected_player = st.selectbox("Select Player Profile", player_list)
        
        player_seasons = sorted(df_recsys[df_recsys['player_name'] == selected_player]['season'].unique(), reverse=True)
        selected_season = st.selectbox("Select Season", player_seasons)

        method = st.radio("Recommendation Model", ["Stronger (Position-Aware + Retained PCs)", "Baseline (Global Pool + PC1-PC2)"])
        method_key = "stronger" if method.startswith("Stronger") else "baseline"
        
        top_n = st.slider("Number of Recommendations", 3, 10, 5)
        
        same_cluster = False
        if method_key == "stronger":
            same_cluster = st.checkbox("Restrict recommendations to the same tactical cluster")

        current_only = st.checkbox("Recommend current form only (latest season)", value=True)

    with col2:
        st.subheader("Scouting Results")
        try:
            # Get target info
            target_profile = df_recsys[
                (df_recsys['player_name'] == selected_player) & 
                (df_recsys['season'] == selected_season)
            ].iloc[0]
            
            # Show profile card
            sc1, sc2, sc3, sc4 = st.columns(4)
            with sc1:
                st.markdown(f'<div class="card"><div class="stat-lbl">Player</div><div class="stat-val" style="font-size: 1.4rem;">{selected_player}</div></div>', unsafe_allow_html=True)
            with sc2:
                st.markdown(f'<div class="card"><div class="stat-lbl">Season</div><div class="stat-val">{selected_season}</div></div>', unsafe_allow_html=True)
            with sc3:
                st.markdown(f'<div class="card"><div class="stat-lbl">Position Group</div><div class="stat-val">{target_profile["position_group"]}</div></div>', unsafe_allow_html=True)
            with sc4:
                st.markdown(f'<div class="card"><div class="stat-lbl">Tactical Cluster</div><div class="stat-val">Cluster {target_profile["kmeans_cluster"]}</div></div>', unsafe_allow_html=True)

            # Generate recommendation
            recs = recommend(
                selected_player, 
                season=selected_season, 
                method=method_key, 
                top_n=top_n, 
                same_cluster=same_cluster, 
                current_only=current_only
            )
            
            st.markdown("### Top Similarity Matches")
            st.dataframe(recs[["player_name", "season", "position_group", "similarity", "distance"]], use_container_width=True)

            # Scatter Plot Visualization
            st.markdown("### 2D Projection Space (PCA)")
            fig, ax = plt.subplots(figsize=(10, 6), facecolor="#0f172a")
            ax.set_facecolor("#1e293b")

            # Plot all background players
            ax.scatter(
                df_recsys['PC1'], df_recsys['PC2'], 
                c='#475569', alpha=0.2, s=15, label='All Players'
            )

            # Plot recommendations
            rec_names = recs['player_name'].tolist()
            rec_profiles = df_recsys[df_recsys['player_name'].isin(rec_names)].groupby('player_name').first().reset_index()
            
            ax.scatter(
                rec_profiles['PC1'], rec_profiles['PC2'], 
                c='#38bdf8', s=100, edgecolors='white', marker='o', label='Recommended'
            )

            # Highlight query player
            ax.scatter(
                target_profile['PC1'], target_profile['PC2'], 
                c='#f59e0b', s=250, edgecolors='white', marker='*', label=f'Query: {selected_player}'
            )

            # Draw connections
            for _, row in rec_profiles.iterrows():
                ax.plot(
                    [target_profile['PC1'], row['PC1']], 
                    [target_profile['PC2'], row['PC2']], 
                    color='#38bdf8', linestyle='--', alpha=0.5
                )

            # Labels and layout
            ax.set_xlabel("PC1 (Volume of Play / Involvement)", color="#94a3b8", fontsize=12)
            ax.set_ylabel("PC2 (Tactical Role / Defensive-Attacking Bias)", color="#94a3b8", fontsize=12)
            ax.tick_params(colors="#94a3b8")
            ax.spines['bottom'].set_color('#334155')
            ax.spines['left'].set_color('#334155')
            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_visible(False)
            ax.grid(color='#334155', linestyle=':', alpha=0.5)
            
            legend = ax.legend(facecolor='#1e293b', edgecolor='#334155')
            for text in legend.get_texts():
                text.set_color('#f8fafc')

            # Annotate names
            ax.text(
                target_profile['PC1'] + 0.1, target_profile['PC2'] + 0.1, 
                f"{selected_player} ({selected_season})", 
                color='#f59e0b', fontweight='bold', fontsize=10
            )
            for _, row in rec_profiles.iterrows():
                ax.text(
                    row['PC1'] + 0.1, row['PC2'] + 0.1, 
                    row['player_name'], 
                    color='#f8fafc', fontsize=9
                )

            st.pyplot(fig)

        except Exception as e:
            st.error(f"Error computing recommendations: {e}")

# ----------------- TAB 2: TACTICAL CLUSTERING ANALYSIS -----------------
elif menu == "Tactical Clustering Analysis":
    st.markdown("## 📊 Style-of-Play Clustering Analysis")
    st.markdown("Analysis of players' tactical profiles segmented using K-Means and DBSCAN algorithms on the PCA feature space.")

    c1, c2 = st.columns([1, 2])

    with c1:
        st.subheader("Cluster Details")
        st.markdown("""
        The clustering algorithm identifies four distinct style-of-play profiles:
        * **Cluster 0**: Attacking Forwards & Creative Wingers
        * **Cluster 1**: Goalkeepers (distinct profile, fully isolated)
        * **Cluster 2**: Midfielders (distribution, progression and control)
        * **Cluster 3**: Defensive Specialists & Center Backs
        """)

        # Cluster distribution stats
        cluster_counts = df_recsys['kmeans_cluster'].value_counts().sort_index()
        fig_dist, ax_dist = plt.subplots(figsize=(6, 4), facecolor="#0f172a")
        ax_dist.set_facecolor("#1e293b")
        
        bars = ax_dist.bar(
            [f"Cluster {i}" for i in cluster_counts.index], 
            cluster_counts.values, 
            color=["#f43f5e", "#10b981", "#8b5cf6", "#f59e0b"]
        )
        ax_dist.set_title("Player Distribution by Cluster", color="#f8fafc")
        ax_dist.tick_params(colors="#94a3b8")
        ax_dist.spines['bottom'].set_color('#334155')
        ax_dist.spines['left'].set_color('#334155')
        ax_dist.spines['top'].set_visible(False)
        ax_dist.spines['right'].set_visible(False)
        
        # Add labels on top of bars
        for bar in bars:
            yval = bar.get_height()
            ax_dist.text(
                bar.get_x() + bar.get_width()/2, yval + 20, 
                f"{yval:,}", 
                ha='center', va='bottom', color='#f8fafc', fontsize=9
            )
            
        st.pyplot(fig_dist)

    with c2:
        st.subheader("2D Representation Space")
        # Visualizing K-Means clusters
        fig_proj, ax_proj = plt.subplots(figsize=(10, 7), facecolor="#0f172a")
        ax_proj.set_facecolor("#1e293b")
        
        colors = ["#f43f5e", "#10b981", "#8b5cf6", "#f59e0b"]
        for i in range(4):
            c_mask = df_recsys['kmeans_cluster'] == i
            ax_proj.scatter(
                df_recsys.loc[c_mask, 'PC1'], 
                df_recsys.loc[c_mask, 'PC2'], 
                c=colors[i], label=f'Cluster {i}', alpha=0.6, s=15
            )

        ax_proj.set_xlabel("PC1 (Volume of Play / Involvement)", color="#94a3b8")
        ax_proj.set_ylabel("PC2 (Tactical Role)", color="#94a3b8")
        ax_proj.tick_params(colors="#94a3b8")
        ax_proj.spines['bottom'].set_color('#334155')
        ax_proj.spines['left'].set_color('#334155')
        ax_proj.spines['top'].set_visible(False)
        ax_proj.spines['right'].set_visible(False)
        ax_proj.grid(color='#334155', linestyle=':', alpha=0.5)
        
        legend_proj = ax_proj.legend(facecolor='#1e293b', edgecolor='#334155')
        for text in legend_proj.get_texts():
            text.set_color('#f8fafc')
            
        st.pyplot(fig_proj)

    st.markdown("### Browse Players inside Clusters")
    selected_c = st.selectbox("Inspect Cluster Details", [0, 1, 2, 3])
    cluster_players = df_recsys[df_recsys['kmeans_cluster'] == selected_c].head(20)
    st.dataframe(cluster_players[["player_name", "season", "position_group", "PC1", "PC2", "dbscan_cluster"]], use_container_width=True)


# ----------------- TAB 3: SQUAD LINEUP OPTIMIZER -----------------
elif menu == "Squad Lineup Optimizer":
    st.markdown("## 🧠 Starting-XI Lineup Optimizer (ILP)")
    st.markdown("Solve an Integer Linear Program (ILP) using PuLP to assemble the mathematically optimal starting XI based on position groups and rating weights.")

    col_opt1, col_opt2 = st.columns([1, 2])

    with col_opt1:
        st.subheader("Optimizer Parameters")
        
        # Available seasons
        seasons = ["2021-2022"]  # Default UCL season available in datasets
        selected_season = st.selectbox("Select Season to Optimize", seasons)
        
        formation = st.selectbox("Select Formation", list(FORMATIONS.keys()))
        min_minutes = st.number_input("Minimum Minutes Played", min_value=90, max_value=3000, value=900, step=100)

        run_opt = st.button("🧬 Optimize Starting XI", use_container_width=True)

    with col_opt2:
        st.subheader("Optimal Starting XI")
        
        if run_opt:
            with st.spinner("Solving ILP..."):
                try:
                    pool = build_candidate_pool(selected_season, min_minutes)
                    xi = optimise_xi(pool, FORMATIONS[formation])
                    
                    st.success(f"Optimal Lineup Formed! Total Rating: {xi['rating'].sum():.3f}")
                    st.dataframe(xi[["player_name", "position_group", "minutes_played", "rating"]], use_container_width=True)

                    # Plot football pitch with optimal XI
                    st.subheader("Tactical Pitch Layout")
                    
                    fig, ax = plt.subplots(figsize=(10, 7.5))
                    # Draw green football pitch
                    pitch = patches.Rectangle((0, 0), 100, 100, edgecolor="white", facecolor="#2e7d32", linewidth=2)
                    ax.add_patch(pitch)
                    
                    # Pitch lines
                    # Halfway line
                    ax.plot([0, 100], [50, 50], color="white", linewidth=2)
                    # Center circle
                    center_circle = patches.Circle((50, 50), 9.15, edgecolor="white", facecolor="none", linewidth=2)
                    ax.add_patch(center_circle)
                    ax.scatter(50, 50, color="white")
                    
                    # Penalty boxes
                    # Goal side A (bottom)
                    box_bottom = patches.Rectangle((18, 0), 64, 16.5, edgecolor="white", facecolor="none", linewidth=2)
                    box_bottom_small = patches.Rectangle((36, 0), 28, 5.5, edgecolor="white", facecolor="none", linewidth=2)
                    ax.add_patch(box_bottom)
                    ax.add_patch(box_bottom_small)
                    
                    # Goal side B (top)
                    box_top = patches.Rectangle((18, 83.5), 64, 16.5, edgecolor="white", facecolor="none", linewidth=2)
                    box_top_small = patches.Rectangle((36, 83.5), 28, 5.5, edgecolor="white", facecolor="none", linewidth=2)
                    ax.add_patch(box_top)
                    ax.add_patch(box_top_small)
                    
                    # Assign coordinates based on formation
                    form_dict = FORMATIONS[formation]
                    
                    # Get coordinates
                    gk_coord = [(50, 8)]
                    if formation == "4-3-3":
                        def_coord = [(20, 25), (40, 25), (60, 25), (80, 25)]
                        mid_coord = [(25, 55), (50, 50), (75, 55)]
                        fw_coord = [(20, 80), (50, 85), (80, 80)]
                    elif formation == "4-4-2":
                        def_coord = [(20, 25), (40, 25), (60, 25), (80, 25)]
                        mid_coord = [(20, 50), (40, 50), (60, 50), (80, 50)]
                        fw_coord = [(35, 78), (65, 78)]
                    elif formation == "3-5-2":
                        def_coord = [(25, 25), (50, 25), (75, 25)]
                        mid_coord = [(15, 50), (32, 50), (50, 45), (68, 50), (85, 50)]
                        fw_coord = [(35, 78), (65, 78)]
                    elif formation == "4-2-3-1":
                        def_coord = [(20, 25), (40, 25), (60, 25), (80, 25)]
                        mid_coord = [(35, 45), (65, 45), (20, 60), (50, 60), (80, 60)]
                        fw_coord = [(50, 80)]
                        
                    # Filter and assign
                    gks = xi[xi["position_group"] == "GK"].sort_values("rating", ascending=False)
                    defs = xi[xi["position_group"] == "DEF"].sort_values("rating", ascending=False)
                    mids = xi[xi["position_group"] == "MID"].sort_values("rating", ascending=False)
                    fws = xi[xi["position_group"] == "FW"].sort_values("rating", ascending=False)
                    
                    players_positions = []
                    
                    if len(gks) >= 1:
                        players_positions.append((gks.iloc[0], gk_coord[0]))
                    
                    for idx, (_, p) in enumerate(defs.iterrows()):
                        if idx < len(def_coord):
                            players_positions.append((p, def_coord[idx]))
                            
                    for idx, (_, p) in enumerate(mids.iterrows()):
                        if idx < len(mid_coord):
                            players_positions.append((p, mid_coord[idx]))
                            
                    for idx, (_, p) in enumerate(fws.iterrows()):
                        if idx < len(fw_coord):
                            players_positions.append((p, fw_coord[idx]))
                            
                    # Plot players
                    for p, (x_pos, y_pos) in players_positions:
                        # Draw player dot
                        ax.scatter(x_pos, y_pos, color="#1e3c72", s=400, edgecolors="white", linewidths=2, zorder=5)
                        
                        # Add name and rating text
                        name_lbl = p["player_name"].split(" ")[-1]
                        ax.text(
                            x_pos, y_pos - 4, 
                            f"{name_lbl}\n({p['rating']:.2f})", 
                            color="white", fontsize=9, fontweight="bold", 
                            ha="center", va="top",
                            bbox=dict(facecolor='#0f172a', alpha=0.7, boxstyle='round,pad=0.3', edgecolor='none'),
                            zorder=6
                        )
                    
                    ax.set_xlim(-5, 105)
                    ax.set_ylim(-5, 105)
                    ax.axis("off")
                    st.pyplot(fig)
                    
                except Exception as e:
                    st.error(f"Failed to optimize lineup: {e}")
        else:
            st.info("Click the 'Optimize Starting XI' button to solve the ILP.")


# ----------------- TAB 4: PASSING NETWORK & xG GRAPH -----------------
elif menu == "Passing Network & xG Graph":
    st.markdown("## 🕸️ Passing Network & real xG Graph")
    st.markdown("Tactical representation of player passing combinations and shot expectations from StatsBomb Event streams.")

    match_id = st.sidebar.number_input("StatsBomb Match ID", value=22912)

    col_pn1, col_pn2 = st.columns([1, 2])

    with col_pn1:
        st.subheader("Match Selection")
        st.markdown(f"**Current Match ID**: {match_id} (2019 UCL Final: Liverpool vs Tottenham Hotspur)")
        
        load_events = st.button("🕸️ Fetch Passing Network Data", use_container_width=True)

    with col_pn2:
        st.subheader("Tactical Graph")
        
        if load_events:
            with st.spinner("Fetching event stream and compiling graph..."):
                try:
                    # Download data from GitHub
                    url = f"https://raw.githubusercontent.com/statsbomb/open-data/master/data/events/{match_id}.json"
                    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                    with urllib.request.urlopen(req, timeout=30) as r:
                        events = json.loads(r.read())
                    
                    # Parse passing networks
                    from src.build_passing_network import build_team_networks, player_xg, centralities
                    
                    graphs = build_team_networks(events)
                    xg_df = player_xg(events)
                    
                    teams = list(graphs.keys())
                    if not teams:
                        st.error("No teams or passes found for this match.")
                    else:
                        selected_team = st.selectbox("Select Team to Plot", teams)
                        g = graphs[selected_team]
                        
                        # Show statistics
                        cent = centralities(g)
                        st.markdown(f"### {selected_team} Centralities")
                        st.dataframe(cent.head(8), use_container_width=True)
                        
                        # Graph Plot
                        st.markdown("### Passing Network Visualization")
                        fig_net, ax_net = plt.subplots(figsize=(10, 8), facecolor="#0f172a")
                        ax_net.set_facecolor("#1e293b")
                        
                        # Define node sizes by weighted degree
                        wdeg = dict(g.degree(weight="weight"))
                        node_sizes = [wdeg[n] * 20 for n in g.nodes()]
                        
                        # Position node layouts
                        pos_net = nx.spring_layout(g, weight="weight", seed=42)
                        
                        # Draw edges scaled by pass weights
                        edges = g.edges(data=True)
                        weights = [d['weight'] for _, _, d in edges]
                        max_w = max(weights) if weights else 1
                        edge_widths = [w / max_w * 5 for w in weights]
                        
                        nx.draw_networkx_nodes(
                            g, pos_net, 
                            node_size=node_sizes, 
                            node_color="#38bdf8", 
                            edgecolors="white", 
                            linewidths=1.5,
                            ax=ax_net
                        )
                        
                        nx.draw_networkx_edges(
                            g, pos_net, 
                            width=edge_widths, 
                            edge_color="#94a3b8", 
                            alpha=0.5, 
                            arrowsize=15, 
                            ax=ax_net
                        )
                        
                        # Draw labels
                        nx.draw_networkx_labels(
                            g, pos_net, 
                            font_size=9, 
                            font_color="#f8fafc", 
                            font_weight="bold", 
                            ax=ax_net
                        )
                        
                        ax_net.axis("off")
                        st.pyplot(fig_net)
                        
                        # Expected goals
                        st.markdown("### Real Expected Goals (xG) Shot Leaders")
                        team_xg = xg_df[xg_df["team"] == selected_team]
                        st.dataframe(team_xg, use_container_width=True)
                        
                except Exception as e:
                    st.error(f"Failed to fetch or build passing network: {e}")
        else:
            st.info("Click 'Fetch Passing Network Data' to load and compute graph networks.")
