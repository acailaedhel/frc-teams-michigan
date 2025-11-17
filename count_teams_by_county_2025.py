# count_teams_by_county_2025.py
import os
import requests
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import pgeocode
import time

# ----- CONFIG -----
import os
TBA_KEY = os.environ.get("TBA_KEY")
HEADERS = {"X-TBA-Auth-Key": TBA_KEY}
YEAR = 2025
STATE_FILTER = "MI"
COUNTY_SHAPEFILE = "Michigan_County.geojson"  # path to Michigan counties GeoJSON or shapefile
OUTPUT_MAP = "mi_frc_teams_by_county_2025.png"
OUTPUT_BAR = "mi_frc_teams_by_county_2025_bar.png"
# ------------------

nomi = pgeocode.Nominatim("us")

def get_mi_district_event_keys(year):
    # Optional: list known FIM district event keys manually, or query TBA for events in Michigan
    url = f"https://www.thebluealliance.com/api/v3/events/{year}"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    time.sleep(0.2)
    events = r.json()
    mi_events = [e for e in events if e.get("state_prov") in ("MI", "Michigan")]
    return [e["key"] for e in mi_events]

def get_teams_for_event(event_key):
    url = f"https://www.thebluealliance.com/api/v3/event/{event_key}/teams"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    time.sleep(0.2)
    return r.json()

def get_team_details(team_key):
    url = f"https://www.thebluealliance.com/api/v3/team/{team_key}"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    time.sleep(0.2)
    return r.json()

def zip_to_county(zipcode):
    if not zipcode or len(str(zipcode)) < 5:
        return None
    z = str(zipcode).strip()[:5]
    info = nomi.query_postal_code(z)
    if pd.isna(info.county_name):
        return None
    # pgeocode returns county_name like "Wayne County" or "Wayne"
    county = info.county_name
    # Normalize: drop " County" suffix if present
    county = county.replace(" County", "").strip()
    return county

def main():
    # 1) find Michigan events in 2025 season
    event_keys = get_mi_district_event_keys(YEAR)
    print(f"Found {len(event_keys)} MI events in {YEAR}")

    # 2) collect all teams that appeared at any MI event
    team_keys = set()
    for ek in event_keys:
        teams = get_teams_for_event(ek)
        for t in teams:
            team_keys.add(t["key"])
        time.sleep(0.2)

    print(f"Collected {len(team_keys)} unique team keys from MI events")

    # 3) get team details and extract postal code + state
    rows = []
    for tk in sorted(team_keys):
        details = get_team_details(tk)
        team_number = details.get("team_number")
        name = details.get("nickname") or details.get("name")
        city = details.get("city")
        state = details.get("state_prov")
        postal = details.get("postal_code")
        # Only include teams in Michigan (some teams may attend events out of state)
        if state and state.upper() in ("MI", "MICHIGAN"):
            rows.append({
                "team_key": tk,
                "team_number": team_number,
                "name": name,
                "city": city,
                "state": state,
                "postal_code": postal
            })

    df = pd.DataFrame(rows)
    print(f"{len(df)} teams with Michigan address in {YEAR} events")

    # 3.5) Export all team data for manual review
    df.to_csv("all_teams_2025.csv", index=False)
    print(f"Exported full team dataset to all_teams_2025.csv for manual review.")

    # 3.7) Fill missing postal_code values by inferring ZIP from city + state using pgeocode
    # Ensure nomi is available (created at module level)
    # Helper to guess ZIP from city/state
    def city_state_to_zip_guess(city, state):
        if not city or not state:
            return None
        city_norm = str(city).strip()
        state_norm = str(state).strip().upper()
        # Normalize state to 2-letter code if it's spelled out
        state_map = {"MICHIGAN": "MI", "Michigan": "MI"}
        if state_norm in state_map:
            state_norm = state_map[state_norm]
        try:
            # Query pgeocode's internal data directly
            df_places = nomi._data
            # Try exact match first (case-insensitive)
            matches = df_places[
                (df_places["place_name"].str.lower() == city_norm.lower()) &
                (df_places["state_code"] == state_norm)
            ]
            if not matches.empty:
                # Return the first postal code found
                postal_code = matches["postal_code"].iloc[0]
                if pd.notna(postal_code):
                    return str(postal_code)
            # If no exact match, try partial match
            matches = df_places[
                df_places["place_name"].str.lower().str.contains(city_norm.lower(), na=False) &
                (df_places["state_code"] == state_norm)
            ]
            if not matches.empty:
                postal_code = matches["postal_code"].iloc[0]
                if pd.notna(postal_code):
                    return str(postal_code)
            return None
        except Exception as e:
            return None

    # Only attempt for rows with NaN or missing postal_code
    mask_missing = df["postal_code"].isna()
    filled = 0

    # Debug: print a sample of missing rows before guesses
    if mask_missing.any():
        print("Sample missing rows before guessing:")
        print(df[mask_missing][["team_key", "name", "city", "state", "postal_code"]].head(10).to_string(index=False))

    for idx in df[mask_missing].index:
        guess = city_state_to_zip_guess(df.at[idx, "city"], df.at[idx, "state"])
        if guess:
            df.at[idx, "postal_code"] = guess
            filled += 1

    print(f"Step 3.7: Filled {filled} postal_code values using city/state guesses.")

    # 4) map postal codes to counties — do this before saving the review CSV
    df["county"] = df["postal_code"].apply(zip_to_county)
    # Clean common mismatches: title case, strip
    df["county"] = df["county"].str.title().str.strip()

    # Save a reviewable CSV so you can manually inspect or edit the guessed ZIPs (includes county now)
    GUESS_CSV = "all_teams_2025_with_zip_guesses.csv"
    abs_path = os.path.abspath(GUESS_CSV)
    df.to_csv(GUESS_CSV, index=False)
    print(f"Wrote {GUESS_CSV} for review at: {abs_path}")

    # Keep only rows with county and in MI
    df = df[df["county"].notna()].copy()
    print(f"{len(df)} teams mapped to counties")

    # 5) aggregate counts by county
    county_counts = df.groupby("county").agg(team_count=("team_key", "nunique")).reset_index()
    county_counts["county"] = county_counts["county"].str.replace(" County", "", regex=False)
    print(county_counts.sort_values("team_count", ascending=False).head(10))

    # 6) load Michigan counties shapefile/geojson
    gdf_counties = gpd.read_file(COUNTY_SHAPEFILE)
    # Ensure a county name field exists - try common fields
    name_fields = ["NAME", "NAME10", "COUNTYNAME", "county", "County", "Name"]
    county_name_field = next((f for f in name_fields if f in gdf_counties.columns), None)
    if not county_name_field:
        raise RuntimeError("Could not find county name field in county shapefile. Inspect columns: " + ",".join(gdf_counties.columns))
    gdf_counties["county"] = gdf_counties[county_name_field].str.title().str.strip()

    # 7) Join counts to geometry
    gdf = gdf_counties.merge(county_counts, on="county", how="left")
    gdf["team_count"] = gdf["team_count"].fillna(0).astype(int)

    # 8) Plot choropleth with team count labels
    fig, ax = plt.subplots(1, 1, figsize=(14, 12))
    gdf.plot(column="team_count", cmap="OrRd", linewidth=0.5, edgecolor="grey",
             legend=True, legend_kwds={'label': "FRC teams (2025 REEFSCAPE season)"}, ax=ax)
    
    # Add text labels with team counts in the center of each county
    for idx, row in gdf.iterrows():
        # Get the centroid (center point) of the county polygon
        centroid = row.geometry.centroid
        # Only label counties with teams (count > 0)
        if row["team_count"] > 0:
            ax.text(centroid.x, centroid.y, str(int(row["team_count"])), 
                   fontsize=9, ha='center', va='center', fontweight='bold', color='black')
    
    ax.set_title("Number of FRC teams by Michigan county — 2025 REEFSCAPE season", fontsize=14)
    ax.axis("off")
    plt.tight_layout()
    plt.savefig(OUTPUT_MAP, dpi=300)
    print(f"Saved map to {OUTPUT_MAP}")

if __name__ == "__main__":
    main()