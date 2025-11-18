# count_teams_by_county_2025.py
import os
import sys
import logging
import requests
import pandas as pd
import geopandas as gpd
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend for windowed/headless environments
import matplotlib.pyplot as plt
import pgeocode
import time
import threading
import queue
try:
    import tkinter as tk
    from tkinter import scrolledtext, messagebox
    TK_AVAILABLE = True
except Exception:
    TK_AVAILABLE = False

# ----- CONFIG -----
import os

# Determine base path early so we can log and locate data both in script and bundled exe
if getattr(sys, "frozen", False):
    base_path = sys._MEIPASS
else:
    base_path = os.path.dirname(os.path.abspath(__file__))

# Configure logging to a file in the base path so windowed executables produce a log
log_file = os.path.join(base_path, "frc_teams.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
    ],
)

COUNTY_SHAPEFILE = os.path.join(base_path, "Michigan_County.geojson")  # path to Michigan counties GeoJSON or shapefile

# Globals that will be set when analysis starts
TBA_KEY = None
YEAR = None
HEADERS = None
OUTPUT_MAP = None
OUTPUT_BAR = None
STATE_FILTER = "MI"

nomi = pgeocode.Nominatim("us")

def get_user_inputs():
    """Prompt user for TBA API key and year."""
    print("\n" + "="*60)
    print("FRC Teams by Michigan County Analysis")
    print("="*60 + "\n")
    # Helper that falls back to a simple Tk dialog when stdin is not available
    def safe_input(prompt, default=None):
        try:
            return input(prompt)
        except (RuntimeError, EOFError):
            # likely running a windowed executable with no stdin; try GUI dialog
            try:
                import tkinter as tk
                from tkinter import simpledialog
                root = tk.Tk()
                root.withdraw()
                res = simpledialog.askstring("Input", prompt)
                root.destroy()
                if res is None:
                    return "" if default is None else str(default)
                return res
            except Exception:
                raise RuntimeError("No stdin available and GUI input failed.")

    # Get TBA API key
    tba_key = os.environ.get("TBA_KEY")
    if not tba_key:
        logging.info("No TBA_KEY env var found; prompting user for API key")
        tba_key = safe_input("Enter your The Blue Alliance API Key\n(Get one at https://www.thebluealliance.com/account): ").strip()
        if not tba_key:
            logging.error("TBA API key was not provided by user; exiting")
            print("ERROR: TBA API key is required!")
            sys.exit(1)
    else:
        logging.info("Using TBA_KEY from environment variable")
        print(f"✓ Using TBA_KEY from environment variable")

    # Get year (allow default)
    while True:
        try:
            year_input = safe_input("\nEnter the competition year (default 2025): ").strip()
            year = int(year_input) if year_input else 2025
            if year < 2000 or year > 2100:
                print("Please enter a reasonable year between 2000 and 2100.")
                continue
            break
        except ValueError:
            print("Please enter a valid year (e.g., 2025).")

    logging.info(f"User selected year: {year}")
    return tba_key, year

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

def main(tba_key=None, year=None):
    global TBA_KEY, YEAR, HEADERS, OUTPUT_MAP, OUTPUT_BAR
    
    # If called with no args, prompt for input
    if tba_key is None or year is None:
        tba_key, year = get_user_inputs()
    
    # Set globals so helper functions can access them
    TBA_KEY = tba_key
    YEAR = year
    HEADERS = {"X-TBA-Auth-Key": TBA_KEY}
    OUTPUT_MAP = f"mi_frc_teams_by_county_{YEAR}.png"
    OUTPUT_BAR = f"mi_frc_teams_by_county_{YEAR}_bar.png"
    
    logging.info(f"Starting analysis for year {YEAR}")
    
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
    try:
        gdf_counties = gpd.read_file(COUNTY_SHAPEFILE)
        logging.info(f"Loaded county geometry via geopandas.read_file: {COUNTY_SHAPEFILE}")
    except Exception as e:
        logging.warning(f"geopandas.read_file failed: {e}. Attempting JSON+shapely fallback.")
        # Fallback: load GeoJSON manually to avoid fiona/pyogrio/GDAL dependency issues
        try:
            import json
            from shapely.geometry import shape
            with open(COUNTY_SHAPEFILE, 'r', encoding='utf-8') as fh:
                geo = json.load(fh)
            features = geo.get('features') if isinstance(geo, dict) else None
            if not features:
                raise RuntimeError('GeoJSON does not contain FeatureCollection')
            rows = []
            for feat in features:
                props = feat.get('properties', {})
                geom = feat.get('geometry')
                try:
                    geom_obj = shape(geom) if geom else None
                except Exception:
                    geom_obj = None
                # put geometry and properties together
                props['geometry'] = geom_obj
                rows.append(props)
            gdf_counties = gpd.GeoDataFrame(rows, geometry='geometry', crs='EPSG:4326')
            logging.info('Loaded county geometries via JSON+shapely fallback')
        except Exception as e2:
            logging.exception('Failed to load county GeoJSON via fallback')
            raise RuntimeError('Failed to load county geometry: ' + str(e2))

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
    
    ax.set_title(f"Number of FRC teams by Michigan county — {YEAR} season", fontsize=14)
    ax.axis("off")
    plt.tight_layout()
    plt.savefig(OUTPUT_MAP, dpi=300)
    print(f"✓ Saved map to {OUTPUT_MAP}")
    
    print("\n" + "="*60)
    print("Analysis complete!")
    print("="*60)
    print(f"Output files:")
    print(f"  - {GUESS_CSV} (review file with guessed ZIP codes)")
    print(f"  - {OUTPUT_MAP} (choropleth map)")
    print("="*60 + "\n")

def run_with_gui():
    """Run the analysis with a Tkinter GUI status window."""
    if not TK_AVAILABLE:
        print("Tkinter not available; running in console mode.")
        main()
        return

    q = queue.Queue()

    class QueueHandler(logging.Handler):
        def emit(self, record):
            try:
                msg = self.format(record)
                q.put(msg + "\n")
            except Exception:
                pass

    # Add the queue handler to the root logger
    qh = QueueHandler()
    qh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
    logging.getLogger().addHandler(qh)

    root = tk.Tk()
    root.title("FRC Teams — Michigan County Analysis")
    root.geometry("900x700")

    # Input frame
    frm = tk.Frame(root)
    frm.pack(padx=10, pady=10, fill="x")

    tk.Label(frm, text="TBA API Key:", font=("Arial", 10)).grid(row=0, column=0, sticky="w", pady=5)
    key_var = tk.StringVar(value=os.environ.get("TBA_KEY", ""))
    key_entry = tk.Entry(frm, textvariable=key_var, width=60, show="*", font=("Arial", 10))
    key_entry.grid(row=0, column=1, sticky="we", padx=5)
    frm.columnconfigure(1, weight=1)

    tk.Label(frm, text="Year:", font=("Arial", 10)).grid(row=1, column=0, sticky="w", pady=5)
    year_var = tk.StringVar(value="2025")
    year_entry = tk.Entry(frm, textvariable=year_var, width=10, font=("Arial", 10))
    year_entry.grid(row=1, column=1, sticky="w", padx=5)

    btn_frame = tk.Frame(root)
    btn_frame.pack(padx=10, pady=5, fill="x")
    
    start_btn = tk.Button(btn_frame, text="Start Analysis", width=15, font=("Arial", 10), bg="#4CAF50", fg="white")
    start_btn.pack(side="left", padx=5)
    
    close_btn = tk.Button(btn_frame, text="Close", width=15, font=("Arial", 10), state="disabled", command=root.destroy)
    close_btn.pack(side="left", padx=5)

    # Status text widget
    tk.Label(root, text="Status:", font=("Arial", 10, "bold")).pack(anchor="w", padx=10, pady=(10, 0))
    st = scrolledtext.ScrolledText(root, width=100, height=25, state="disabled", font=("Courier", 9))
    st.pack(padx=10, pady=5, fill="both", expand=True)

    def append_text(s):
        st.configure(state="normal")
        st.insert("end", s)
        st.see("end")
        st.configure(state="disabled")
        root.update()

    def poll_queue():
        while True:
            try:
                s = q.get_nowait()
            except queue.Empty:
                break
            append_text(s)
        root.after(200, poll_queue)

    def worker(tkey, tyr):
        try:
            if tkey:
                os.environ["TBA_KEY"] = tkey
            append_text(f"Using year: {tyr}\n")
            main(tkey, int(tyr))
            logging.info("Analysis finished successfully.")
            append_text("\n" + "="*60 + "\n")
            append_text("ANALYSIS COMPLETE!\n")
            append_text("="*60 + "\n")
            root.after(0, lambda: close_btn.config(state="normal"))
            root.after(500, lambda: messagebox.showinfo("Complete", "Analysis complete! You can now close this window."))
        except Exception as e:
            logging.exception(f"Error during analysis: {e}")
            append_text(f"\nERROR: {e}\n")
            root.after(0, lambda: close_btn.config(state="normal"))

    def on_start():
        start_btn.config(state="disabled")
        key_entry.config(state="disabled")
        year_entry.config(state="disabled")
        key = key_var.get().strip()
        yr = year_var.get().strip() or "2025"
        if not key:
            messagebox.showerror("Error", "Please enter a TBA API key or set TBA_KEY environment variable.")
            start_btn.config(state="normal")
            key_entry.config(state="normal")
            year_entry.config(state="normal")
            return
        append_text("="*60 + "\n")
        append_text("Starting FRC Teams Analysis...\n")
        append_text("="*60 + "\n\n")
        t = threading.Thread(target=worker, args=(key, yr), daemon=True)
        t.start()
        poll_queue()

    start_btn.config(command=on_start)

    root.mainloop()

if __name__ == "__main__":
    # Use GUI if frozen (windowed exe) or if stdin is not a TTY (not an interactive terminal)
    if getattr(sys, "frozen", False) or not sys.stdin or not sys.stdin.isatty():
        run_with_gui()
    else:
        try:
            main()
        except KeyboardInterrupt:
            print("\n\nCancelled by user.")
            sys.exit(0)
        except Exception as e:
            print(f"\n\nERROR: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)