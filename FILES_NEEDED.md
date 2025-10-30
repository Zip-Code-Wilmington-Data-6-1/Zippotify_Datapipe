# 📁 **FILES NEEDED FOR SPOTIFY GENRE PROCESSING**

## 🎯 **Core Processing Files**

### **✅ ESSENTIAL FILES (Must Have)**

#### **1. Main Processing Script:**

- `spotify_genres_optimized.py` - Main processing engine
  - **Size:** 19KB
  - **Purpose:** Handles Spotify API calls, database operations, caching
  - **Dependencies:** All Python packages + local modules

#### **2. Database & Models:**

- `database.py` - Database connection configuration
  - **Content:** PostgreSQL connection string, SQLAlchemy setup
- `models.py` - Database table definitions
  - **Content:** DimSong, DimGenre, DimSongGenre, DimArtist models

#### **3. Environment Configuration:**

- `.env` - Spotify API credentials
  ```
  SPOTIPY_CLIENT_ID=c3098fc0c1df43a0a54c655e8ccf84eb
  SPOTIPY_CLIENT_SECRET=2d9ac922ca6a472f860dba199bedbc05
  ```

#### **4. Python Dependencies:**

- `requirements.txt` - All required Python packages
  - **Key packages:** spotipy, sqlalchemy, psycopg2-binary, python-dotenv
  - **Total:** 47 packages including FastAPI, pandas, etc.

---

## 🚀 **Phase Scripts (Smart Scaling)**

### **✅ EXECUTION SCRIPTS:**

- `start_phase1.sh` - Conservative test (6 concurrent requests)
- `start_phase2.sh` - Moderate scale (12 concurrent requests)
- `start_phase3.sh` - Target scale (16 concurrent requests)
- `phase_monitor.py` - Monitor progress across phases

---

## 📊 **Monitoring & Recovery Tools**

### **✅ MONITORING FILES:**

- `progress_tracker.py` - Comprehensive progress analysis
- `live_monitor.py` - Real-time dashboard
- `quick_status.py` - Fast status checks
- `quick_progress.sh` - Bash one-liner status

### **✅ RECOVERY TOOLS:**

- `emergency_recovery.py` - Stop all processes during rate limiting
- `recovery_analysis.py` - Analyze what went wrong
- `cooldown_calculator.py` - Calculate safe restart times
- `smart_scaling.py` - Create scaling strategy

---

## 💾 **Cache & Data Files**

### **✅ CACHE FILES (Performance Boosters):**

- `artist_genre_cache_0.pkl` through `artist_genre_cache_9.pkl`
  - **Purpose:** Store artist genre mappings to avoid repeat API calls
  - **Sizes:** 8KB (basic) to 51KB (heavily used processes)
  - **Content:** Pickled dictionaries of artist_name → genres

### **✅ RUNTIME FILES (Generated During Processing):**

- `phase1_*.pid`, `phase2_*.pid`, `phase3_*.pid` - Process IDs
- `phase1_*.log`, `phase2_*.log`, `phase3_*.log` - Processing logs
- `phase1_start_time.txt`, etc. - Timing files

---

## 🗂️ **File Categories by Importance**

### **🔴 CRITICAL (Cannot run without these):**

```
spotify_genres_optimized.py
database.py
models.py
.env
requirements.txt (packages installed)
```

### **🟡 EXECUTION (Needed for smart scaling):**

```
start_phase1.sh
start_phase2.sh
start_phase3.sh
phase_monitor.py
```

### **🟢 MONITORING (Helpful but optional):**

```
progress_tracker.py
live_monitor.py
quick_status.py
emergency_recovery.py
```

### **⚪ CACHE (Performance boost):**

```
artist_genre_cache_*.pkl files
```

### **🔵 LOGS (Generated during runtime):**

```
phase*_*.log files
speed_*.log files (from previous runs)
*.pid files
```

---

## 📦 **Deployment Checklist**

### **For Fresh Setup:**

✅ Copy core files: `spotify_genres_optimized.py`, `database.py`, `models.py`  
✅ Copy `.env` with valid Spotify credentials  
✅ Install packages: `pip install -r requirements.txt`  
✅ Copy phase scripts: `start_phase*.sh`, `phase_monitor.py`  
✅ Copy monitoring tools (optional): `progress_tracker.py`, etc.

### **For Existing Setup:**

✅ Verify `.env` has correct credentials  
✅ Check `artist_genre_cache_*.pkl` files exist (created by `prewarm_cache.py`)  
✅ Confirm database connection in `database.py`  
✅ Test with: `python phase_monitor.py`

---

## 💡 **Key Dependencies**

### **External Services:**

- **Spotify Web API** - Must have valid client ID/secret
- **PostgreSQL Database** - Must be running and accessible
- **Python 3.11+** - With all required packages

### **Generated Files:**

- Cache files created by first run or `prewarm_cache.py`
- Log files created automatically during processing
- PID files created by phase scripts

---

## 🎯 **Minimal Working Set**

**For basic processing (without smart scaling):**

```
spotify_genres_optimized.py
database.py
models.py
.env
requirements.txt (installed)
```

**For smart scaling approach:**

```
+ All phase scripts (start_phase*.sh)
+ phase_monitor.py
+ progress_tracker.py (recommended)
```

**Total essential files:** ~8-12 files  
**Total with monitoring:** ~20-25 files  
**All files (with logs/cache):** ~80+ files
