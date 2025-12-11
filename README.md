# Setlist.fm Crawler & Search Engine

A complete web scraping, data enrichment, and search system for concert setlists from **setlist.fm**, enhanced with Wikipedia data and powered by Apache Lucene.

---

## 📋 Overview

This project implements a full pipeline for collecting, processing, and searching concert setlist data:

1. **Crawler** - BFS web crawler that discovers and downloads setlist pages from setlist.fm
2. **Extractor** - PySpark-based data extraction and Wikipedia enrichment using Spark distributed processing
3. **Indexer** - Apache Lucene index builder with multi-field indexing
4. **Search** - Full-text search engine with filtering capabilities
5. **GUI** - Desktop search interface built with PySide6

---

## 🏗️ Architecture

### Data Pipeline

```
┌──────────┐    ┌───────────┐    ┌─────────┐    ┌────────┐
│ Crawler  │ -> │ Extractor │ -> │ Indexer │ -> │ Search │
│ (BFS)    │    │ (Spark)   │    │ (Lucene)│    │  (GUI) │
└──────────┘    └───────────┘    └─────────┘    └────────┘
    │                 │
    v                 v
  htmls/      enriched_setlists.jsonl
              + Wikipedia data
```

### Technologies

- **Python 3** - Core language
- **Apache Spark (PySpark)** - Distributed data processing
- **PyLucene** - Full-text search indexing and retrieval
- **PySide6** - Desktop GUI framework
- **Docker** - Containerized execution environment
- **Wikipedia** - Data enrichment via parsed XML dumps

---

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.x (for local development)

### Installation

1. **Install dependencies:**
```cmd
pip install -r requirements.txt
```

2. **Start Docker containers:**
```cmd
cd vinf
docker-compose up -d
```

This starts two containers:
- `spark` - Apache Spark 4.0.1 with Java 21 and Python 3
- `pylucene` - PyLucene environment for indexing/searching

---

## 📦 Components

### 1. Crawler (`crawler.py`)

BFS-based web crawler that discovers and downloads setlist pages while respecting robots.txt.

**Features:**
- Breadth-first traversal from root URL
- robots.txt compliance
- Polite crawling with random delays (0.3-0.8s)
- State persistence for resume capability
- HTML storage for offline processing

**Usage:**
```cmd
python crawler.py --root https://www.setlist.fm --max-pages 1000 --max-setlists 500
```

**Arguments:**
- `--root` - Starting URL (default: https://www.setlist.fm)
- `--max-pages` - Maximum pages to crawl (default: 1000)
- `--max-setlists` - Maximum setlist pages to save (default: 200)
- `--resume` - Resume from saved state (default: True)

**Output:**
- `htmls/*.html` - Downloaded setlist pages
- `data/crawler_state.json` - Crawler state for resumption

---

### 2. Extractor (`extractor.py`)

PySpark-based extraction pipeline that:
1. Parses HTML files to extract setlist metadata
2. Enriches data with Wikipedia information (artist bios, venue details)
3. Outputs structured JSONL data

**Features:**
- Distributed HTML parsing with Spark
- Wikipedia data matching for artists and venues
- Handles multiple Wikipedia dump formats
- US state code normalization
- Genre, discography, and biography extraction

**Usage (inside Spark container):**
```bash
docker exec -it spark bash
python /opt/app/extractor.py \
  --htmls /opt/app/htmls \
  --wiki /opt/app/data/wiki_pages \
  --output /opt/app/data/enriched_setlists
```

**Arguments:**
- `--htmls` - Directory containing HTML files
- `--wiki` - Path to Wikipedia JSONL dump
- `--output` - Output directory for enriched data

**Output:**
- `enriched_setlists.jsonl` - Structured setlist data with Wikipedia enrichment

**Data Structure:**
```json
{
  "url": "https://www.setlist.fm/setlist/...",
  "artist": "Artist Name",
  "date": "July 26, 2025",
  "venue": "Venue Name",
  "city": "City",
  "country": "Country",
  "tour": "Tour Name",
  "songs_count": 38,
  "songs": ["Song 1", "Song 2", ...],
  "wiki_artist_bio": "Artist biography...",
  "wiki_artist_genre": "Rock, Pop",
  "wiki_venue_opened": "2017"
}
```

---

### 3. Indexer (`indexer.py`)

Builds Apache Lucene index from enriched JSONL data with optimized field configurations.

**Features:**
- Multi-field indexing (artist, venue, city, country, songs, tour, bio, etc.)
- Numeric range indexing for years and song counts
- Stored fields for retrieval
- Standard analyzer for text processing

**Usage (inside PyLucene container):**
```bash
docker exec -it pylucene python /app/indexer.py \
  --input /app/data/enriched_setlists.jsonl \
  --index /app/index
```

**Indexed Fields:**
- **TextField:** artist, venue, city, country, tour, songs, date_text, bio, genre, discography
- **IntPoint:** date_year, songs_count, venue_opened_year, artist_years_active
- **StringField:** url (stored only)

---

### 4. Search (`search.py`)

Command-line search interface with multi-field querying and filtering.

**Features:**
- Full-text search across all indexed fields
- Field boosting (artist: 2.0x, date: 3.0x, songs: 2.0x, etc.)
- AND operator for multi-term queries
- Year range filtering
- Song count filtering

**Usage (inside PyLucene container):**
```bash
docker exec -it pylucene python /app/search.py \
  --index /app/index \
  --query "Pink Floyd" \
  --limit 20 \
  --year-min 1970 \
  --year-max 1980
```

**Arguments:**
- `--index` - Path to Lucene index directory
- `--query` - Search query text
- `--limit` - Maximum results (default: 20)
- `--year-min` - Minimum year filter
- `--year-max` - Maximum year filter
- `--songs-min` - Minimum song count
- `--songs-max` - Maximum song count

---

### 5. GUI Search (`gui_search.py`)

Desktop GUI application for interactive searching.

**Features:**
- Real-time search with progress indication
- Year range sliders
- Song count filtering
- Results table with clickable details
- Background thread execution for responsiveness

**Usage:**
```cmd
python gui_search.py
```

**Configuration (edit in file):**
```python
DOCKER_CONTAINER_NAME = "pylucene"
SEARCH_SCRIPT_PATH = "/app/search.py"
INDEX_DIR_IN_CONTAINER = "/app/index"
```

**Interface:**
- Search bar with instant query execution
- Filter controls for year range and song count
- Results table showing: Artist, Date, Venue, City, Country, Songs, Tour
- Double-click rows for detailed JSON view

---

## 📁 Data Files

### Input
- `htmls/*.html` - Raw HTML files from crawler
- `data/wiki_pages/*.jsonl` - Wikipedia dump extracts

### Output
- `data/enriched_setlists.jsonl` - Processed and enriched setlist data
- `index/*` - Lucene index files
- `data/crawler_state.json` - Crawler state for resumption

---

## 🔍 Search Examples

### Command-line
```bash
# Search for artist
docker exec -it pylucene python /app/search.py --index /app/index --query "Metallica"

# Search with year filter
docker exec -it pylucene python /app/search.py --index /app/index --query "Beatles" --year-min 1965 --year-max 1970

# Search by venue and city
docker exec -it pylucene python /app/search.py --index /app/index --query "Madison Square Garden New York"

# Search by song
docker exec -it pylucene python /app/search.py --index /app/index --query "Bohemian Rhapsody"
```

### GUI
Simply type queries in the search bar - results update automatically with full filtering capabilities.

---

## ⚙️ Technical Details

### Polite Crawling
- User-Agent: `VINF-course-FIIT/1.0`
- Random delays: 0.3-0.8 seconds between requests
- robots.txt compliance
- Request timeout: 15-20 seconds

### Wikipedia Enrichment
- Matches artists and venues with Wikipedia articles
- Extracts: biography, genre, discography, birth name, website, origin
- Venue data: opened year, capacity, location
- Handles US state code normalization (e.g., "NY" → "New York")

### Lucene Indexing
- **Analyzer:** StandardAnalyzer
- **Document fields:** 20+ fields including metadata, enriched data, and searchable text
- **Boosting:** Artist (2.0x), Date (3.0x), Songs (2.0x), Venue/City/Country/Tour (1.5x)
- **Range queries:** Year, song count, venue opened year

---

## 📝 Requirements

```txt
requests
rich
tiktoken
PySide6
pyspark (in Spark container)
pylucene (in PyLucene container)
```



