import lucene # type: ignore
import json
import re
from pathlib import Path

from java.nio.file import Paths # type: ignore
from org.apache.lucene.analysis.standard import StandardAnalyzer # type: ignore
from org.apache.lucene.document import ( # type: ignore
    Document, Field, StringField, TextField,
    IntPoint, StoredField
) 
from org.apache.lucene.index import IndexWriter, IndexWriterConfig # type: ignore
from org.apache.lucene.store import FSDirectory # type: ignore


lucene.initVM()

# ------------------------------------------------------
# PARSERY
# ------------------------------------------------------

def parse_years_active(years_str: str):
    # napr. "2011–present"
    if not years_str:
        return None
    m = re.match(r"(\d{4})", years_str)
    if m:
        return int(m.group(1))
    return None

def parse_opened_year(opened_str: str):
    # "{{start date|2017|04|22}}" -> 2017
    if not opened_str:
        return None
    m = re.search(r"\|\s*(\d{4})\s*\|", opened_str)
    if m:
        return int(m.group(1))
    return None


# ------------------------------------------------------
# INDEX BUILDER
# ------------------------------------------------------

def build_index(jsonl_path: str, index_dir: str):
    directory = FSDirectory.open(Paths.get(index_dir))
    analyzer = StandardAnalyzer()

    config = IndexWriterConfig(analyzer)
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)  # FIX ✔️

    writer = IndexWriter(directory, config)

    jsonl_path = Path(jsonl_path)

    with jsonl_path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):

            line = line.strip()
            if not line:
                continue

            data = json.loads(line)
            doc = Document()

            # ------------------------------------------------------
            # BASIC FIELDS
            # ------------------------------------------------------
            artist = data.get("artist") or ""
            venue = data.get("venue") or ""
            city = data.get("city") or ""
            country = data.get("country") or ""
            date_str = data.get("date") or ""
            songs_list = data.get("songs") or []
            songs_count = data.get("songs_count") or 0

            doc.add(TextField("artist", artist, Field.Store.YES))
            doc.add(StringField("artist_exact", artist, Field.Store.YES))

            doc.add(TextField("venue", venue, Field.Store.YES))
            doc.add(StringField("venue_exact", venue, Field.Store.YES))

            doc.add(TextField("city", city, Field.Store.YES))
            doc.add(StringField("city_exact", city, Field.Store.YES))

            doc.add(TextField("country", country, Field.Store.YES))
            doc.add(StringField("country_exact", country, Field.Store.YES))

            doc.add(StoredField("date", date_str))
            doc.add(TextField("date_text", date_str, Field.Store.NO))

            # extrahuj rok z "Feb 11, 2022"
            m = re.search(r"(\d{4})", date_str)
            if m:
                year = int(m.group(1))
                doc.add(IntPoint("date_year", year))
                doc.add(StoredField("date_year_store", year))

            songs_text = "\n".join(songs_list)
            doc.add(TextField("songs", songs_text, Field.Store.YES))

            doc.add(IntPoint("songs_count", int(songs_count)))
            doc.add(StoredField("songs_count_store", int(songs_count)))

            if data.get("path"):
                doc.add(StringField("path", data["path"], Field.Store.YES))
            if data.get("url"):
                doc.add(StringField("url", data["url"], Field.Store.YES))
            
            tour = data.get("tour") or ""
            if tour:
                doc.add(TextField("tour", tour, Field.Store.YES))
                doc.add(StringField("tour_exact", tour, Field.Store.YES))

            # ------------------------------------------------------
            # ARTIST SECTIONS
            # ------------------------------------------------------
            artist_bio = data.get("artist_artist_bio") or ""
            artist_discography = data.get("artist_discography") or ""

            if artist_bio:
                doc.add(TextField("artist_bio", artist_bio, Field.Store.YES))
            if artist_discography:
                doc.add(TextField("artist_discography", artist_discography, Field.Store.YES))

            # ------------------------------------------------------
            # ARTIST INFOBOX
            # ------------------------------------------------------
            artist_genre = data.get("artist_genre") or ""
            if artist_genre:
                doc.add(TextField("artist_genre", artist_genre, Field.Store.YES))
                doc.add(StringField("artist_genre_exact", artist_genre, Field.Store.YES))

            artist_origin = data.get("artist_origin") or ""
            if artist_origin:
                doc.add(TextField("artist_origin", artist_origin, Field.Store.YES))

            artist_years_active = data.get("artist_years_active") or ""
            if artist_years_active:
                doc.add(TextField("artist_years_active", artist_years_active, Field.Store.YES))
                start_year = parse_years_active(artist_years_active)
                if start_year:
                    doc.add(IntPoint("artist_years_active_from", start_year))
                    doc.add(StoredField("artist_years_active_from_store", start_year))

            artist_birth_name = data.get("artist_birth_name") or ""
            if artist_birth_name:
                doc.add(TextField("artist_birth_name", artist_birth_name, Field.Store.YES))

            artist_website = data.get("artist_website") or ""
            if artist_website:
                doc.add(TextField("artist_website", artist_website, Field.Store.YES))

            artist_current_members = data.get("artist_current_members") or ""
            if artist_current_members:
                doc.add(TextField("artist_current_members", artist_current_members, Field.Store.NO))

            # ------------------------------------------------------
            # VENUE SECTIONS & INFOBOX
            # ------------------------------------------------------
            venue_bio = data.get("venue_bio") or ""
            if venue_bio:
                doc.add(TextField("venue_bio", venue_bio, Field.Store.YES))

            venue_capacity = data.get("venue_capacity") or ""
            if venue_capacity:
                doc.add(TextField("venue_capacity", venue_capacity, Field.Store.YES))

            venue_location = data.get("venue_location") or ""
            if venue_location:
                doc.add(TextField("venue_location", venue_location, Field.Store.YES))

            venue_opened = data.get("venue_opened") or ""
            if venue_opened:
                doc.add(TextField("venue_opened", venue_opened, Field.Store.YES))
                opened_year = parse_opened_year(venue_opened)
                if opened_year:
                    doc.add(IntPoint("venue_opened_year", opened_year))
                    doc.add(StoredField("venue_opened_year_store", opened_year))

            # ------------------------------------------------------
            # CITY SECTIONS & INFOBOX
            # ------------------------------------------------------
            city_bio = data.get("city_bio") or ""
            if city_bio:
                doc.add(TextField("city_bio", city_bio, Field.Store.YES))

            city_area = data.get("city_area") or ""
            if city_area:
                doc.add(TextField("city_area", city_area, Field.Store.YES))

            city_population = data.get("city_population") or ""
            if city_population:
                doc.add(TextField("city_population", city_population, Field.Store.YES))

            # ------------------------------------------------------
            # COUNTRY SECTIONS & INFOBOX
            # ------------------------------------------------------
            country_bio = data.get("country_bio") or ""
            if country_bio:
                doc.add(TextField("country_bio", country_bio, Field.Store.YES))

            country_capital = data.get("country_capital") or ""
            if country_capital:
                doc.add(TextField("country_capital", country_capital, Field.Store.YES))

            country_area = data.get("country_area") or ""
            if country_area:
                doc.add(TextField("country_area", country_area, Field.Store.YES))

            country_population = data.get("country_population") or ""
            if country_population:
                doc.add(TextField("country_population", country_population, Field.Store.YES))

            # ------------------------------------------------------
            # WRITE DOCUMENT
            # ------------------------------------------------------
            writer.addDocument(doc)

            if i % 10000 == 0:
                print(f"Indexed {i} documents...")

    writer.close()
    directory.close()
    print("Index build complete.")


# ------------------------------------------------------
# CLI
# ------------------------------------------------------

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Build Lucene index from enriched setlist JSONL data"
    )
    parser.add_argument(
        "jsonl_path",
        help="Path to input JSONL file with enriched setlist data"
    )
    parser.add_argument(
        "index_dir",
        help="Path to output directory for Lucene index"
    )
    
    args = parser.parse_args()
    build_index(args.jsonl_path, args.index_dir)
