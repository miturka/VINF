import os
import re
import html
import argparse
import xml.etree.ElementTree as ET

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel


# US state code to full state name mapping for Wikipedia lookups
US_STATE_CODES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "Washington, D.C."
}


def normalize_title(s: str) -> str:
    """Normalize string for matching (trim, normalize whitespace, lowercase)."""
    if s is None:
        return None
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s.lower()


def normalize_title_variants(s: str) -> list:
    """
    Generate normalized variants for matching Wikipedia titles with clarifiers.
    Returns list of normalized forms:
    - Full title: "Sticky Fingers (band)" -> "sticky fingers (band)"
    - Without clarifier: "Sticky Fingers (band)" -> "sticky fingers"
    """
    if s is None:
        return []
    
    variants = []
    norm_full = normalize_title(s)
    variants.append(norm_full)
    
    # Remove parenthetical clarifier: "Name (clarifier)" -> "Name"
    without_clarifier = re.sub(r'\s*\([^)]+\)\s*$', '', s)
    if without_clarifier != s:
        norm_without = normalize_title(without_clarifier)
        if norm_without != norm_full:
            variants.append(norm_without)
    
    return variants


def normalize_entity(entity_value: str, entity_type: str, country: str = None) -> str:
    if entity_value is None:
        return None
    
    # Handle USA -> United States for country lookups
    if entity_type == "country" and entity_value.upper() == "USA":
        return normalize_title("United States")
    
    # Handle US state codes as cities when country is USA
    if entity_type == "city" and country and country.upper() == "USA":
        # Check if this is a 2-letter state code
        state_code = entity_value.strip().upper()
        if state_code in US_STATE_CODES:
            return normalize_title(US_STATE_CODES[state_code])
    
    # Default normalization
    return normalize_title(entity_value)


def detect_page_type(page_text: str) -> str:
    # Patterns to detect page types from Wikipedia content
    artist_patterns = [
        r"\[\[Category:[^\]]*\b(musicians|singers|bands|rappers|musical groups|music artists)\b",
        r"{{Infobox\s+(musical artist|band|musician|singer)",
        r"\[\[Category:[^\]]*\b(rock bands|pop singers|hip hop|jazz|classical music)\b",
    ]
    venue_patterns = [
        r"\[\[Category:[^\]]*\b(music venues|concert halls|stadiums|arenas|amphitheatres)\b",
        r"{{Infobox\s+(venue|stadium|arena)",
        r"\[\[Category:[^\]]*\b(buildings and structures|entertainment venues)\b",
    ]
    city_patterns = [
        r"\[\[Category:[^\]]*\b(cities|towns|municipalities|populated places)\b",
        r"{{Infobox\s+(settlement|city|town)",
        r"\[\[Category:[^\]]*\b(capitals|county seats)\b",
    ]
    country_patterns = [
        r"\[\[Category:[^\]]*\b(countries|sovereign states|member states)\b",
        r"{{Infobox\s+country",
    ]
    
    scores = {
        "country": 0,
        "city": 0,
        "venue": 0,
        "artist": 0,
    }
    
    for pattern in country_patterns:
        if re.search(pattern, page_text, re.IGNORECASE):
            scores["country"] += 1
    
    for pattern in city_patterns:
        if re.search(pattern, page_text, re.IGNORECASE):
            scores["city"] += 1
    
    for pattern in venue_patterns:
        if re.search(pattern, page_text, re.IGNORECASE):
            scores["venue"] += 1
    
    for pattern in artist_patterns:
        if re.search(pattern, page_text, re.IGNORECASE):
            scores["artist"] += 1
    
    # Return type with highest score, or None if all scores are 0
    max_score = max(scores.values())
    if max_score > 0:
        return max(scores, key=scores.get)
    return None


# ============================================================================
# PART 1: HTML EXTRACTION
# ============================================================================


def split_venue_city_country(s):
    """Split a string of the form 'VENUE, CITY, COUNTRY' into its components."""
    if not s:
        return None, None, None
    parts = [p.strip() for p in s.split(",")]
    if len(parts) >= 3:
        return parts[0], parts[-2], parts[-1]
    if len(parts) == 2:
        return parts[0], parts[1], None
    return s, None, None


def extract_artist_and_venue(html_content, url=None):
    """Extract artist name, venue/city/country, date, tour, songs from setlist page HTML."""
    # Remove newlines for easier regex matching
    html_clean = html_content.replace("\n", " ")

    # Extract artist name from the first link in setlistHeadline
    artist_match = re.search(
        r'<div class="setlistHeadline">.*?<span><a href="[^"]*setlists/[^"]+\.html"[^>]*><span>([^<]+)</span></a></span>\s*Setlist',
        html_clean,
    )
    artist = html.unescape(artist_match.group(1).strip()) if artist_match else None

    # Extract venue/city/country from the second link
    venue_match = re.search(
        r'<span>at\s+<span><a href="[^"]*venue/[^"]+\.html"[^>]*><span>([^<]+)</span></a></span></span>',
        html_clean,
    )
    venue_full = html.unescape(venue_match.group(1).strip()) if venue_match else None
    venue, city, country = split_venue_city_country(venue_full)

    # Extract date
    date_match = re.search(
        r'<span class="month">([^<]+)</span>\s*'
        r'<span class="day">([^<]+)</span>\s*'
        r'<span class="year">([^<]+)</span>',
        html_clean,
    )
    if date_match:
        month = date_match.group(1).strip()
        day = date_match.group(2).strip()
        year = date_match.group(3).strip()
        date = f"{month} {day}, {year}"
    else:
        date = None

    # Extract tour name
    tour_match = re.search(
        r'<span>Tour:</span>\s*<span><a href="[^"]*"[^>]*><span>([^<]+)</span></a></span>',
        html_clean,
    )
    tour = html.unescape(tour_match.group(1).strip()) if tour_match else None

    # Extract songs
    songs = []
    song_matches = re.findall(
        r'<a class="songLabel" href="[^"]*"[^>]*>([^<]+)</a>', html_clean
    )
    if song_matches:
        for song in song_matches:
            song_clean = html.unescape(song.strip())
            songs.append(song_clean)

    return {
        "url": url,
        "artist": artist,
        "date": date,
        "tour": tour,
        "venue": venue,
        "city": city,
        "country": country,
        "songs_count": len(songs),
        "songs": songs,
    }


def process_html_file(path):
    """Read HTML file and extract setlist data."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            html_content = f.read()
    except Exception:
        return None

    file_name = os.path.basename(path)
    data = extract_artist_and_venue(html_content, url=file_name)
    if not data:
        return None

    data["path"] = path
    data["size_bytes"] = len(html_content.encode("utf-8"))
    return data


# ============================================================================
# PART 2: WIKIPEDIA EXTRACTION 
# ============================================================================

def get_wikitext_from_page(page_xml: str) -> str:
    """Extract the <text>...</text> wikitext from a Wikipedia <page> XML string."""
    root = ET.fromstring(page_xml)
    text_el = root.find(".//text")
    return text_el.text or ""


def normalize_headings(wikitext: str) -> str:
    """
    Ensure headings start on their own line.
    Fixes cases like '...}}</ref>== History ==' (no newline before heading).
    """
    return re.sub(r"(?<!\n)(={2,6}[^=\n]+={2,6})", r"\n\1", wikitext)

SECTION_RE = re.compile(
    r"(?:^|\n)(={2,6})\s*(?P<title>[^=\n]+?)\s*\1",
    flags=re.MULTILINE,
)

def find_section_block(wikitext: str, section_name: str) -> str | None:
    """
    Return the raw wikitext for a section with a given name (e.g. 'History').
    Includes everything until the next heading of the same or higher level.
    """
    wt = normalize_headings(wikitext)
    matches = list(SECTION_RE.finditer(wt))
    if not matches:
        return None

    target = section_name.strip().lower()
    for i, m in enumerate(matches):
        title = m.group("title")
        title_clean = title.strip().strip(" '\"").lower()
        if title_clean == target:
            level = len(m.group(1))
            start = m.end()

            end = len(wt)
            for m2 in matches[i + 1:]:
                if len(m2.group(1)) <= level:
                    end = m2.start()
                    break

            return wt[start:end].strip()

    return None


def clean_wikitext(text: str) -> str:
    """Simple wikitext → plain text cleaning."""
    if not text:
        return ""

    text = html.unescape(text)

    # Convert headings to plain text lines
    text = re.sub(
        r"(?:^|\n)(={2,6})\s*([^=\n]+?)\s*\1",
        lambda m: "\n" + m.group(2).strip() + "\n",
        text,
        flags=re.MULTILINE,
    )

    # Remove <ref>...</ref> and self-closing refs
    text = re.sub(r"<ref[^>]*>.*?</ref>", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<ref[^/>]*/>", "", text, flags=re.IGNORECASE)

    # Remove HTML comments
    text = re.sub(r"<!--.*?-->", "", text, flags=re.DOTALL)

    # Remove Wikipedia tables using depth counting
    def remove_tables(text: str) -> str:
        result = []
        i = 0
        while i < len(text):
            if i < len(text) - 1 and text[i:i+2] == "{|":
                depth = 1
                i += 2
                while i < len(text) - 1 and depth > 0:
                    if text[i:i+2] == "{|":
                        depth += 1
                        i += 2
                    elif text[i:i+2] == "|}":
                        depth -= 1
                        i += 2
                    else:
                        i += 1
            else:
                result.append(text[i])
                i += 1
        return "".join(result)
    
    text = remove_tables(text)

    # Remove file/image links [[File:...]] / [[Image:...]]
    text = re.sub(r"\[\[(?:File|Image):[^]]+\]\]", "", text, flags=re.IGNORECASE)

    # Replace [[Foo|Bar]] or [[Foo]] with visible text
    def link_repl(m):
        inner = m.group(1)
        parts = inner.split("|")
        last = parts[-1]
        last = re.sub(r"^\s*[^:]*:\s*", "", last)
        return last

    text = re.sub(r"\[\[([^]]+)\]\]", link_repl, text)

    # Extract content from list templates
    def extract_list_content(text: str) -> str:
        """Replace {{Plainlist|content}} with just the content using depth counting."""
        list_templates = ["plainlist", "flatlist", "hlist", "ubl", "nowrap", "url", "official url", "official website", 
                         "start date", "start date and age", "end date", "end date and age"]
        result = []
        i = 0
        
        while i < len(text):
            if i < len(text) - 1 and text[i:i+2] == "{{":
                match_start = i
                i += 2
                
                template_name_end = i
                while template_name_end < len(text) and text[template_name_end] not in "|}\n":
                    template_name_end += 1
                
                template_name = text[i:template_name_end].strip().lower()
                
                if template_name in list_templates:
                    i = template_name_end
                    if i < len(text) and text[i] == "|":
                        i += 1
                        
                        content_start = i
                        depth = 1
                        while i < len(text) - 1 and depth > 0:
                            if text[i:i+2] == "{{":
                                depth += 1
                                i += 2
                            elif text[i:i+2] == "}}":
                                depth -= 1
                                if depth == 0:
                                    result.append(text[content_start:i])
                                    i += 2
                                    break
                                i += 2
                            else:
                                i += 1
                    else:
                        result.append(text[match_start:i])
                else:
                    result.append(text[match_start:i])
            else:
                result.append(text[i])
                i += 1
        
        return "".join(result)
    
    text = extract_list_content(text)

    # Remove remaining templates
    def remove_templates(text: str) -> str:
        """Remove {{...}} templates using depth counting."""
        result = []
        i = 0
        while i < len(text):
            if i < len(text) - 1 and text[i:i+2] == "{{":
                depth = 1
                i += 2
                while i < len(text) - 1 and depth > 0:
                    if text[i:i+2] == "{{":
                        depth += 1
                        i += 2
                    elif text[i:i+2] == "}}":
                        depth -= 1
                        i += 2
                    else:
                        i += 1
                if depth > 0:
                    i += 1
            else:
                result.append(text[i])
                i += 1
        return "".join(result)
    
    text = remove_templates(text)

    # Remove bold/italic
    text = text.replace("'''", "").replace("''", "")

    # Remove remaining HTML tags
    text = re.sub(r"</?[^>]+>", "", text)

    # Normalize whitespace
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = "\n".join(line.strip() for line in text.splitlines())
    text = re.sub(r"[ \t]{2,}", " ", text)
    
    # Remove bullet point asterisks from wikitext lists
    text = re.sub(r"(?:^|\n)\s*\*\s+", "\n", text)
    text = re.sub(r"\*\s+", ", ", text)
    
    # Remove remaining table markup artifacts
    text = re.sub(r"!\s*scope\s*=\s*[\"']?\w+[\"']?", "", text, flags=re.IGNORECASE)
    text = re.sub(r"rowspan\s*=\s*[\"']?\d+[\"']?", "", text, flags=re.IGNORECASE)
    text = re.sub(r"colspan\s*=\s*[\"']?\d+[\"']?", "", text, flags=re.IGNORECASE)
    text = re.sub(r"class\s*=\s*[\"'][^\"']*[\"']", "", text, flags=re.IGNORECASE)
    text = re.sub(r"style\s*=\s*[\"'][^\"']*[\"']", "", text, flags=re.IGNORECASE)
    text = re.sub(r"width\s*=\s*[\"']?[^\"'\s]+[\"']?", "", text, flags=re.IGNORECASE)
    text = re.sub(r"[!+\-]{1,3}\s*", " ", text)
    
    # Clean up leftover wikitext syntax
    text = re.sub(r"\[\[", "", text)
    text = re.sub(r"\]\]", "", text)
    text = re.sub(r"\|+", ", ", text)
    text = re.sub(r",\s*,+", ",", text)
    text = re.sub(r"^\s*,\s*", "", text)
    text = re.sub(r",\s*$", "", text)

    return text.strip()


def extract_section_clean(page_xml: str, section_name: str) -> str | None:
    """Return cleaned plain-text content of the given section from a <page> XML."""
    wikitext = get_wikitext_from_page(page_xml)
    raw = find_section_block(wikitext, section_name)
    if raw is None:
        return None
    return clean_wikitext(raw)


def extract_infobox_block(wikitext: str, template_prefix: str = "Infobox") -> str | None:
    """Find the first '{{Infobox ...}}' block using brace depth counting."""
    idx = wikitext.find("{{" + template_prefix)
    if idx == -1:
        return None

    depth = 0
    i = idx
    while i < len(wikitext) - 1:
        if wikitext[i:i+2] == "{{":
            depth += 1
            i += 2
            continue
        if wikitext[i:i+2] == "}}":
            depth -= 1
            i += 2
            if depth == 0:
                return wikitext[idx:i]
            continue
        i += 1

    return None


def parse_infobox_fields(infobox_text: str) -> dict:
    """Parse an infobox into a dict {field_name: raw_value_wikitext}."""
    if not infobox_text:
        return {}

    inner = infobox_text.strip()
    if inner.startswith("{{"):
        inner = inner[2:]
    if inner.endswith("}}"):
        inner = inner[:-2]
    inner = inner.strip()

    if "|" in inner:
        _template_name, rest = inner.split("|", 1)
    else:
        return {}

    fields = {}
    parts = []
    depth = 0
    current = []
    i = 0
    while i < len(rest):
        if rest[i:i+2] == "{{":
            depth += 1
            current.append(rest[i:i+2])
            i += 2
        elif rest[i:i+2] == "}}":
            depth -= 1
            current.append(rest[i:i+2])
            i += 2
        elif rest[i] == "|" and depth == 0:
            parts.append("".join(current))
            current = []
            i += 1
        else:
            current.append(rest[i])
            i += 1
    if current:
        parts.append("".join(current))

    for part in parts:
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key:
            fields[key] = value

    return fields


def clean_infobox_value(value: str) -> str:
    """Use the same wikitext cleaner for infobox values."""
    return clean_wikitext(value)


def extract_infobox_field_clean(page_xml: str, field_name: str) -> str | None:
    """Get a specific field from the infobox (cleaned plain text)."""
    wikitext = get_wikitext_from_page(page_xml)
    ib = extract_infobox_block(wikitext, template_prefix="Infobox")
    if ib is None:
        return None
    
    fields = parse_infobox_fields(ib)
    target = field_name.strip().lower()
    for k, v in fields.items():
        if k.strip().lower() == target:
            return clean_infobox_value(v)
    return None


def process_wiki_page(row):
    """Process a Wikipedia page and extract sections/infobox based on entity type."""

    title = row.title
    page_text = row.page_text
    entity_type = row.entity_type if hasattr(row, 'entity_type') else "unknown"

    if "may refer to" in page_text.lower() or "{{disambiguation" in page_text.lower():
        return {
            "title": title,
            "title_norm": title.lower().strip(),
            "entity_type": "unknown",
            "sections": {},
            "infobox_fields": {},
        }

    if page_text.lower().startswith("#redirect"):
        return {
            "title": title,
            "title_norm": title.lower().strip(),
            "entity_type": "unknown",
            "sections": {},
            "infobox_fields": {},
        }

    result = {
        "title": title,
        "title_norm": title.lower().strip(),
        "entity_type": entity_type,
        "sections": {},
        "infobox_fields": {},
    }

    # Extract sections based on entity type
    if entity_type == "artist":
        # Collect bio sections (excluding Discography)
        bio_parts = []
        for section_name in [
            "History",
            "Background",
            "History and background",
            "Career",
            "Early life",
            "Early life and career",
            "Formation",
            "Formation and early years",
        ]:
            txt = extract_section_clean(page_text, section_name)
            if txt:
                bio_parts.append(txt)
        
        # Merge all bio sections into one
        bio = "\n\n".join(bio_parts) if bio_parts else None
        if bio:
            result["sections"]["artist_bio"] = bio
        
        # Extract Discography separately
        discography = extract_section_clean(page_text, "Discography")
        if discography:
            result["sections"]["discography"] = discography
    
    elif entity_type == "venue":
        # Collect venue bio sections
        venue_bio_parts = []
        for section_name in ["History", "Background", "Overview"]:
            txt = extract_section_clean(page_text, section_name)
            if txt:
                venue_bio_parts.append(txt)
        
        venue_bio = "\n\n".join(venue_bio_parts) if venue_bio_parts else None
        if venue_bio:
            result["sections"]["venue_bio"] = venue_bio
    
    elif entity_type == "city":
        # Collect city bio sections
        city_bio_parts = []
        for section_name in ["History", "Overview", "Geography"]:
            txt = extract_section_clean(page_text, section_name)
            if txt:
                city_bio_parts.append(txt)
        
        city_bio = "\n\n".join(city_bio_parts) if city_bio_parts else None
        if city_bio:
            result["sections"]["city_bio"] = city_bio
    
    elif entity_type == "country":
        # Collect country bio sections
        country_bio_parts = []
        for section_name in ["History", "Overview"]:
            txt = extract_section_clean(page_text, section_name)
            if txt:
                country_bio_parts.append(txt)
        
        country_bio = "\n\n".join(country_bio_parts) if country_bio_parts else None
        if country_bio:
            result["sections"]["country_bio"] = country_bio

    # Extract infobox fields based on entity type
    if entity_type == "artist":
        infobox_fields = [
            "current_members",
            "years_active",
            "genre",
            "origin",
            "birth_name",
            "website",
        ]
    elif entity_type == "venue":
        infobox_fields = ["capacity", "location", "opened"]
    elif entity_type == "city":
        infobox_fields = ["area", "population"]
    elif entity_type == "country":
        infobox_fields = ["capital", "population", "area"]
    else:
        infobox_fields = []

    for field in infobox_fields:
        value = extract_infobox_field_clean(page_text, field)
        if value:
            result["infobox_fields"][field] = value

    return result


# ============================================================================
# PART 3: MAIN PIPELINE
# ============================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Combined Spark pipeline: Extract HTML + Extract Wiki + Enrich"
    )
    parser.add_argument(
        "--html-dir",
        required=True,
        help="Input directory with .html files (e.g. data/htmls)",
    )
    parser.add_argument(
        "--wiki-dump",
        required=True,
        help="Path to Wikipedia dump .xml.bz2 file (e.g. data/wiki.xml.bz2)",
    )
    parser.add_argument(
        "--out-dir",
        required=True,
        help="Output directory for enriched dataset (e.g. data/enriched_setlists)",
    )

    args = parser.parse_args()
    html_dir = args.html_dir
    wiki_dump = args.wiki_dump
    out_dir = args.out_dir

    print("=" * 80)
    print("COMBINED SPARK PIPELINE: HTML EXTRACTION + WIKI DUMP EXTRACTION + ENRICHMENT")
    print("=" * 80)
    print(f"📁 HTML dir: {html_dir}")
    print(f"📚 Wiki dump: {wiki_dump}")
    print(f"📂 Output dir: {out_dir}")
    print()

    spark = (
        SparkSession.builder.appName("EnrichedSetlistExtractor")
        .config("spark.sql.shuffle.partitions", "32")
        .getOrCreate()
    )
    sc = spark.sparkContext

    # ========================================================================
    # STAGE 1: Extract setlists from HTML files
    # ========================================================================
    print("=" * 80)
    print("STAGE 1: EXTRACTING SETLISTS FROM HTML FILES")
    print("=" * 80)

    # Collect all .html files
    html_paths = []
    for root, dirs, files in os.walk(html_dir):
        for name in files:
            if name.endswith(".html"):
                html_paths.append(os.path.join(root, name))
    html_paths = sorted(html_paths)

    print(f"📁 Found {len(html_paths)} .html files")

    if not html_paths:
        print("⚠️ No HTML files found, exiting.")
        spark.stop()
        return

    # Parallelize and extract
    num_slices = min(32, len(html_paths))
    html_rdd = sc.parallelize(html_paths, numSlices=num_slices)
    setlist_rdd = html_rdd.map(process_html_file).filter(lambda x: x is not None)

    # Convert to DataFrame
    setlist_df = spark.createDataFrame(setlist_rdd)
    setlist_df.cache()  # Cache to avoid re-reading HTML files

    print(f"✅ Extracted {setlist_df.count()} setlists")
    print("\n📊 Sample setlist data:")
    setlist_df.select("artist", "venue", "city", "country", "date", "songs_count").show(
        5, truncate=False
    )

    # ========================================================================
    # STAGE 2: Extract Wikipedia pages from dump
    # ========================================================================
    print("\n" + "=" * 80)
    print("STAGE 2: EXTRACTING WIKIPEDIA PAGES FROM DUMP")
    print("=" * 80)

    # Collect unique entities from setlists for Wikipedia matching
    print("📊 Collecting unique entities from setlists...")
    
    def distinct_non_null(col_name: str):
        return (
            setlist_df
            .select(col_name)
            .where(F.col(col_name).isNotNull())
            .distinct()
        )

    artists_df = distinct_non_null("artist")
    venues_df = distinct_non_null("venue")
    cities_df = distinct_non_null("city")
    countries_df = distinct_non_null("country")

    print(f"  • Unique artists:   {artists_df.count()}")
    print(f"  • Unique venues:    {venues_df.count()}")
    print(f"  • Unique cities:    {cities_df.count()}")
    print(f"  • Unique countries: {countries_df.count()}")

    # Collect and normalize entity sets
    artist_set = {normalize_entity(r["artist"], "artist") for r in artists_df.collect() if r["artist"]}
    venue_set = {normalize_entity(r["venue"], "venue") for r in venues_df.collect() if r["venue"]}
    
    # For cities and countries, handle special cases (USA -> United States, state codes)
    city_country_pairs = setlist_df.select("city", "country").where(
        F.col("city").isNotNull() & F.col("country").isNotNull()
    ).distinct().collect()
    
    city_set = set()
    for row in city_country_pairs:
        wiki_name = normalize_entity(row["city"], "city", row["country"])
        if wiki_name:
            city_set.add(wiki_name)
    
    country_set = set()
    for r in countries_df.collect():
        if r["country"]:
            wiki_name = normalize_entity(r["country"], "country")
            if wiki_name:
                country_set.add(wiki_name)

    print("✅ Normalized entity sets:")
    print(f"  • artist_set:   {len(artist_set)}")
    print(f"  • venue_set:    {len(venue_set)}")
    print(f"  • city_set:     {len(city_set)}")
    print(f"  • country_set:  {len(country_set)}")

    # Broadcast entity sets to executors
    bc_artists = sc.broadcast(artist_set)
    bc_venues = sc.broadcast(venue_set)
    bc_cities = sc.broadcast(city_set)
    bc_countries = sc.broadcast(country_set)

    # Load Wikipedia dump as text RDD
    print(f"📥 Reading Wikipedia dump from {wiki_dump}...")
    wiki_rdd = sc.textFile(wiki_dump)
    print(f"  • Partitions: {wiki_rdd.getNumPartitions()}")

    # Define parser function for wiki dump
    title_re = re.compile(r"<title>(.*?)</title>")
    ns_re = re.compile(r"<ns>(\d+)</ns>")

    def parse_partition(lines_iter):
        """Parse Wikipedia XML dump partition and yield matching pages."""
        in_page = False
        buffer = []
        title = None
        is_redirect = False
        namespace = None

        for line in lines_iter:
            if "<page>" in line:
                in_page = True
                buffer = [line]
                title = None
                is_redirect = False
                namespace = None
                continue

            if in_page:
                buffer.append(line)

                if "<title>" in line and "</title>" in line:
                    m = title_re.search(line)
                    if m:
                        title = m.group(1)

                if "<redirect" in line:
                    is_redirect = True

                if "<ns>" in line and "</ns>" in line:
                    m = ns_re.search(line)
                    if m:
                        namespace = int(m.group(1))

                if "</page>" in line:
                    page_text = "".join(buffer)
                    if title and not is_redirect and namespace == 0:
                        # Filter out disambiguation pages
                        if "(disambiguation)" in title.lower():
                            in_page = False
                            buffer = []
                            title = None
                            continue

                        # Generate title variants (with and without clarifier)
                        title_variants = normalize_title_variants(title)
                        
                        # Check which entity sets contain any variant of this title
                        requested_types = []
                        for variant in title_variants:
                            if variant in bc_countries.value:
                                requested_types.append("country")
                            if variant in bc_cities.value:
                                requested_types.append("city")
                            if variant in bc_artists.value:
                                requested_types.append("artist")
                            if variant in bc_venues.value:
                                requested_types.append("venue")
                        
                        # Remove duplicates while preserving order
                        requested_types = list(dict.fromkeys(requested_types))

                        if requested_types:
                            # Detect actual page type from Wikipedia content
                            detected_type = detect_page_type(page_text)
                            
                            # Only yield if detected type matches one of the requested types
                            if detected_type is None:
                                if "city" in requested_types:
                                    detected_type = "city"
                                elif "country" in requested_types:
                                    detected_type = "country"

                            if detected_type in requested_types:
                                # Use the LAST variant (without clarifier) as normalized key
                                title_norm = title_variants[-1] if title_variants else normalize_title(title)
                                
                                yield Row(
                                    title=title,
                                    title_norm=title_norm,
                                    page_text=page_text,
                                    entity_type=detected_type,
                                )

                    # Reset
                    in_page = False
                    buffer = []
                    title = None
                    is_redirect = False
                    namespace = None

    # Extract matching pages from wiki dump
    print("🔍 Scanning Wikipedia dump for matching pages...")
    pages_rdd = wiki_rdd.mapPartitions(parse_partition)
    wiki_df = spark.createDataFrame(pages_rdd)

    # Remove duplicates and cache
    wiki_df = wiki_df.dropDuplicates(["title_norm"])
    wiki_df = wiki_df.persist(StorageLevel.MEMORY_AND_DISK)

    print(f"📚 Matched {wiki_df.count()} Wikipedia pages")
    print("📊 Entity type breakdown:")
    wiki_df.groupBy("entity_type").count().show()
    
    print(f"   • Artists: {wiki_df.filter(F.col('entity_type') == 'artist').count()}")
    print(f"   • Venues: {wiki_df.filter(F.col('entity_type') == 'venue').count()}")
    print(f"   • Cities: {wiki_df.filter(F.col('entity_type') == 'city').count()}")
    print(f"   • Countries: {wiki_df.filter(F.col('entity_type') == 'country').count()}")

    # Process Wikipedia pages to extract sections/infobox
    print("\n🔬 Extracting sections and infobox from Wikipedia pages...")
    wiki_processed_rdd = wiki_df.rdd.map(process_wiki_page)
    wiki_processed_df = spark.createDataFrame(wiki_processed_rdd)
    wiki_processed_df = wiki_processed_df.withColumn(
        "title_norm", F.lower(F.trim(F.col("title_norm")))
    )
    wiki_processed_df.cache()  # Cache processed Wikipedia data for multiple joins

    wiki_processed_df = wiki_processed_df.filter(
        F.col("entity_type") != "unknown"
    ).dropDuplicates(["entity_type", "title_norm"])

    print("✅ Wikipedia processing complete")
    print("\n📊 Sample Wikipedia data:")
    wiki_processed_df.select("title", "entity_type").show(5, truncate=False)

    # ========================================================================
    # STAGE 3: Perform left joins to enrich setlist data
    # ========================================================================
    print("\n" + "=" * 80)
    print("STAGE 3: ENRICHING SETLISTS WITH WIKIPEDIA DATA")
    print("=" * 80)

    # Create UDF for normalizing city/country values for Wikipedia matching
    def normalize_for_wiki(value, country=None):
        """Normalize values to match Wikipedia page titles."""
        if not value:
            return None
        
        value_upper = value.strip().upper()
        
        # Handle USA -> United States
        if value_upper == "USA":
            return "united states"
        
        # Handle US state codes when country is USA
        if country and country.strip().upper() == "USA" and value_upper in US_STATE_CODES:
            return US_STATE_CODES[value_upper].lower()
        
        # Default: just lowercase and trim
        return value.strip().lower()
    
    # Register UDFs
    from pyspark.sql.types import StringType
    normalize_country_udf = F.udf(lambda x: normalize_for_wiki(x), StringType())
    normalize_city_udf = F.udf(lambda city, country: normalize_for_wiki(city, country), StringType())
    
    # Normalize fields for joining
    setlist_df_norm = (
        setlist_df
        .withColumn("artist_norm", F.lower(F.trim(F.col("artist"))))
        .withColumn("venue_norm", F.lower(F.trim(F.col("venue"))))
        .withColumn("country_norm", normalize_country_udf(F.col("country")))
        .withColumn("city_norm", normalize_city_udf(F.col("city"), F.col("country")))
    )

    # Split Wikipedia pages by entity type and cache each for joins
    wiki_artists = (
        wiki_processed_df.filter(F.col("entity_type") == "artist")
        .dropDuplicates(["title_norm"])
        .cache()
    )

    wiki_venues = (
        wiki_processed_df.filter(F.col("entity_type") == "venue")
        .dropDuplicates(["title_norm"])
        .cache()
    )

    wiki_cities = (
        wiki_processed_df.filter(F.col("entity_type") == "city")
        .dropDuplicates(["title_norm"])
        .cache()
    )

    wiki_countries = (
        wiki_processed_df.filter(F.col("entity_type") == "country")
        .dropDuplicates(["title_norm"])
        .cache()
    )

    # Left join with artists
    print("\n🔗 Joining with artist data...")
    enriched_df = setlist_df_norm.join(
        wiki_artists.select(
            F.col("title_norm").alias("wiki_artist_title"),
            F.col("sections").alias("artist_sections"),
            F.col("infobox_fields").alias("artist_infobox"),
        ),
        setlist_df_norm.artist_norm == F.col("wiki_artist_title"),
        "left",
    )

    # Left join with venues
    print("🔗 Joining with venue data...")
    enriched_df = enriched_df.join(
        wiki_venues.select(
            F.col("title_norm").alias("wiki_venue_title"),
            F.col("sections").alias("venue_sections"),
            F.col("infobox_fields").alias("venue_infobox"),
        ),
        enriched_df.venue_norm == F.col("wiki_venue_title"),
        "left",
    )

    # Left join with cities
    print("🔗 Joining with city data...")
    enriched_df = enriched_df.join(
        wiki_cities.select(
            F.col("title_norm").alias("wiki_city_title"),
            F.col("sections").alias("city_sections"),
            F.col("infobox_fields").alias("city_infobox"),
        ),
        enriched_df.city_norm == F.col("wiki_city_title"),
        "left",
    )

    # Left join with countries
    print("🔗 Joining with country data...")
    enriched_df = enriched_df.join(
        wiki_countries.select(
            F.col("title_norm").alias("wiki_country_title"),
            F.col("sections").alias("country_sections"),
            F.col("infobox_fields").alias("country_infobox"),
        ),
        enriched_df.country_norm == F.col("wiki_country_title"),
        "left",
    )

    # Drop temporary normalized columns and wiki title columns
    enriched_df = enriched_df.drop(
        "artist_norm",
        "venue_norm",
        "city_norm",
        "country_norm",
        "wiki_artist_title",
        "wiki_venue_title",
        "wiki_city_title",
        "wiki_country_title",
    )

    # Flatten nested fields into root level with prefixes
    print("🔄 Flattening Wikipedia fields to root level...")
    
    # Flatten artist sections (e.g., artist_bio, artist_discography)
    for section_name in ["artist_bio", "discography"]:
        enriched_df = enriched_df.withColumn(
            f"artist_{section_name}",
            F.col("artist_sections").getItem(section_name)
        )
    
    # Flatten artist infobox (e.g., artist_genre, artist_origin)
    for field_name in ["current_members", "years_active", "genre", "origin", "birth_name", "website"]:
        enriched_df = enriched_df.withColumn(
            f"artist_{field_name}",
            F.col("artist_infobox").getItem(field_name)
        )
    
    # Flatten venue sections
    enriched_df = enriched_df.withColumn(
        "venue_bio",
        F.col("venue_sections").getItem("venue_bio")
    )
    
    # Flatten venue infobox (e.g., venue_capacity, venue_location)
    for field_name in ["capacity", "location", "opened"]:
        enriched_df = enriched_df.withColumn(
            f"venue_{field_name}",
            F.col("venue_infobox").getItem(field_name)
        )
    
    # Flatten city sections
    enriched_df = enriched_df.withColumn(
        "city_bio",
        F.col("city_sections").getItem("city_bio")
    )
    
    # Flatten city infobox (e.g., city_area, city_population)
    for field_name in ["area", "population"]:
        enriched_df = enriched_df.withColumn(
            f"city_{field_name}",
            F.col("city_infobox").getItem(field_name)
        )
    
    # Flatten country sections
    enriched_df = enriched_df.withColumn(
        "country_bio",
        F.col("country_sections").getItem("country_bio")
    )
    
    # Flatten country infobox (e.g., country_capital, country_population)
    for field_name in ["capital", "population", "area"]:
        enriched_df = enriched_df.withColumn(
            f"country_{field_name}",
            F.col("country_infobox").getItem(field_name)
        )
    
    # Drop the nested columns after flattening
    enriched_df = enriched_df.drop(
        "artist_sections",
        "artist_infobox",
        "venue_sections",
        "venue_infobox",
        "city_sections",
        "city_infobox",
        "country_sections",
        "country_infobox",
    )

    print("✅ Joins complete")

    # ========================================================================
    # STAGE 4: Save results
    # ========================================================================
    print("\n" + "=" * 80)
    print("STAGE 4: SAVING ENRICHED DATASET")
    print("=" * 80)

    # Show statistics
    total_setlists = enriched_df.count()
    enriched_artists = enriched_df.filter(F.col("artist_artist_bio").isNotNull()).count()
    enriched_venues = enriched_df.filter(F.col("venue_capacity").isNotNull()).count()
    enriched_cities = enriched_df.filter(F.col("city_population").isNotNull()).count()
    enriched_countries = enriched_df.filter(
        F.col("country_capital").isNotNull()
    ).count()

    print("\n📊 Enrichment statistics:")
    print(f"  • Total setlists: {total_setlists}")
    print(
        f"  • Enriched with artist data: {enriched_artists} ({100 * enriched_artists / total_setlists:.1f}%)"
    )
    print(
        f"  • Enriched with venue data: {enriched_venues} ({100 * enriched_venues / total_setlists:.1f}%)"
    )
    print(
        f"  • Enriched with city data: {enriched_cities} ({100 * enriched_cities / total_setlists:.1f}%)"
    )
    print(
        f"  • Enriched with country data: {enriched_countries} ({100 * enriched_countries / total_setlists:.1f}%)"
    )

    # Show sample
    print("\n📊 Sample enriched data:")
    enriched_df.select(
        "artist",
        "venue",
        "city",
        "country",
        "date",
        "songs_count",
        "artist_genre",
        "artist_origin",
        "venue_capacity",
        "city_population",
    ).show(3, truncate=True)

    # Save as Spark JSON dataset
    print(f"\n💾 Writing output to {out_dir} as JSON dataset...")
    enriched_df.write.mode("overwrite").json(out_dir)

    # Also save as single JSON file for convenience
    single_file = out_dir.rstrip("/").rstrip("\\") + "_single.json"
    print(f"💾 Writing single JSON file to {single_file}...")

    single_dir = out_dir.rstrip("/").rstrip("\\") + "_single"

    print(f"💾 Writing single JSON (one-partition dataset) to {single_dir}...")
    (enriched_df.coalesce(1).write.mode("overwrite").json(single_dir))

    print("\n" + "=" * 80)
    print("✅ PIPELINE COMPLETE!")
    print("=" * 80)
    print(f"📂 Spark dataset: {out_dir}")
    print(f"📄 Single JSON: {single_file}")

    # Unpersist cached DataFrames to free memory
    setlist_df.unpersist()
    wiki_df.unpersist()
    wiki_processed_df.unpersist()
    wiki_artists.unpersist()
    wiki_venues.unpersist()
    wiki_cities.unpersist()
    wiki_countries.unpersist()
    enriched_df.unpersist()

    input("\nPress Enter to stop Spark...")
    spark.stop()
    print("✅ Spark stopped cleanly")


if __name__ == "__main__":
    main()
