# search.py  (INSIDE DOCKER)
import json
import time
import lucene  # type: ignore

from java.nio.file import Paths  # type: ignore
from org.apache.lucene.store import FSDirectory  # type: ignore
from org.apache.lucene.index import DirectoryReader  # type: ignore
from org.apache.lucene.search import (  # type: ignore
    IndexSearcher,
    MatchAllDocsQuery,
    BooleanQuery,
    BooleanClause,
    BoostQuery,
)  
from org.apache.lucene.document import IntPoint  # type: ignore
from org.apache.lucene.analysis.standard import StandardAnalyzer  # type: ignore
from org.apache.lucene.queryparser.classic import QueryParser  # type: ignore
from org.apache.lucene.document import Document  # type: ignore


lucene.initVM()


class SetlistSearcher:
    def __init__(self, index_dir: str):
        directory = FSDirectory.open(Paths.get(index_dir))
        self.reader = DirectoryReader.open(directory)
        self.searcher = IndexSearcher(self.reader)
        self.analyzer = StandardAnalyzer()

    def search_fulltext(
        self,
        query_text: str,
        limit: int = 20,
        year_min: int = None,
        year_max: int = None,
        songs_min: int = None,
        songs_max: int = None,
    ) -> list[dict]:
        query_text = query_text or ""

        if query_text.strip():
            # Split query into terms and require ALL terms (anywhere across all fields)
            # Define field boosts
            field_boosts = {
                "artist": 2.0,
                "date_text": 3.0,
                "venue": 1.5,
                "city": 1.5,
                "country": 1.5,
                "tour": 1.5,
                "songs": 2.0,
            }

            fields = [
                "artist",
                "venue",
                "city",
                "country",
                "tour",
                "songs",
                "date_text",
                "artist_bio",
                "artist_discography",
                "artist_genre",
                "artist_origin",
                "artist_birth_name",
                "artist_website",
                "artist_current_members",
                "artist_genre_exactvenue_bio",
                "venue_capacity",
                "venue_location",
                "venue_opened",
                "city_bio",
                "city_area",
                "city_population",
                "country_bio",
                "country_capital",
                "country_area",
                "country_population",
            ]

            # Split query into individual terms
            terms = query_text.lower().split()

            # AND across terms (each term must appear somewhere)
            main_query = BooleanQuery.Builder()

            for term in terms:
                # OR across fields for this term (can appear in any field)
                term_query = BooleanQuery.Builder()
                for field in fields:
                    try:
                        parser = QueryParser(field, self.analyzer)
                        field_query = parser.parse(term)

                        # Apply boost if defined
                        boost = field_boosts.get(field, 1.0)
                        if boost != 1.0:
                            field_query = BoostQuery(field_query, boost)

                        term_query.add(field_query, BooleanClause.Occur.SHOULD)
                    except Exception:
                        pass
                main_query.add(term_query.build(), BooleanClause.Occur.SHOULD)

            lucene_query = main_query.build()
        else:
            lucene_query = MatchAllDocsQuery()

        # Apply range filters
        final_query = BooleanQuery.Builder()
        final_query.add(lucene_query, BooleanClause.Occur.MUST)

        if year_min is not None or year_max is not None:
            y_min = year_min if year_min is not None else 0
            y_max = year_max if year_max is not None else 9999
            year_range = IntPoint.newRangeQuery("date_year", y_min, y_max)
            final_query.add(year_range, BooleanClause.Occur.MUST)

        if songs_min is not None or songs_max is not None:
            s_min = songs_min if songs_min is not None else 0
            s_max = songs_max if songs_max is not None else 999999
            songs_range = IntPoint.newRangeQuery("songs_count", s_min, s_max)
            final_query.add(songs_range, BooleanClause.Occur.MUST)

        top_docs = self.searcher.search(final_query.build(), limit)
        results: list[dict] = []

        for score_doc in top_docs.scoreDocs:
            doc: Document = self.searcher.storedFields().document(score_doc.doc)

            # Get songs - stored as newline-separated text, not JSON
            songs_text = doc.get("songs") or ""
            songs = [s.strip() for s in songs_text.split("\n") if s.strip()]

            results.append(
                {
                    "artist": doc.get("artist"),
                    "venue": doc.get("venue"),
                    "city": doc.get("city"),
                    "country": doc.get("country"),
                    "date": doc.get("date"),
                    "songs_count": doc.get("songs_count_store"),
                    "songs": songs,
                    "url": doc.get("url"),
                    "path": doc.get("path"),
                    "tour": doc.get("tour"),
                    "artist_genre": doc.get("artist_genre"),
                    "artist_origin": doc.get("artist_origin"),
                    "artist_years_active": doc.get("artist_years_active"),
                    "artist_birth_name": doc.get("artist_birth_name"),
                    "artist_website": doc.get("artist_website"),
                    "artist_bio": doc.get("artist_bio"),
                    "artist_discography": doc.get("artist_discography"),
                    "venue_bio": doc.get("venue_bio"),
                    "venue_capacity": doc.get("venue_capacity"),
                    "venue_location": doc.get("venue_location"),
                    "venue_opened": doc.get("venue_opened"),
                    "city_bio": doc.get("city_bio"),
                    "city_area": doc.get("city_area"),
                    "city_population": doc.get("city_population"),
                    "country_bio": doc.get("country_bio"),
                    "country_capital": doc.get("country_capital"),
                    "country_area": doc.get("country_area"),
                    "country_population": doc.get("country_population"),
                    "score": score_doc.score,
                }
            )

        return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="PyLucene fulltext search (runs INSIDE Docker)"
    )
    parser.add_argument("--index-dir", required=True, help="Path to Lucene index")
    parser.add_argument("--query", default="", help="Fulltext query")
    parser.add_argument("--limit", type=int, default=20, help="Max results")
    parser.add_argument("--year-min", type=int, help="Minimum year (inclusive)")
    parser.add_argument("--year-max", type=int, help="Maximum year (inclusive)")
    parser.add_argument("--songs-min", type=int, help="Minimum song count (inclusive)")
    parser.add_argument("--songs-max", type=int, help="Maximum song count (inclusive)")

    args = parser.parse_args()

    searcher = SetlistSearcher(index_dir=args.index_dir)
    
    start_time = time.time()
    results = searcher.search_fulltext(
        query_text=args.query,
        limit=args.limit,
        year_min=args.year_min,
        year_max=args.year_max,
        songs_min=args.songs_min,
        songs_max=args.songs_max,
    )
    elapsed_time = time.time() - start_time

    print(json.dumps(results, ensure_ascii=False))
    print(f"\n⏱️  Query completed in {elapsed_time:.4f} seconds", file=__import__('sys').stderr)
