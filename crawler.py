import re
import json
import requests
import time
import random
import os
import argparse
from collections import deque
from urllib.parse import urljoin, urlparse, urldefrag
from rich.console import Console
from rich.table import Table

console = Console()


class UniqueDeque:
    """A deque that only allows unique items."""

    def __init__(self, iterable=None):
        self._dq = deque()
        self._set = set()
        if iterable:
            for it in iterable:
                self.append(it)

    def append(self, item):
        if item not in self._set:
            self._dq.append(item)
            self._set.add(item)

    def appendleft(self, item):
        if item not in self._set:
            self._dq.appendleft(item)
            self._set.add(item)

    def extend(self, iterable):
        for it in iterable:
            self.append(it)

    def extendleft(self, iterable):
        for it in iterable:
            self.appendleft(it)

    def popleft(self):
        item = self._dq.popleft()
        self._set.remove(item)
        return item

    def __len__(self):
        return len(self._dq)

    def __iter__(self):
        return iter(self._dq)

    def __bool__(self):
        return bool(self._dq)

    def __contains__(self, item):
        return item in self._set

    def clear(self):
        self._dq.clear()
        self._set.clear()

    def tolist(self):
        return list(self._dq)

    def __repr__(self):
        return f"UniqueDeque({list(self._dq)})"


HEADERS = {"User-Agent": "VINF-course-FIIT/1.0"}

SETLIST_RE = re.compile(
    r"^https?://[^/]*setlist\.fm/setlist/[^?#]+-[0-9a-f]{4,}\.html$", re.IGNORECASE
)
HREF_RE = re.compile(r'href\s*=\s*["\']([^"\']+)["\']', re.IGNORECASE)

STATE_FILE = "data/crawler_state.json"
HTMLS_FOLDER = "htmls"


def is_setlist_url(url: str) -> bool:
    return bool(SETLIST_RE.match(url))


def normalize_url(url: str) -> str:
    """Normalize URL by removing fragment and stripping whitespace."""
    url, _ = urldefrag(url)
    return url.strip()


def same_host(url: str, root_netloc: str) -> bool:
    """Check if the URL belongs to the same host/domain as root_netloc."""
    try:
        netloc = urlparse(url).netloc.lower()
        return netloc.endswith(root_netloc)
    except Exception:
        return False


def extract_links(html_text: str, base_url: str):
    """Extracts and normalizes all href links from the given HTML text."""
    # simple HTML href extraction via regex
    links = []
    for m in HREF_RE.finditer(html_text):
        raw = m.group(1).strip()
        if not raw or raw.startswith(("javascript:", "mailto:", "tel:")):
            continue
        abs_url = urljoin(base_url, raw)
        links.append(normalize_url(abs_url))
    # filter http/https
    clean = []
    for u in links:
        scheme = urlparse(u).scheme
        if scheme in ("http", "https"):
            clean.append(u)
    # dedupe preserve order
    out = []
    seen = set()
    for u in clean:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def load_robots_txt(root: str):
    "Loads /robots.txt and returns list of Disallow prefixes for User-agent"
    try:
        parsed = urlparse(root)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
        r = requests.get(robots_url, headers=HEADERS, timeout=10)
        if r.status_code != 200:
            return []
        disallows = []
        agent_star = False
        for line in r.text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.lower().startswith("user-agent:"):
                agent = line.split(":", 1)[1].strip()
                agent_star = agent == "*" or agent == '"*"'
            elif agent_star and line.lower().startswith("disallow:"):
                path = line.split(":", 1)[1].strip()
                if path:
                    disallows.append(path)
        return disallows
    except Exception:
        return []


def allowed_by_robots(url: str, disallows):
    """Check if URL is allowed by robots.txt disallows."""
    try:
        path = urlparse(url).path
        for d in disallows:
            if path.startswith(d):
                return False
        return True
    except Exception:
        return True


def load_crawler_state():
    """Load crawler state from disk to resume crawling."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"visited": [], "frontier": [], "found_setlists": {}, "pages_fetched": 0}


def save_crawler_state(visited, frontier, found_setlists, pages_fetched):
    """Save crawler state to disk for resumability."""
    state = {
        "visited": list(visited),
        "frontier": list(frontier),
        "found_setlists": found_setlists,
        "pages_fetched": pages_fetched,
    }
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def crawl_from_root(
    root_url: str,
    max_pages: int = 1000,
    max_setlists: int = 200,
    polite_min: float = 0.3,
    polite_max: float = 0.8,
    resume: bool = True,
):
    """
    BFS from root: fetches HTML, extracts hrefs, adds to frontier.
    When it finds a setlist page - saves the HTML to htmls folder.
    """
    os.makedirs("data", exist_ok=True)
    os.makedirs(HTMLS_FOLDER, exist_ok=True)

    parsed_root = urlparse(root_url)
    root_netloc = parsed_root.netloc.lower()
    domain_suffix = root_netloc.split(":")[0]

    robots_disallows = load_robots_txt(root_url)
    console.print(f"[yellow]Robots disallows:[/yellow] {robots_disallows}")

    # Load previous state if resuming
    if resume:
        console.print("[cyan]Attempting to resume from previous state...[/cyan]")
        state = load_crawler_state()
        visited = set(state["visited"])
        frontier = UniqueDeque(state["frontier"])
        found_setlists = state["found_setlists"]
        pages_fetched = state["pages_fetched"]
        console.print(
            f"[green]✓ Resumed:[/green] {len(visited)} visited, {len(frontier)} in frontier, {len(found_setlists)} setlists found, {pages_fetched} pages fetched"
        )

        if not frontier:
            frontier.append(normalize_url(root_url))
    else:
        console.print("[cyan]Starting fresh crawl...[/cyan]")
        visited = set()
        frontier = UniqueDeque([normalize_url(root_url)])
        found_setlists = {}
        pages_fetched = 0

    save_interval = 10  # Save state every N pages

    while frontier and pages_fetched < max_pages and len(found_setlists) < max_setlists:
        url = frontier.popleft()
        if url in visited:
            continue
        visited.add(url)

        if not same_host(url, domain_suffix):
            console.print(f"[dim yellow]⊗ [domain] SKIP:[/dim yellow] {url}")
            continue

        if not allowed_by_robots(url, robots_disallows):
            console.print(f"[dim yellow]⊗ [robots] SKIP:[/dim yellow] {url}")
            continue

        try:
            console.print(f"[blue]→ [page] GET:[/blue] {url}")
            resp = requests.get(url, headers=HEADERS, timeout=15)
            if resp.status_code >= 400:
                console.print(f"[red]✗ HTTP {resp.status_code}[/red]")
                continue
            html_text = resp.text
            pages_fetched += 1
        except Exception as e:
            console.print(f"[red]✗ ERROR fetching:[/red] {e}")
            continue

        # If it's a setlist URL, record it
        if is_setlist_url(url):
            if url not in found_setlists:
                # Generate a unique filename based on the setlist count
                setlist_number = len(found_setlists) + 1
                html_filename = f"setlist_{setlist_number}.html"
                html_path = os.path.join(HTMLS_FOLDER, html_filename)
                
                # Save the HTML content to file
                with open(html_path, "w", encoding="utf-8") as f:
                    f.write(html_text)
                
                timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
                found_setlists[url] = {
                    "discovered_at": timestamp,
                    "html_file": html_filename,
                    "status": "saved",
                }
                console.print(f"[green]✓ setlist saved:[/green] {html_filename} <- {url}")

            if len(found_setlists) >= max_setlists:
                console.print(
                    f"[yellow]! Reached max setlists limit ({max_setlists})[/yellow]"
                )
                break

        # Extract all links from the page
        links = extract_links(html_text, base_url=url)

        # Prioritize links that look relevant
        prioritized = []
        rest = []
        for link in links:
            if not same_host(link, domain_suffix):
                continue
            if link in visited:
                continue
            if not allowed_by_robots(link, robots_disallows):
                continue
            # prioritize links that look relevant
            if "/setlist/" in link or "/setlists/" in link:
                prioritized.append(link)
            else:
                rest.append(link)

        # Prioritize relevant links at the front of the queue
        for item in prioritized:
            frontier.appendleft(item)
        for item in rest:
            frontier.append(item)

        # Periodically save state
        if pages_fetched % save_interval == 0:
            console.print(
                f"[magenta]💾 Saving state...[/magenta] ({pages_fetched} pages, {len(found_setlists)} setlists)"
            )
            save_crawler_state(visited, frontier, found_setlists, pages_fetched)

        # Politeness
        time.sleep(random.uniform(polite_min, polite_max))

    # Final save
    console.print("\n[magenta]💾 Saving final state...[/magenta]")
    save_crawler_state(visited, frontier, found_setlists, pages_fetched)

    # Create summary table
    table = Table(title="Crawl Complete", show_header=True, header_style="bold magenta")
    table.add_column("Metric", style="cyan", width=20)
    table.add_column("Value", style="green", justify="right")

    table.add_row("Pages fetched", str(pages_fetched))
    table.add_row("Setlists found", str(len(found_setlists)))
    table.add_row("Visited URLs", str(len(visited)))
    table.add_row("Frontier size", str(len(frontier)))

    console.print("\n")
    console.print(table)
    console.print("\n[cyan]Output files:[/cyan]")
    console.print(f"  • {STATE_FILE}")
    console.print(f"  • {HTMLS_FOLDER}/ ({len(found_setlists)} HTML files)\n")


def print_crawler_stats():
    """Print stats about crawler_state.json and exit."""
    if not os.path.exists(STATE_FILE):
        console.print(f"[red]✗ No crawler state file found at {STATE_FILE}[/red]")
        return

    with open(STATE_FILE, "r", encoding="utf-8") as f:
        state = json.load(f)

    visited = state.get("visited", [])
    frontier = state.get("frontier", [])
    found_setlists = state.get("found_setlists", {})
    pages_fetched = state.get("pages_fetched", 0)

    console.print("[cyan]Crawler State Stats:[/cyan]")
    console.print(f"  • Visited URLs: {len(visited)}")
    console.print(f"  • Frontier size: {len(frontier)}")
    console.print(f"  • Found setlists: {len(found_setlists)}")
    console.print(f"  • Pages fetched: {pages_fetched}")


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Root-first crawler for setlist.fm (URL discovery only)"
    )
    parser.add_argument(
        "--root",
        default="https://www.setlist.fm/artists",
        help="Root URL to start from",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=100_000,
        help="Max number of HTML pages to fetch (BFS)",
    )
    parser.add_argument(
        "--max-setlists",
        type=int,
        default=20_000,
        help="Stop after collecting this many setlist pages",
    )
    parser.add_argument("--polite-min", type=float, default=0.3)
    parser.add_argument("--polite-max", type=float, default=0.8)
    parser.add_argument(
        "--no-resume", action="store_true", help="Start fresh (ignore previous state)"
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Print stats about crawler_state.json and exit",
    )

    args = parser.parse_args(argv)

    if args.stats:
        print_crawler_stats()
        return

    crawl_from_root(
        root_url=args.root,
        max_pages=args.max_pages,
        max_setlists=args.max_setlists,
        polite_min=args.polite_min,
        polite_max=args.polite_max,
        resume=not args.no_resume,
    )


if __name__ == "__main__":
    main()
