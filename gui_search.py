import sys
import json
import subprocess

from PySide6.QtWidgets import (
    QApplication,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLineEdit,
    QPushButton,
    QLabel,
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QDialog,
    QTextEdit,
    QSpinBox,
    QCheckBox,
)
from PySide6.QtCore import Qt, QThread, Signal


DOCKER_CONTAINER_NAME = "pylucene"
PYTHON_IN_CONTAINER = "python"
SEARCH_SCRIPT_PATH = "/app/search.py"
INDEX_DIR_IN_CONTAINER = "/app/index"


class SearchThread(QThread):
    """Background thread for search to keep UI responsive."""

    finished = Signal(list)

    def __init__(
        self,
        query: str,
        limit: int = 100,
        year_min=None,
        year_max=None,
        songs_min=None,
        songs_max=None,
    ):
        super().__init__()
        self.query = query
        self.limit = limit
        self.year_min = year_min
        self.year_max = year_max
        self.songs_min = songs_min
        self.songs_max = songs_max

    def run(self):
        results = run_docker_search(
            query=self.query,
            limit=self.limit,
            year_min=self.year_min,
            year_max=self.year_max,
            songs_min=self.songs_min,
            songs_max=self.songs_max,
        )
        self.finished.emit(results)


def run_docker_search(
    query: str,
    limit: int = 50,
    year_min=None,
    year_max=None,
    songs_min=None,
    songs_max=None,
) -> list[dict]:
    """Run search.py inside the Docker container and return results."""

    cmd = [
        "docker",
        # "compose",
        "exec",
        DOCKER_CONTAINER_NAME,
        PYTHON_IN_CONTAINER,
        SEARCH_SCRIPT_PATH,
        "--index-dir",
        INDEX_DIR_IN_CONTAINER,
        "--query",
        query,
        "--limit",
        str(limit),
    ]

    # Add optional range filters
    if year_min is not None:
        cmd.extend(["--year-min", str(year_min)])
    if year_max is not None:
        cmd.extend(["--year-max", str(year_max)])
    if songs_min is not None:
        cmd.extend(["--songs-min", str(songs_min)])
    if songs_max is not None:
        cmd.extend(["--songs-max", str(songs_max)])

    proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8")

    if proc.returncode != 0:
        print("Docker exec failed:", proc.stderr, file=sys.stderr)
        return []

    try:
        data = json.loads(proc.stdout)
        if isinstance(data, list):
            return data
        else:
            return []
    except json.JSONDecodeError:
        print("Failed to decode JSON from docker:", proc.stdout, file=sys.stderr)
        return []


class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Enhanced Setlist Search v1.0")

        self._build_ui()

    def _build_ui(self):
        main_layout = QVBoxLayout(self)

        top_row = QHBoxLayout()

        label = QLabel("Query:")
        self.query_edit = QLineEdit()
        self.query_edit.setPlaceholderText("Search artist, venue, city, songs...")

        search_button = QPushButton("Search")
        search_button.clicked.connect(self.run_search)

        self.query_edit.returnPressed.connect(self.run_search)

        top_row.addWidget(label)
        top_row.addWidget(self.query_edit, stretch=1)
        top_row.addWidget(search_button)

        # Loading spinner label
        self.loading_label = QLabel()
        self.loading_label.setFixedSize(24, 24)
        self.loading_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.loading_label.hide()
        top_row.addWidget(self.loading_label)

        # Filter row for year and song count ranges
        filter_row = QHBoxLayout()

        # Year filters
        year_label = QLabel("Year:")
        self.year_min_check = QCheckBox("Min")
        self.year_min_spin = QSpinBox()
        self.year_min_spin.setRange(1900, 2100)
        self.year_min_spin.setValue(2000)
        self.year_min_spin.setEnabled(False)
        self.year_min_check.toggled.connect(self.year_min_spin.setEnabled)

        self.year_max_check = QCheckBox("Max")
        self.year_max_spin = QSpinBox()
        self.year_max_spin.setRange(1900, 2100)
        self.year_max_spin.setValue(2025)
        self.year_max_spin.setEnabled(False)
        self.year_max_check.toggled.connect(self.year_max_spin.setEnabled)

        filter_row.addWidget(year_label)
        filter_row.addWidget(self.year_min_check)
        filter_row.addWidget(self.year_min_spin)
        filter_row.addWidget(self.year_max_check)
        filter_row.addWidget(self.year_max_spin)

        filter_row.addSpacing(20)

        # Song count filters
        songs_label = QLabel("Songs:")
        self.songs_min_check = QCheckBox("Min")
        self.songs_min_spin = QSpinBox()
        self.songs_min_spin.setRange(0, 500)
        self.songs_min_spin.setValue(1)
        self.songs_min_spin.setEnabled(False)
        self.songs_min_check.toggled.connect(self.songs_min_spin.setEnabled)

        self.songs_max_check = QCheckBox("Max")
        self.songs_max_spin = QSpinBox()
        self.songs_max_spin.setRange(0, 500)
        self.songs_max_spin.setValue(100)
        self.songs_max_spin.setEnabled(False)
        self.songs_max_check.toggled.connect(self.songs_max_spin.setEnabled)

        filter_row.addWidget(songs_label)
        filter_row.addWidget(self.songs_min_check)
        filter_row.addWidget(self.songs_min_spin)
        filter_row.addWidget(self.songs_max_check)
        filter_row.addWidget(self.songs_max_spin)

        filter_row.addStretch()

        self.table = QTableWidget(0, 9)
        self.table.setHorizontalHeaderLabels(
            [
                "Artist",
                "Venue",
                "City",
                "Country",
                "Date",
                "Tour",
                "Songs",
                "Score",
                "View",
            ]
        )

        header = self.table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(6, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(7, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(8, QHeaderView.ResizeMode.ResizeToContents)

        self.table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)

        main_layout.addLayout(top_row)
        main_layout.addLayout(filter_row)
        main_layout.addWidget(self.table, stretch=1)

    def run_search(self):
        query = self.query_edit.text()

        # Get filter values
        year_min = (
            self.year_min_spin.value() if self.year_min_check.isChecked() else None
        )
        year_max = (
            self.year_max_spin.value() if self.year_max_check.isChecked() else None
        )
        songs_min = (
            self.songs_min_spin.value() if self.songs_min_check.isChecked() else None
        )
        songs_max = (
            self.songs_max_spin.value() if self.songs_max_check.isChecked() else None
        )

        # Show loading spinner with text animation
        self.loading_label.setText("⌜")
        self.loading_label.show()

        # Start search in background thread
        self.search_thread = SearchThread(
            query,
            limit=100,
            year_min=year_min,
            year_max=year_max,
            songs_min=songs_min,
            songs_max=songs_max,
        )
        self.search_thread.finished.connect(self._on_search_finished)
        self.search_thread.start()

    def _on_search_finished(self, results: list[dict]):
        """Called when search thread completes."""
        self.loading_label.hide()
        self.results = results
        self._populate_table(self.results)

    def _populate_table(self, results: list[dict]):
        self.table.setRowCount(len(results))

        for row, r in enumerate(results):
            self.table.setItem(row, 0, QTableWidgetItem(r.get("artist") or ""))
            self.table.setItem(row, 1, QTableWidgetItem(r.get("venue") or ""))
            self.table.setItem(row, 2, QTableWidgetItem(r.get("city") or ""))
            self.table.setItem(row, 3, QTableWidgetItem(r.get("country") or ""))
            self.table.setItem(row, 4, QTableWidgetItem(r.get("date") or ""))
            self.table.setItem(row, 5, QTableWidgetItem(r.get("tour") or ""))
            self.table.setItem(
                row, 6, QTableWidgetItem(str(r.get("songs_count") or ""))
            )

            score = r.get("score")
            score_str = f"{score:.3f}" if isinstance(score, (int, float)) else ""
            self.table.setItem(row, 7, QTableWidgetItem(score_str))

            # Add view button with eye icon
            view_btn = QPushButton("👁")
            view_btn.setMaximumWidth(50)
            view_btn.clicked.connect(lambda checked, idx=row: self._show_document(idx))
            self.table.setCellWidget(row, 8, view_btn)

    def _show_document(self, row_idx: int):
        """Show full document details in a dialog."""
        if not hasattr(self, "results") or row_idx >= len(self.results):
            return

        doc = self.results[row_idx]
        dialog = QDialog(self)
        dialog.setWindowTitle(f"Setlist Details - {doc.get('artist', 'Unknown')}")
        dialog.resize(800, 600)

        layout = QVBoxLayout(dialog)

        # Create text display
        text_edit = QTextEdit()
        text_edit.setReadOnly(True)

        # Format document content
        content = f"""<h2>{doc.get("artist", "Unknown Artist")}</h2>
<p><b>Date:</b> {doc.get("date", "N/A")}</p>
<p><b>Venue:</b> {doc.get("venue", "N/A")}</p>
<p><b>City:</b> {doc.get("city", "N/A")}</p>
<p><b>Country:</b> {doc.get("country", "N/A")}</p>
"""

        if doc.get("tour"):
            content += f"<p><b>Tour:</b> {doc.get('tour')}</p>\n"

        content += f'<p><b>URL:</b> <a href="{doc.get("url", "")}">{doc.get("url", "N/A")}</a></p>\n'
        content += f"<p><b>Score:</b> {doc.get('score', 0):.3f}</p>\n"

        content += f"\n<h3>Songs ({doc.get('songs_count', 0)})</h3>\n<ol>\n"

        # Add songs
        songs = doc.get("songs", [])
        if songs:
            for song in songs:
                content += f"<li>{song}</li>\n"
        else:
            content += "<li><i>No songs available</i></li>\n"
        content += "</ol>\n"

        # Artist info from infobox
        content += "<h3>Artist Info</h3>\n"
        if doc.get("artist_genre"):
            content += f"<p><b>Genre:</b> {doc.get('artist_genre')}</p>\n"
        if doc.get("artist_origin"):
            content += f"<p><b>Origin:</b> {doc.get('artist_origin')}</p>\n"
        if doc.get("artist_years_active"):
            content += f"<p><b>Years Active:</b> {doc.get('artist_years_active')}</p>\n"
        if doc.get("artist_birth_name"):
            content += f"<p><b>Birth Name:</b> {doc.get('artist_birth_name')}</p>\n"
        if doc.get("artist_website"):
            content += f"<p><b>Website:</b> {doc.get('artist_website')}</p>\n"

        # Artist Wikipedia sections
        if doc.get("artist_bio"):
            content += f"<h3>Artist Biography</h3><p>{doc.get('artist_bio')}</p>\n"
        if doc.get("artist_discography"):
            content += f"<h3>Discography</h3><p>{doc.get('artist_discography')}</p>\n"

        # Venue info
        if (
            doc.get("venue_bio")
            or doc.get("venue_capacity")
            or doc.get("venue_location")
            or doc.get("venue_opened")
        ):
            content += "<h3>Venue Info</h3>\n"
            if doc.get("venue_bio"):
                content += f"<p><b>About Venue:</b> {doc.get('venue_bio')}</p>\n"
            if doc.get("venue_capacity"):
                content += f"<p><b>Capacity:</b> {doc.get('venue_capacity')}</p>\n"
            if doc.get("venue_location"):
                content += f"<p><b>Location:</b> {doc.get('venue_location')}</p>\n"
            if doc.get("venue_opened"):
                content += f"<p><b>Opened:</b> {doc.get('venue_opened')}</p>\n"

        # City info
        if doc.get("city_bio") or doc.get("city_area") or doc.get("city_population"):
            content += "<h3>City Info</h3>\n"
            if doc.get("city_bio"):
                content += f"<p><b>About City:</b> {doc.get('city_bio')}</p>\n"
            if doc.get("city_area"):
                content += f"<p><b>Area:</b> {doc.get('city_area')}</p>\n"
            if doc.get("city_population"):
                content += f"<p><b>Population:</b> {doc.get('city_population')}</p>\n"

        # Country info
        if (
            doc.get("country_bio")
            or doc.get("country_capital")
            or doc.get("country_area")
            or doc.get("country_population")
        ):
            content += "<h3>Country Info</h3>\n"
            if doc.get("country_bio"):
                content += f"<p><b>About Country:</b> {doc.get('country_bio')}</p>\n"
            if doc.get("country_capital"):
                content += f"<p><b>Capital:</b> {doc.get('country_capital')}</p>\n"
            if doc.get("country_area"):
                content += f"<p><b>Area:</b> {doc.get('country_area')}</p>\n"
            if doc.get("country_population"):
                content += (
                    f"<p><b>Population:</b> {doc.get('country_population')}</p>\n"
                )

        text_edit.setHtml(content)

        layout.addWidget(text_edit)

        # Add close button
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(dialog.close)
        layout.addWidget(close_btn)

        dialog.exec()


def main():
    app = QApplication(sys.argv)
    window = MainWindow()
    window.resize(1200, 700)
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
