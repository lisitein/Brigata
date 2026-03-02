from pathlib import Path
from typing import Optional

class Handler:
    def __init__(self) -> None:
        self.dbPathOrUrl: str = ""

    def getDbPathOrUrl(self) -> str:
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, pathOrUrl: str) -> bool:
        if pathOrUrl is None:
            return False
        self.dbPathOrUrl = str(pathOrUrl)
        return True


class UploadHandler(Handler):
    """
    Generic upload dispatcher.

    - Verifies that a DB path/URL is set.
    - Chooses the correct concrete upload handler based on file extension.
    - Imports concrete handlers dynamically to avoid circular imports.
    - Returns True on success, False on failure.
    """

    def __init__(self) -> None:
        super().__init__()

    def _choose_handler_class(self, suffix: str):
        """
        Map file suffix to the concrete handler class.
        Import modules dynamically to avoid circular imports at module load time.
        """
        s = suffix.lower()
        if s == ".csv":
            # CSV -> graph journals (li.py)
            from li import JournalUploadHandler
            return JournalUploadHandler
        if s == ".json":
            # JSON -> relational categories/areas (daniele.py)
            from daniele import CategoryUploadHandler
            return CategoryUploadHandler
        # add other mappings here if needed (e.g., .ttl, .rdf)
        return None

    def pushDataToDb(self, path: str) -> bool:
        db_path = self.getDbPathOrUrl()
        if not db_path:
            print("Error: No database path or URL provided. Call setDbPathOrUrl() first.")
            return False

        if not path:
            print("Error: No file path provided.")
            return False

        p = Path(path)
        if not p.exists():
            print(f"Error: File not found: {path}")
            return False

        suffix = p.suffix
        handler_cls = self._choose_handler_class(suffix)
        if handler_cls is None:
            print(f"Error: Unsupported file format: {suffix}")
            return False

        try:
            handler = handler_cls()
            # pass the same DB path/URL to the concrete handler
            handler.setDbPathOrUrl(db_path)
            result = handler.pushDataToDb(path)
            if result is True:
                return True
            # if handler returns a DataFrame or other truthy value, treat as success
            if result:
                return True
            return False
        except Exception as e:
            # keep the message concise but informative for debugging
            print(f"Error while pushing data to DB: {e}")
            return False
