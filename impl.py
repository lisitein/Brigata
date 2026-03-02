from typing import Any

__all__ = [
    "Handler", "UploadHandler", "JournalUploadHandler", "CategoryUploadHandler",
    "QueryHandler", "JournalQueryHandler", "CategoryQueryHandler",
    "IdentifiableEntity", "Area", "Category", "Journal", "BasicQueryEngine", "FullQueryEngine"
]

# --- Helper to create a placeholder that raises a clear error when used ---


def _missing_placeholder(name: str, module: str):
    class _Missing:
        def __init__(self, *args, **kwargs):
            raise RuntimeError(f"Cannot instantiate {name}: module '{module}' is not importable or the symbol is missing.")

        def __getattr__(self, item):
            raise RuntimeError(f"Cannot access attribute '{item}' on {name}: module '{module}' is not importable or the symbol is missing.")

    _Missing.__name__ = name
    return _Missing


# --- Import core handler base classes (required) ---
try:
    from baseHandler import Handler, UploadHandler  # type: ignore
except Exception as e:
    # Provide placeholders so importing impl.py never fails silently
    Handler = _missing_placeholder("Handler", "baseHandler")
    UploadHandler = _missing_placeholder("UploadHandler", "baseHandler")


# --- Import upload handlers (optional but expected) ---
try:
    from daniele import CategoryUploadHandler  # type: ignore
except Exception:
    CategoryUploadHandler = _missing_placeholder("CategoryUploadHandler", "daniele")

try:
    from li import JournalUploadHandler  # type: ignore
except Exception:
    JournalUploadHandler = _missing_placeholder("JournalUploadHandler", "li")


# --- Import query handlers (optional but expected) ---
try:
    from Yang import QueryHandler as QueryHandlerBase, JournalQueryHandler, CategoryQueryHandler  # type: ignore
    QueryHandler = QueryHandlerBase
except Exception:
    QueryHandler = _missing_placeholder("QueryHandler", "Yang")
    JournalQueryHandler = _missing_placeholder("JournalQueryHandler", "Yang")
    CategoryQueryHandler = _missing_placeholder("CategoryQueryHandler", "Yang")


# --- Import engine and data model (optional but expected) ---
try:
    from laura import (
        IdentifiableEntity,
        Area,
        Category,
        Journal,
        BasicQueryEngine,
        FullQueryEngine,
    )  # type: ignore
except Exception:
    IdentifiableEntity = _missing_placeholder("IdentifiableEntity", "laura")
    Area = _missing_placeholder("Area", "laura")
    Category = _missing_placeholder("Category", "laura")
    Journal = _missing_placeholder("Journal", "laura")
    BasicQueryEngine = _missing_placeholder("BasicQueryEngine", "laura")
    FullQueryEngine = _missing_placeholder("FullQueryEngine", "laura")
