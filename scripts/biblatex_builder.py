"""
biblatex_builder.py

A clean utility for generating properly formatted BibLaTeX entries from structured input.
This module supports tech reports, journal articles, datasets, and books. It enforces formatting rules
for citation keys, author names, and DOIs.
"""

import re
import textwrap
from typing import Any

__all__ = [
    "generate_biblatex_article",
    "generate_biblatex_techreport",
    "generate_biblatex_dataset",
    "generate_biblatex_book",
]


def _format_biblatex_entry(
    entry_type: str, cite_key: str, fields: dict[str, Any]
) -> str:
    """
    Format a BibLaTeX entry using consistent indentation and field alignment.

    Args:
        entry_type (str): Type of BibLaTeX entry (e.g., "article", "techreport").
        cite_key (str): Unique citation key without spaces.
        fields (dict[str, str]): Dictionary of BibLaTeX fields and their values.

    Returns:
        str: A formatted BibLaTeX entry as a triple-quoted Python string.

    Raises:
        ValueError: If the cite_key contains spaces or invalid characters.
    """
    if " " in cite_key:
        raise ValueError("Citation key must not contain spaces.")
    if not re.fullmatch(r"[\w\-:.]+", cite_key):
        raise ValueError("Citation key contains invalid characters.")

    # left justify, align the '=' vertically based on max key length, and remove trailing comma
    max_key_len = max(map(len, fields))
    entry_lines = [f"@{entry_type}{{{cite_key},"]
    entry_lines += [
        f"{key.ljust(max_key_len)} = {{{value}}}," for key, value in fields.items()
    ]
    entry_lines[-1] = entry_lines[-1].rstrip(",")  # remove trailing comma
    entry_lines.append("}")

    return f'"""\n{textwrap.indent("\n".join(entry_lines), "    ")}\n"""'


def _normalize_doi(doi: str) -> str:
    """
    Normalize a DOI string to full URL form.

    Args:
        doi (str): The DOI, possibly with or without https://doi.org/ prefix.

    Returns:
        str: DOI in full URL form starting with 'https://doi.org/'.
    """
    doi = doi.strip()
    if doi.startswith("http"):
        doi = doi.split("doi.org/")[-1]
    elif doi.startswith("doi.org/"):
        doi = doi[8:]
    return f"https://doi.org/{doi}"


def _validate_and_format_authors(authors: list[str]) -> str:
    """
    Validate and format a list of authors for BibLaTeX.

    Each author must be in 'Last, First' format. The function ensures a space after
    the comma and removes spaces between initials (e.g., 'G. M.' → 'G.M.').

    Args:
        authors (list[str]): List of author names in 'Last, First' format.

    Returns:
        str: BibLaTeX-compatible string with authors joined by ' and '.

    Raises:
        ValueError: If an author is not in the expected format.
    """
    formatted = []
    for name in authors:
        if "," not in name:
            raise ValueError(
                f"Author '{name}' must be in 'Last, First' format (with a comma)."
            )

        last, first = (part.strip() for part in name.split(",", 1))
        first_clean = re.sub(r"(?<=\.)\s+(?=[A-Z]\.)", "", first)
        formatted.append(f"{last}, {first_clean}")

    return " and ".join(formatted)


def generate_biblatex_techreport(
    cite_key: str,
    author: list[str],
    title: str,
    institution: str,
    year: str,
    number: str,
) -> str:
    """
    Generate a BibLaTeX @techreport entry.

    Args:
        cite_key (str): Citation key (no spaces).
        author (str): Author string.
        title (str): Title of the report.
        institution (str): Publishing institution.
        year (str): Publication year.
        number (str): Report number or version.

    Returns:
        str: Formatted BibLaTeX @techreport entry as a multiline string.
    """
    author_str = _validate_and_format_authors(author)
    fields = {
        "author": author_str,
        "title": title,
        "institution": institution,
        "year": year,
        "number": number,
    }
    return _format_biblatex_entry("techreport", cite_key, fields)


def generate_biblatex_article(
    cite_key: str,
    author: list[str],
    title: str,
    journal: str,
    year: str,
    volume: str,
    number: str,
    pages: list[int] | str,
    doi: str | None = None,
) -> str:
    """
    Generate a BibLaTeX @article entry.

    Args:
        cite_key (str): Citation key (no spaces).
        author (list[str]): List of authors in 'Last, First' format.
        title (str): Title of the article.
        journal (str): Journal name.
        year (str): Publication year.
        volume (str): Volume number.
        number (str): Issue number.
        pages (list[int]): List of [start, end] page numbers.
        doi (str, optional): DOI identifier (with or without prefix).

    Returns:
        str: Formatted BibLaTeX @article entry as a multiline string.
    """
    author_str = _validate_and_format_authors(author)

    fields = {
        "author": author_str,
        "title": title,
        "journal": journal,
        "year": year,
        "volume": volume,
        "number": number,
    }

    # Format the page numbers
    if isinstance(pages, list):
        if len(pages) != 2:
            raise ValueError(
                "If 'pages' is a list, it must contain exactly two integers: [start, end]."
            )
        fields["pages"] = f"{pages[0]}–{pages[1]}"  # en dash between pages
    elif isinstance(pages, str):
        fields["pages"] = pages
    elif pages != " ":
        raise TypeError(
            "'pages' must be a list of two integers, a string, or an empty string."
        )

    if doi:
        fields["doi"] = _normalize_doi(doi)

    return _format_biblatex_entry("article", cite_key, fields)


def generate_biblatex_dataset(
    cite_key: str,
    author: list[str],
    title: str,
    year: str,
    url: str,
    note: str | None = None,
    doi: str | None = None,
) -> str:
    """
    Generate a BibLaTeX @misc entry for a dataset.

    Args:
        cite_key (str): Citation key (no spaces).
        author (list[str]): List of authors in 'Last, First' format.
        title (str): Title of the dataset.
        year (str): Publication or release year.
        url (str): Direct URL to the dataset.
        note (str, optional): Any additional notes.
        doi (str, optional): DOI identifier (with or without prefix).

    Returns:
        str: Formatted BibLaTeX @misc entry as a multiline string.
    """
    author_str = _validate_and_format_authors(author)
    fields = {
        "author": author_str,
        "title": title,
        "year": year,
        "url": url,
    }
    if note:
        fields["note"] = note
    if doi:
        fields["doi"] = _normalize_doi(doi)

    return _format_biblatex_entry("misc", cite_key, fields)


def generate_biblatex_book(
    cite_key: str,
    author: list[str],
    title: str,
    publisher: str,
    year: str,
    edition: str | None = None,
) -> str:
    """
    Generate a BibLaTeX @book entry.

    Args:
        cite_key (str): Citation key (no spaces).
        author (list[str]): List of authors in 'Last, First' format.
        title (str): Title of the book.
        publisher (str): Name of the publisher.
        year (str): Year of publication.
        edition (str, optional): Edition information, e.g., '2nd'.

    Returns:
        str: Formatted BibLaTeX @book entry as a multiline string.
    """
    author_str = _validate_and_format_authors(author)
    fields = {
        "author": author_str,
        "title": title,
        "publisher": publisher,
        "year": year,
    }
    if edition:
        fields["edition"] = edition

    return _format_biblatex_entry("book", cite_key, fields)
