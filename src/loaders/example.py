# src/loaders/example.py
from bs4 import BeautifulSoup
from prefect import flow

from models import DocsPage
from network_utils import fetch


def get_doc_pages_from_sitemap(base_url: str, sitemap_url: str) -> list[str]:
    sitemap_url = f"{base_url}/{sitemap_url}"
    sitemap_xml = fetch(sitemap_url)
    soup = BeautifulSoup(sitemap_xml, "xml")
    urls = []
    for loc in soup.find_all("loc"):
        url = loc.get_text()
        urls.append(url)

    return urls


def parse_docs_file(html: str, url: str) -> DocsPage:
    soup = BeautifulSoup(html, "html.parser")

    # Extract the title: Prefer an <h1> tag; if missing, use the <title> element.
    h1 = soup.find("h1")
    if h1:
        title = h1.get_text(strip=True)
    elif soup.title:
        title = soup.title.get_text(strip=True)
    else:
        title = "Untitled Document"

    # Try to extract just the main content.
    # Option 1: Look for a container that holds the MDX content.
    content_container = soup.find(attrs={"data-mdx-content": True})
    
    if content_container:
        # Remove common elements that shouldn't be in the content
        elements_to_remove = [
            "nav",  # Navigation elements
            "footer",  # Footer elements
            "aside",  # Sidebars
            ".table-of-contents",  # Table of contents
            "#navbar",  # Navigation bar
        ]
        
        for selector in elements_to_remove:
            for element in content_container.select(selector):
                element.decompose()
                
        content = content_container.get_text(separator="\n", strip=True)
    else:
        # Option 2: If no MDX container, try to extract content from <main>.
        main = soup.find("main")
        if main:
            # Remove navigation and other common elements
            for selector in [
                "nav",
                "footer",
                "aside",
                ".table-of-contents",
                "#navbar",
            ]:
                for element in main.select(selector):
                    element.decompose()
            content = main.get_text(separator="\n", strip=True)
        else:
            # Fallback: use the article tag if it exists, otherwise body
            article = soup.find("article")
            if article:
                content = article.get_text(separator="\n", strip=True)
            else:
                content = soup.body.get_text(separator="\n", strip=True)

    unique_id = f"{title}-{url}"

    return DocsPage(
        url=url,
        title=title,
        tool="Windsurf",
        content=content,
        unique_id=unique_id,
    )


@flow(log_prints=True)
def fetch_and_parse_codeium_docs(base_url: str, sitemap_url: str = "sitemap.xml"):
    urls = get_doc_pages_from_sitemap(base_url, sitemap_url)
    print(f"Found {len(urls)} doc file URLs in sitemap.")

    docs_files = []

    for url in urls:
        print("URL: ")
        print(url)
        html = fetch(url)
        doc_file = parse_docs_file(html, url)
        docs_files.append(doc_file)

    for docs_file in docs_files[:10]:
        print(docs_file.model_dump_json(indent=2))
        print("\n")

    return docs_files


if __name__ == "__main__":
    fetch_and_parse_codeium_docs(
        base_url="https://docs.windsurf.com", sitemap_url="sitemap.xml"
    )
