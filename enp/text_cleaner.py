import re
from typing import Dict, Optional

def clean_gmail_text(body: str) -> str:
    """Clean Gmail body: remove HTML, tracking pixels, signatures, quoted replies."""
    if not body:
        return ""
    
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', body)
    
    # Remove tracking pixels (invisible images)
    text = re.sub(r'<img[^>]*width=["\']?1["\']?[^>]*>', '', text)
    text = re.sub(r'<img[^>]*height=["\']?1["\']?[^>]*>', '', text)
    
    # Remove email signatures (common patterns)
    signature_patterns = [
        r'Best regards,.*?$',
        r'Thanks,.*?$',
        r'Sincerely,.*?$',
        r'--+\n.*?$',
        r'^\s*[A-Za-z]+ [A-Za-z]+\n[^\n]*\n[^\n]*$',  # Name + contact info
    ]
    for pattern in signature_patterns:
        text = re.sub(pattern, '', text, flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)
    
    # Remove quoted replies (lines starting with >)
    lines = text.split('\n')
    cleaned_lines = [line for line in lines if not line.strip().startswith('>')]
    text = '\n'.join(cleaned_lines)
    
    # Clean up extra whitespace
    text = re.sub(r'\n\s*\n', '\n', text)  # Multiple newlines
    text = text.strip()
    
    return text


def clean_chrome_text(html: str) -> str:
    """Clean Chrome content: extract main article text using Readability-like approach."""
    if not html:
        return ""
    
    # Remove script and style tags with content
    text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.IGNORECASE | re.DOTALL)
    
    # Remove common noise elements
    noise_patterns = [
        r'<nav[^>]*>.*?</nav>',  # Navigation
        r'<footer[^>]*>.*?</footer>',  # Footer
        r'<aside[^>]*>.*?</aside>',  # Sidebar
        r'<div[^>]*class=["\'].*?ad.*?["\'][^>]*>.*?</div>',  # Ads
        r'<!--.*?-->',  # HTML comments
    ]
    for pattern in noise_patterns:
        text = re.sub(pattern, '', text, flags=re.IGNORECASE | re.DOTALL)
    
    # Extract main article content
    article_match = re.search(r'<article[^>]*>(.*?)</article>', text, re.DOTALL | re.IGNORECASE)
    if article_match:
        text = article_match.group(1)
    else:
        # Fallback: look for main tag or content div
        main_match = re.search(r'<main[^>]*>(.*?)</main>', text, re.DOTALL | re.IGNORECASE)
        if main_match:
            text = main_match.group(1)
    
    # Remove HTML tags but preserve structure
    text = re.sub(r'<h[1-6][^>]*>([^<]*)</h[1-6]>', r'\n\n\1\n', text)
    text = re.sub(r'<p[^>]*>([^<]*)</p>', r'\1\n', text)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)  # Remove remaining tags
    
    # Decode HTML entities
    text = text.replace('&nbsp;', ' ')
    text = text.replace('&lt;', '<')
    text = text.replace('&gt;', '>')
    text = text.replace('&amp;', '&')
    text = text.replace('&quot;', '"')
    
    # Clean up whitespace
    text = re.sub(r'\n\s*\n', '\n', text)
    text = text.strip()
    
    return text


def clean_youtube_text(title: str = "", description: str = "", transcript: str = "") -> str:
    """Clean YouTube text: combine title, description, and transcript snippet."""
    parts = []
    
    if title:
        parts.append(title.strip())
    if description:
        # YouTube descriptions often have URLs, clean them
        clean_desc = re.sub(r'https?://\S+', '', description)
        parts.append(clean_desc.strip())
    if transcript:
        # Use first 500 chars of transcript
        parts.append(transcript[:500].strip())
    
    return " ".join(parts)


def clean_text(row: Dict) -> Optional[str]:
    """Main text cleaning function that routes to source-specific cleaners.
    
    Args:
        row: Dictionary with source, body/html/title, etc.
    
    Returns:
        Cleaned text string or None if unable to clean
    """
    source = row.get("source", "").lower()
    
    try:
        if source == "gmail":
            body = row.get("body", "")
            return clean_gmail_text(body)
        
        elif source == "chrome":
            html = row.get("html") or row.get("body", "")
            return clean_chrome_text(html)
        
        elif source == "youtube":
            title = row.get("title", "")
            description = row.get("description", "")
            transcript = row.get("transcript", "")
            return clean_youtube_text(title, description, transcript)
        
        else:
            # Fallback: just use string representation
            text = str(row.get("body") or row.get("title") or row)
            return text.strip() if text else None
    
    except Exception as e:
        print(f"Error cleaning text for {source}: {e}")
        return None