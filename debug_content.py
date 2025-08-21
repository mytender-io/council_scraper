"""
Debug what content the AI is actually seeing from council websites
"""

import asyncio
import aiohttp
from bs4 import BeautifulSoup

async def check_website_content(url, name):
    """Check what's actually on a council website"""
    print(f"\n🔍 Checking {name}:")
    print(f"URL: {url}")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                print(f"Status: {response.status}")
                
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Remove scripts and styles
                    for script in soup(["script", "style"]):
                        script.decompose()
                    
                    # Get text content
                    text = soup.get_text()
                    lines = [line.strip() for line in text.splitlines() if line.strip()]
                    clean_text = ' '.join(lines)
                    
                    print(f"Content length: {len(clean_text)} characters")
                    print(f"Contains 'premises licence': {'premises licence' in clean_text.lower()}")
                    print(f"Contains 'alcohol': {'alcohol' in clean_text.lower()}")
                    print(f"Contains 'application': {'application' in clean_text.lower()}")
                    print(f"Contains 'register': {'register' in clean_text.lower()}")
                    
                    # Look for licence-related content
                    licence_keywords = ['premises licence', 'alcohol licence', 'entertainment licence', 'licence application', 'licence holder']
                    found_keywords = [kw for kw in licence_keywords if kw in clean_text.lower()]
                    print(f"Found keywords: {found_keywords}")
                    
                    # Show first 500 chars
                    print(f"First 500 chars: {clean_text[:500]}...")
                    
                    # Look for search forms
                    forms = soup.find_all('form')
                    print(f"Forms found: {len(forms)}")
                    for i, form in enumerate(forms[:3]):
                        inputs = form.find_all('input')
                        print(f"  Form {i+1}: {len(inputs)} inputs")
                        
                    # Look for tables that might contain licence data
                    tables = soup.find_all('table')
                    print(f"Tables found: {len(tables)}")
                    
                    # Look for links that might lead to licence data
                    links = soup.find_all('a', href=True)
                    licence_links = []
                    for link in links:
                        href = link.get('href', '')
                        text = link.get_text(strip=True)
                        if any(word in (href + ' ' + text).lower() for word in ['licence', 'register', 'search', 'database']):
                            licence_links.append((text[:50], href))
                    
                    if licence_links:
                        print(f"Potential licence links found: {len(licence_links)}")
                        for text, href in licence_links[:5]:
                            print(f"  - {text}: {href}")
                    
                else:
                    print(f"❌ HTTP Error: {response.status}")
                    
    except Exception as e:
        print(f"❌ Error: {e}")

async def main():
    """Debug multiple council websites"""
    councils = [
        ("Westminster City Council", "https://www.westminster.gov.uk/licensing"),
        ("Islington Council", "https://www.islington.gov.uk/business/licensing"),
        ("Hackney Council", "https://www.hackney.gov.uk/licensing"),
    ]
    
    for name, url in councils:
        await check_website_content(url, name)
        await asyncio.sleep(1)  # Be respectful

if __name__ == "__main__":
    asyncio.run(main())
