"""
Test deep navigation to find actual licence data
"""

import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urljoin

async def follow_licence_links(base_url, council_name):
    """Follow links to find actual licence data"""
    print(f"\n🔍 Deep navigation for {council_name}")
    print(f"Base URL: {base_url}")
    
    try:
        async with aiohttp.ClientSession() as session:
            # Step 1: Get main page
            async with session.get(base_url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status != 200:
                    print(f"❌ Main page failed: {response.status}")
                    return
                
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Step 2: Find most promising links
                promising_links = []
                links = soup.find_all('a', href=True)
                
                for link in links:
                    href = link.get('href')
                    text = link.get_text(strip=True).lower()
                    combined = (href + ' ' + text).lower()
                    
                    # Score links based on relevance
                    score = 0
                    if 'register' in combined: score += 3
                    if 'search' in combined: score += 2
                    if 'database' in combined: score += 3
                    if 'view' in combined and ('licence' in combined or 'application' in combined): score += 2
                    if 'premises licence' in combined: score += 4
                    if 'alcohol' in combined: score += 1
                    
                    if score >= 2:
                        full_url = urljoin(base_url, href)
                        promising_links.append((score, text[:80], full_url))
                
                # Sort by score
                promising_links.sort(key=lambda x: x[0], reverse=True)
                
                print(f"Found {len(promising_links)} promising links:")
                for score, text, url in promising_links[:5]:
                    print(f"  Score {score}: {text}")
                    print(f"    -> {url}")
                
                # Step 3: Try the top 3 links
                for score, text, url in promising_links[:3]:
                    print(f"\n🔗 Following: {text}")
                    try:
                        async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as sub_response:
                            if sub_response.status == 200:
                                sub_html = await sub_response.text()
                                sub_soup = BeautifulSoup(sub_html, 'html.parser')
                                
                                # Remove scripts and styles
                                for script in sub_soup(["script", "style"]):
                                    script.decompose()
                                
                                sub_text = sub_soup.get_text()
                                lines = [line.strip() for line in sub_text.splitlines() if line.strip()]
                                clean_text = ' '.join(lines)
                                
                                print(f"  Status: {sub_response.status}")
                                print(f"  Content length: {len(clean_text)} chars")
                                
                                # Look for actual licence data patterns
                                licence_indicators = [
                                    'premises name', 'licence holder', 'address', 'granted', 
                                    'application number', 'licence number', 'status'
                                ]
                                
                                found_data = []
                                for indicator in licence_indicators:
                                    if indicator in clean_text.lower():
                                        found_data.append(indicator)
                                
                                print(f"  Data indicators found: {found_data}")
                                
                                # Check for tables (common for licence data)
                                tables = sub_soup.find_all('table')
                                if tables:
                                    print(f"  📊 Found {len(tables)} tables")
                                    for i, table in enumerate(tables[:2]):
                                        rows = table.find_all('tr')
                                        if rows:
                                            print(f"    Table {i+1}: {len(rows)} rows")
                                            # Show first row as example
                                            first_row = rows[0]
                                            cells = [td.get_text(strip=True) for td in first_row.find_all(['td', 'th'])]
                                            if cells:
                                                print(f"    Sample: {' | '.join(cells[:4])}")
                                
                                # Check for search forms (might need to submit)
                                forms = sub_soup.find_all('form')
                                if forms:
                                    print(f"  📝 Found {len(forms)} forms")
                                    for i, form in enumerate(forms[:2]):
                                        inputs = form.find_all('input')
                                        selects = form.find_all('select')
                                        print(f"    Form {i+1}: {len(inputs)} inputs, {len(selects)} selects")
                                
                                # Show a sample of the content
                                if len(clean_text) > 1000:
                                    print(f"  Sample content: {clean_text[:500]}...")
                                
                            else:
                                print(f"  ❌ HTTP {sub_response.status}")
                                
                    except Exception as e:
                        print(f"  ❌ Error: {e}")
                    
                    await asyncio.sleep(1)  # Be respectful
                        
    except Exception as e:
        print(f"❌ Error: {e}")

async def main():
    """Test deep navigation on known councils"""
    test_councils = [
        ("Westminster City Council", "https://www.westminster.gov.uk/licensing"),
        ("Hackney Council", "https://www.hackney.gov.uk/licensing"),
    ]
    
    for name, url in test_councils:
        await follow_licence_links(url, name)
        await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(main())
