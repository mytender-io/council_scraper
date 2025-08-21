"""
UK Council Discovery Module

This module discovers and catalogs all UK council websites and their premises licence registers.
It uses various sources including gov.uk data, local government directories, and AI-assisted discovery.
"""

import json
import asyncio
import aiohttp
from typing import List, Dict, Optional, Set
from datetime import datetime
import logging
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from langchain_openai import ChatOpenAI
from langchain.schema import HumanMessage

from .models import Council
from .config import get_settings

logger = logging.getLogger(__name__)


class CouncilDiscovery:
    """Discovers UK councils and their licence registers"""
    
    def __init__(self):
        self.settings = get_settings()
        self.llm = ChatOpenAI(
            model_name=self.settings.openai_model,
            api_key=self.settings.openai_api_key,
            temperature=0
        )
        self.councils: List[Council] = []
        
    def discover_councils(self) -> List[Council]:
        """Main method to discover all UK councils"""
        logger.info("Starting UK council discovery")
        
        # Load from multiple sources
        councils_data = []
        
        # Source 1: UK Government Open Data
        councils_data.extend(self._load_from_gov_uk())
        
        # Source 2: Local Government Association
        councils_data.extend(self._load_from_lga())
        
        # Source 3: Hardcoded known councils (fallback)
        councils_data.extend(self._load_hardcoded_councils())
        
        # Deduplicate and create Council objects
        unique_councils = self._deduplicate_councils(councils_data)
        
        self.councils = [Council(**council_data) for council_data in unique_councils]
        
        logger.info(f"Discovered {len(self.councils)} councils")
        return self.councils
    
    def _load_from_gov_uk(self) -> List[Dict]:
        """Load council data from UK Government open data sources"""
        logger.info("Loading councils from gov.uk sources")
        councils = []
        
        try:
            # UK Government register of local authorities
            gov_data_urls = [
                "https://local-authority-eng.register.gov.uk/records.json",
                "https://local-authority-sct.register.gov.uk/records.json", 
                "https://local-authority-wls.register.gov.uk/records.json",
            ]
            
            for url in gov_data_urls:
                try:
                    response = requests.get(url, timeout=30)
                    if response.status_code == 200:
                        data = response.json()
                        for key, record in data.items():
                            entry = record.get('entry', {})
                            if entry:
                                council = {
                                    'name': entry.get('official-name', entry.get('name', '')),
                                    'code': entry.get('local-authority-eng', entry.get('local-authority-sct', entry.get('local-authority-wls', key))),
                                    'website_url': self._construct_website_url(entry.get('official-name', '')),
                                    'region': self._determine_region_from_url(url)
                                }
                                if council['website_url']:
                                    councils.append(council)
                except Exception as e:
                    logger.warning(f"Failed to load from {url}: {e}")
                    
        except Exception as e:
            logger.warning(f"Failed to load gov.uk data: {e}")
            
        logger.info(f"Loaded {len(councils)} councils from gov.uk sources")
        return councils
    
    def _load_from_lga(self) -> List[Dict]:
        """Load council data from Local Government Association"""
        logger.info("Loading councils from LGA sources")
        councils = []
        
        try:
            # Scrape LGA council directory
            lga_url = "https://www.local.gov.uk/our-support/guidance-and-resources/communications-support/digital-councils/social-media/go-further/a-z-councils"
            
            response = requests.get(lga_url, timeout=30)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Look for council links
                links = soup.find_all('a', href=True)
                for link in links:
                    href = link.get('href', '')
                    text = link.get_text(strip=True)
                    
                    if self._is_council_link(href, text):
                        council = {
                            'name': text,
                            'website_url': href if href.startswith('http') else f"https://{href}",
                            'region': 'Unknown'
                        }
                        councils.append(council)
                        
        except Exception as e:
            logger.warning(f"Failed to load LGA data: {e}")
            
        logger.info(f"Loaded {len(councils)} councils from LGA sources")
        return councils
    
    def _load_hardcoded_councils(self) -> List[Dict]:
        """Load hardcoded council data as fallback"""
        logger.info("Loading hardcoded council data")
        
        # Major UK councils with known premises licence registers
        hardcoded_councils = [
            # London Boroughs
            {'name': 'Westminster City Council', 'website_url': 'https://www.westminster.gov.uk', 'region': 'London', 'code': 'WCC'},
            {'name': 'Camden Council', 'website_url': 'https://www.camden.gov.uk', 'region': 'London', 'code': 'CAM'},
            {'name': 'Islington Council', 'website_url': 'https://www.islington.gov.uk', 'region': 'London', 'code': 'ISL'},
            {'name': 'Hackney Council', 'website_url': 'https://www.hackney.gov.uk', 'region': 'London', 'code': 'HAC'},
            {'name': 'Tower Hamlets Council', 'website_url': 'https://www.towerhamlets.gov.uk', 'region': 'London', 'code': 'TWH'},
            {'name': 'Southwark Council', 'website_url': 'https://www.southwark.gov.uk', 'region': 'London', 'code': 'SWK'},
            
            # Major Cities
            {'name': 'Manchester City Council', 'website_url': 'https://www.manchester.gov.uk', 'region': 'North West', 'code': 'MCC'},
            {'name': 'Birmingham City Council', 'website_url': 'https://www.birmingham.gov.uk', 'region': 'West Midlands', 'code': 'BCC'},
            {'name': 'Leeds City Council', 'website_url': 'https://www.leeds.gov.uk', 'region': 'Yorkshire', 'code': 'LCC'},
            {'name': 'Liverpool City Council', 'website_url': 'https://www.liverpool.gov.uk', 'region': 'North West', 'code': 'LIV'},
            {'name': 'Bristol City Council', 'website_url': 'https://www.bristol.gov.uk', 'region': 'South West', 'code': 'BRI'},
            {'name': 'Newcastle City Council', 'website_url': 'https://www.newcastle.gov.uk', 'region': 'North East', 'code': 'NEW'},
            {'name': 'Sheffield City Council', 'website_url': 'https://www.sheffield.gov.uk', 'region': 'Yorkshire', 'code': 'SHE'},
            
            # Scotland
            {'name': 'Glasgow City Council', 'website_url': 'https://www.glasgow.gov.uk', 'region': 'Scotland', 'code': 'GLA'},
            {'name': 'Edinburgh Council', 'website_url': 'https://www.edinburgh.gov.uk', 'region': 'Scotland', 'code': 'EDI'},
            
            # Wales  
            {'name': 'Cardiff Council', 'website_url': 'https://www.cardiff.gov.uk', 'region': 'Wales', 'code': 'CAR'},
            {'name': 'Swansea Council', 'website_url': 'https://www.swansea.gov.uk', 'region': 'Wales', 'code': 'SWA'},
        ]
        
        logger.info(f"Loaded {len(hardcoded_councils)} hardcoded councils")
        return hardcoded_councils
    
    def _construct_website_url(self, council_name: str) -> Optional[str]:
        """Construct likely website URL from council name"""
        if not council_name:
            return None
            
        # Clean the name
        name = council_name.lower()
        name = name.replace(' council', '').replace(' city', '').replace(' borough', '')
        name = name.replace(' district', '').replace(' metropolitan', '').replace(' royal', '')
        name = name.replace(' ', '').replace('-', '').replace("'", '')
        
        # Common URL patterns
        patterns = [
            f"https://www.{name}.gov.uk",
            f"https://www.{name}council.gov.uk", 
            f"https://www.{name}city.gov.uk",
            f"https://www.{name}.co.uk",
        ]
        
        return patterns[0]  # Return the most likely pattern
    
    def _determine_region_from_url(self, url: str) -> str:
        """Determine region from source URL"""
        if 'eng' in url:
            return 'England'
        elif 'sct' in url:
            return 'Scotland'
        elif 'wls' in url:
            return 'Wales'
        return 'Unknown'
    
    def _is_council_link(self, href: str, text: str) -> bool:
        """Check if a link appears to be a council website"""
        council_indicators = [
            'council', 'borough', 'city', 'district', 'county',
            '.gov.uk', 'local', 'authority'
        ]
        
        combined_text = (href + ' ' + text).lower()
        return any(indicator in combined_text for indicator in council_indicators)
    
    def _deduplicate_councils(self, councils_data: List[Dict]) -> List[Dict]:
        """Remove duplicate councils based on name similarity"""
        unique_councils = {}
        
        for council in councils_data:
            name = council.get('name', '').lower().strip()
            if not name:
                continue
                
            # Normalize name for deduplication
            normalized_name = name.replace(' council', '').replace(' city council', '')
            normalized_name = normalized_name.replace(' borough council', '').replace(' district council', '')
            
            # Keep the most complete version
            if normalized_name not in unique_councils:
                unique_councils[normalized_name] = council
            else:
                # Keep the one with more information
                existing = unique_councils[normalized_name]
                if len(str(council)) > len(str(existing)):
                    unique_councils[normalized_name] = council
        
        return list(unique_councils.values())
    
    async def discover_licence_registers(self) -> List[Council]:
        """Discover premises licence registers for each council using AI"""
        logger.info("Discovering premises licence registers using AI")
        
        if not self.councils:
            self.discover_councils()
        
        # Process councils in batches
        batch_size = 10
        for i in range(0, len(self.councils), batch_size):
            batch = self.councils[i:i+batch_size]
            await self._process_council_batch(batch)
            
            # Save progress after each batch
            self.save_councils_data()
            
            # Rate limiting
            await asyncio.sleep(2)
        
        return self.councils
    
    async def _process_council_batch(self, councils: List[Council]):
        """Process a batch of councils to find their licence registers"""
        tasks = []
        
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            for council in councils:
                task = self._find_licence_register(session, council)
                tasks.append(task)
            
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _find_licence_register(self, session: aiohttp.ClientSession, council: Council):
        """Find premises licence register for a specific council"""
        try:
            logger.info(f"Searching licence register for {council.name}")
            
            # Try to load the council's homepage
            async with session.get(str(council.website_url)) as response:
                if response.status == 200:
                    html_content = await response.text()
                    
                    # Use AI to analyze the website and find licence register
                    licence_url = await self._ai_find_licence_register(council, html_content[:10000])  # Limit content size
                    
                    if licence_url:
                        council.licence_register_url = licence_url
                        council.scrape_successful = True
                        logger.info(f"Found licence register for {council.name}: {licence_url}")
                    else:
                        council.scrape_successful = False
                        council.error_message = "No licence register found"
                        
                else:
                    council.scrape_successful = False
                    council.error_message = f"Website returned status {response.status}"
                    
        except Exception as e:
            logger.error(f"Error processing {council.name}: {e}")
            council.scrape_successful = False
            council.error_message = str(e)
        
        council.last_scraped = datetime.now()
    
    async def _ai_find_licence_register(self, council: Council, html_content: str) -> Optional[str]:
        """Use AI to find the premises licence register URL"""
        try:
            prompt = f"""
You are helping to find premises licence registers on UK council websites.

Council: {council.name}
Website: {council.website_url}

Here is the HTML content from their homepage (truncated):
{html_content}

Please analyze this content and identify:
1. Links or sections related to "premises licence", "licensing", "alcohol licence", "entertainment licence"
2. Business or commercial licensing sections
3. Planning and licensing departments

Look for terms like:
- Premises licence
- Alcohol licensing  
- Entertainment licence
- Licensing register
- Licence applications
- Business licensing

Return ONLY the most likely URL for the premises licence register or licensing section.
If you cannot find a specific URL, return "NOT_FOUND".
Make sure the URL is complete and valid.

URL:"""

            response = await asyncio.to_thread(
                self.llm.invoke,
                [HumanMessage(content=prompt)]
            )
            
            result = response.content.strip()
            
            if result and result != "NOT_FOUND" and "http" in result:
                # Clean up the URL - extract just the URL part
                import re
                # Find URLs in the response
                url_pattern = r'https?://[^\s\]\)\n]+'
                urls = re.findall(url_pattern, result)
                
                if urls:
                    # Take the first valid URL found
                    url = urls[0].rstrip('.,;:!?')  # Remove trailing punctuation
                    
                    # Additional cleaning
                    if url.startswith('http') and not any(invalid in url for invalid in ['example.com', 'placeholder']):
                        return url
                        
                # Fallback: try to extract from text patterns
                lines = result.split('\n')
                for line in lines:
                    line = line.strip()
                    if line.startswith('http') and council.name.split()[0].lower() in line.lower():
                        return line.split()[0]
                        
                return None
                
        except Exception as e:
            logger.error(f"AI analysis failed for {council.name}: {e}")
        
        return None
    
    def save_councils_data(self, filename: Optional[str] = None):
        """Save discovered councils data to JSON file"""
        if filename is None:
            filename = f"{self.settings.data_dir}/councils/discovered_councils.json"
            
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        
        councils_data = [council.dict() for council in self.councils]
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(councils_data, f, indent=2, default=str, ensure_ascii=False)
            
        logger.info(f"Saved {len(self.councils)} councils to {filename}")
    
    def load_councils_data(self, filename: Optional[str] = None) -> List[Council]:
        """Load previously discovered councils data"""
        if filename is None:
            filename = f"{self.settings.data_dir}/councils/discovered_councils.json"
            
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                councils_data = json.load(f)
                
            self.councils = [Council(**council_data) for council_data in councils_data]
            logger.info(f"Loaded {len(self.councils)} councils from {filename}")
            
        except FileNotFoundError:
            logger.warning(f"No saved councils data found at {filename}")
            
        return self.councils


# CLI function for standalone usage
async def main():
    """Main function for running council discovery standalone"""
    import logging
    logging.basicConfig(level=logging.INFO)
    
    discovery = CouncilDiscovery()
    
    # Discover councils
    councils = discovery.discover_councils()
    print(f"Discovered {len(councils)} councils")
    
    # Discover licence registers
    councils_with_registers = await discovery.discover_licence_registers()
    
    successful = len([c for c in councils_with_registers if c.scrape_successful])
    print(f"Found licence registers for {successful}/{len(councils_with_registers)} councils")
    
    # Save results
    discovery.save_councils_data()
    print("Council data saved successfully")


if __name__ == "__main__":
    asyncio.run(main())
