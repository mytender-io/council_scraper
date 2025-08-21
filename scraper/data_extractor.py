"""
Data Extraction Module

Uses AI-powered extraction to scrape premises licence data from UK council websites.
Handles different website types and formats with intelligent parsing strategies.
"""

import json
import asyncio
import aiohttp
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime, timedelta
import logging
from pathlib import Path
import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from tenacity import retry, stop_after_attempt, wait_exponential

from langchain_openai import ChatOpenAI
from langchain.schema import HumanMessage

from .models import Council, WebsiteAnalysis, PremisesLicence, LicenceType, LicenceStatus, ScrapingResult
from .config import get_settings

logger = logging.getLogger(__name__)


class DataExtractor:
    """Extracts premises licence data from council websites using AI assistance"""
    
    def __init__(self):
        self.settings = get_settings()
        self.llm = ChatOpenAI(
            model_name=self.settings.openai_model,
            api_key=self.settings.openai_api_key,
            temperature=0
        )
        
    async def extract_all_licences(self, councils: List[Council], analyses: List[WebsiteAnalysis]) -> List[ScrapingResult]:
        """Extract premises licences from all councils"""
        logger.info(f"Starting licence extraction for {len(councils)} councils")
        
        results = []
        
        # Create analysis lookup
        analysis_map = {analysis.council_name: analysis for analysis in analyses}
        
        # Process councils in batches
        batch_size = 3  # Smaller batches for intensive scraping
        
        for i in range(0, len(councils), batch_size):
            batch = councils[i:i+batch_size]
            batch_results = await self._extract_batch(batch, analysis_map)
            results.extend(batch_results)
            
            # Save progress after each batch
            self._save_extraction_results(results)
            
            # Rate limiting between batches
            await asyncio.sleep(5)
            
        logger.info(f"Extraction completed. Processed {len(results)} councils")
        return results
    
    async def _extract_batch(self, councils: List[Council], analysis_map: Dict[str, WebsiteAnalysis]) -> List[ScrapingResult]:
        """Extract licences from a batch of councils"""
        tasks = []
        
        for council in councils:
            if council.licence_register_url:
                analysis = analysis_map.get(council.name)
                task = self._extract_council_licences(council, analysis)
                tasks.append(task)
                
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions and return valid results
        valid_results = []
        for result in results:
            if isinstance(result, ScrapingResult):
                valid_results.append(result)
            elif isinstance(result, Exception):
                logger.error(f"Extraction failed: {result}")
                
        return valid_results
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def _extract_council_licences(self, council: Council, analysis: Optional[WebsiteAnalysis]) -> ScrapingResult:
        """Extract premises licences from a single council"""
        start_time = datetime.now()
        
        result = ScrapingResult(
            council_name=council.name,
            success=False
        )
        
        try:
            logger.info(f"Extracting licences from {council.name}")
            
            # Determine extraction strategy based on analysis
            if analysis and analysis.javascript_required:
                licences = await self._extract_with_selenium(council, analysis)
            else:
                licences = await self._extract_with_http(council, analysis)
                
            result.licences_extracted = licences
            result.licences_found = len(licences)
            result.success = len(licences) > 0
            
            if result.success:
                logger.info(f"Successfully extracted {len(licences)} licences from {council.name}")
            else:
                logger.warning(f"No licences found for {council.name}")
                result.error_message = "No licence data found"
                
        except Exception as e:
            logger.error(f"Extraction failed for {council.name}: {e}")
            result.error_message = str(e)
            
        finally:
            result.scraping_duration = (datetime.now() - start_time).total_seconds()
            
        return result
    
    async def _extract_with_http(self, council: Council, analysis: Optional[WebsiteAnalysis]) -> List[PremisesLicence]:
        """Extract licences using HTTP requests and BeautifulSoup"""
        licences = []
        
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.settings.request_timeout)
        ) as session:
            
            # Get the main licence page
            async with session.get(str(council.licence_register_url)) as response:
                if response.status != 200:
                    raise Exception(f"HTTP {response.status} from {council.licence_register_url}")
                    
                html_content = await response.text()
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # Try different extraction approaches
                
                # Approach 1: Look for licence tables
                table_licences = await self._extract_from_tables(soup, council, session)
                licences.extend(table_licences)
                
                # Approach 2: Look for licence lists
                if not table_licences:
                    list_licences = await self._extract_from_lists(soup, council, session)
                    licences.extend(list_licences)
                
                # Approach 3: AI-powered extraction
                if not licences:
                    ai_licences = await self._ai_extract_licences(html_content, council)
                    licences.extend(ai_licences)
                
                # Approach 4: Search for additional pages
                if analysis and analysis.potential_licence_urls:
                    for url in analysis.potential_licence_urls[:5]:  # Limit to 5 additional URLs
                        additional_licences = await self._extract_from_url(session, url, council)
                        licences.extend(additional_licences)
                        
                        # Rate limiting
                        await asyncio.sleep(1)
        
        return self._deduplicate_licences(licences)
    
    async def _extract_with_selenium(self, council: Council, analysis: Optional[WebsiteAnalysis]) -> List[PremisesLicence]:
        """Extract licences using Selenium for JavaScript-heavy sites"""
        licences = []
        driver = None
        
        try:
            # Setup Chrome options
            chrome_options = Options()
            if self.settings.headless_browser:
                chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            
            driver = webdriver.Chrome(options=chrome_options)
            driver.set_page_load_timeout(self.settings.browser_timeout)
            
            # Load the page
            driver.get(str(council.licence_register_url))
            
            # Wait for content to load
            wait = WebDriverWait(driver, 15)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            
            # Try to handle search forms if needed
            await self._handle_search_forms(driver, council)
            
            # Handle pagination if detected
            if analysis and analysis.pagination_detected:
                licences = await self._extract_paginated_data(driver, council)
            else:
                # Extract from current page
                page_source = driver.page_source
                soup = BeautifulSoup(page_source, 'html.parser')
                
                # Use same extraction methods as HTTP
                table_licences = await self._extract_from_tables(soup, council, None)
                licences.extend(table_licences)
                
                if not licences:
                    list_licences = await self._extract_from_lists(soup, council, None)
                    licences.extend(list_licences)
                    
                if not licences:
                    ai_licences = await self._ai_extract_licences(page_source, council)
                    licences.extend(ai_licences)
            
        except Exception as e:
            logger.error(f"Selenium extraction failed for {council.name}: {e}")
            raise
            
        finally:
            if driver:
                driver.quit()
                
        return self._deduplicate_licences(licences)
    
    async def _extract_from_tables(self, soup: BeautifulSoup, council: Council, session: Optional[aiohttp.ClientSession]) -> List[PremisesLicence]:
        """Extract licence data from HTML tables"""
        licences = []
        
        # Find tables that might contain licence data
        tables = soup.find_all('table')
        
        for table in tables:
            # Check if table contains licence-related headers
            headers = []
            header_row = table.find('tr')
            if header_row:
                headers = [th.get_text(strip=True).lower() for th in header_row.find_all(['th', 'td'])]
                
            licence_indicators = [
                'premises', 'licence', 'alcohol', 'entertainment', 'name', 'address',
                'status', 'date', 'application', 'holder'
            ]
            
            if any(indicator in ' '.join(headers) for indicator in licence_indicators):
                # This table likely contains licence data
                rows = table.find_all('tr')[1:]  # Skip header row
                
                for row in rows[:50]:  # Limit to first 50 rows per table
                    cells = [td.get_text(strip=True) for td in row.find_all(['td', 'th'])]
                    
                    if len(cells) >= 3:  # Must have at least 3 columns for meaningful data
                        licence = await self._parse_table_row(cells, headers, council)
                        if licence:
                            licences.append(licence)
                            
        return licences
    
    async def _extract_from_lists(self, soup: BeautifulSoup, council: Council, session: Optional[aiohttp.ClientSession]) -> List[PremisesLicence]:
        """Extract licence data from HTML lists or card layouts"""
        licences = []
        
        # Look for list items or card-like structures
        containers = soup.find_all(['ul', 'ol', 'div'], class_=lambda x: x and any(
            indicator in str(x).lower() for indicator in [
                'licence', 'premises', 'result', 'item', 'card', 'entry'
            ]
        ))
        
        for container in containers[:10]:  # Limit to first 10 containers
            items = container.find_all(['li', 'div'], class_=lambda x: x and any(
                indicator in str(x).lower() for indicator in [
                    'licence', 'premises', 'result', 'item', 'card', 'entry'
                ]
            ))
            
            for item in items[:20]:  # Limit to first 20 items per container
                text_content = item.get_text(separator=' ', strip=True)
                
                if len(text_content) > 50:  # Must have substantial content
                    licence = await self._parse_text_content(text_content, council)
                    if licence:
                        licences.append(licence)
                        
        return licences
    
    async def _parse_table_row(self, cells: List[str], headers: List[str], council: Council) -> Optional[PremisesLicence]:
        """Parse a table row into a premises licence"""
        try:
            # Create a mapping of data based on headers
            data = {}
            for i, header in enumerate(headers):
                if i < len(cells):
                    data[header] = cells[i]
                    
            # Extract key information using AI
            combined_text = ' | '.join(cells)
            return await self._ai_parse_licence_data(combined_text, council, 'table_row')
            
        except Exception as e:
            logger.error(f"Error parsing table row: {e}")
            return None
    
    async def _parse_text_content(self, text_content: str, council: Council) -> Optional[PremisesLicence]:
        """Parse text content into a premises licence"""
        try:
            return await self._ai_parse_licence_data(text_content, council, 'text_content')
        except Exception as e:
            logger.error(f"Error parsing text content: {e}")
            return None
    
    async def _ai_extract_licences(self, html_content: str, council: Council) -> List[PremisesLicence]:
        """Use AI to extract licence data from raw HTML"""
        try:
            # Clean and truncate HTML for AI processing
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove script, style, and other non-content elements
            for element in soup(['script', 'style', 'nav', 'header', 'footer']):
                element.decompose()
                
            clean_text = soup.get_text(separator=' ', strip=True)
            
            # Truncate for AI processing
            if len(clean_text) > 8000:
                clean_text = clean_text[:8000] + "..."
                
            licences = await self._ai_bulk_extract_licences(clean_text, council)
            return licences
            
        except Exception as e:
            logger.error(f"AI extraction failed for {council.name}: {e}")
            return []
    
    async def _ai_parse_licence_data(self, text_data: str, council: Council, data_type: str) -> Optional[PremisesLicence]:
        """Use AI to parse individual licence data"""
        try:
            prompt = f"""
You are extracting UK premises licence data from council website content.

Council: {council.name}
Content: {text_data}

Extract premises licence information and return ONLY a valid JSON object.

Required format:
{{
  "premises_name": "string or null",
  "premises_address": "string or null", 
  "postcode": "string or null",
  "licence_holder": "string or null",
  "licence_type": "premises licence",
  "licence_status": "granted",
  "application_date": "YYYY-MM-DD or null",
  "granted_date": "YYYY-MM-DD or null",
  "licensable_activities": ["Sale of Alcohol"],
  "licensed_hours": "string or null",
  "conditions": []
}}

Rules:
- If no clear licence data is found, return: {{"premises_name": null, "premises_address": null}}
- Use null for missing data, not empty strings
- licensable_activities must be an array
- Only include real premises licence data, not navigation/website text
- Return only the JSON object, no explanatory text

JSON:"""

            response = await asyncio.to_thread(
                self.llm.invoke,
                [HumanMessage(content=prompt)]
            )
            
            # Parse AI response
            licence_data = self._parse_ai_licence_response(response.content, council)
            return licence_data
            
        except Exception as e:
            logger.error(f"AI licence parsing failed: {e}")
            return None
    
    async def _ai_bulk_extract_licences(self, text_content: str, council: Council) -> List[PremisesLicence]:
        """Use AI to extract multiple licences from bulk text content"""
        try:
            prompt = f"""
Extract all premises licence information from this content from {council.name}:

Content:
{text_content}

Find all premises licences mentioned and extract key information for each:
- Premises name
- Address
- Licence holder
- Licence type
- Status
- Dates
- Activities

Return a JSON array of licence objects. Each object should have keys:
premises_name, premises_address, licence_holder, licence_type, licence_status, 
application_date, granted_date, licensable_activities, licensed_hours, conditions.

Only extract real licence data, not navigation elements or general information.
Limit to maximum 20 licences.
"""

            response = await asyncio.to_thread(
                self.llm.invoke,
                [HumanMessage(content=prompt)]
            )
            
            # Parse AI response into multiple licences
            licences = self._parse_ai_bulk_response(response.content, council)
            return licences
            
        except Exception as e:
            logger.error(f"AI bulk extraction failed: {e}")
            return []
    
    def _parse_ai_licence_response(self, response_content: str, council: Council) -> Optional[PremisesLicence]:
        """Parse AI response into a PremisesLicence object"""
        try:
            # Extract JSON from response
            start = response_content.find('{')
            end = response_content.rfind('}') + 1
            
            if start >= 0 and end > start:
                json_str = response_content[start:end]
                data = json.loads(json_str)
                
                # Create licence object
                licence = PremisesLicence(
                    licence_id=f"{council.name}_{datetime.now().timestamp()}",
                    council_name=council.name,
                    council_code=council.code,
                    premises_name=data.get('premises_name', ''),
                    premises_address=data.get('premises_address', ''),
                    postcode=data.get('postcode'),
                    licence_type=self._map_licence_type(data.get('licence_type')),
                    licence_status=self._map_licence_status(data.get('licence_status')),
                    application_date=self._parse_date(data.get('application_date')),
                    granted_date=self._parse_date(data.get('granted_date')),
                    licensable_activities=data.get('licensable_activities', []),
                    designated_premises_supervisor=data.get('licence_holder'),
                    conditions=data.get('conditions', []),
                    source_url=council.licence_register_url,
                    raw_data=data
                )
                
                # Validate essential fields
                if licence.premises_name and licence.premises_address:
                    return licence
                    
        except Exception as e:
            logger.error(f"Error parsing AI response: {e}")
            
        return None
    
    def _parse_ai_bulk_response(self, response_content: str, council: Council) -> List[PremisesLicence]:
        """Parse AI response into multiple PremisesLicence objects"""
        licences = []
        
        try:
            # Extract JSON array from response
            start = response_content.find('[')
            end = response_content.rfind(']') + 1
            
            if start >= 0 and end > start:
                json_str = response_content[start:end]
                data_list = json.loads(json_str)
                
                for i, data in enumerate(data_list[:20]):  # Limit to 20 licences
                    licence = PremisesLicence(
                        licence_id=f"{council.name}_{datetime.now().timestamp()}_{i}",
                        council_name=council.name,
                        council_code=council.code,
                        premises_name=data.get('premises_name', ''),
                        premises_address=data.get('premises_address', ''),
                        postcode=data.get('postcode'),
                        licence_type=self._map_licence_type(data.get('licence_type')),
                        licence_status=self._map_licence_status(data.get('licence_status')),
                        application_date=self._parse_date(data.get('application_date')),
                        granted_date=self._parse_date(data.get('granted_date')),
                        licensable_activities=data.get('licensable_activities', []),
                        designated_premises_supervisor=data.get('licence_holder'),
                        conditions=data.get('conditions', []),
                        source_url=council.licence_register_url,
                        raw_data=data
                    )
                    
                    # Validate essential fields
                    if licence.premises_name and licence.premises_address:
                        licences.append(licence)
                        
        except Exception as e:
            logger.error(f"Error parsing AI bulk response: {e}")
            
        return licences
    
    def _map_licence_type(self, type_str: Optional[str]) -> LicenceType:
        """Map string to LicenceType enum"""
        if not type_str:
            return LicenceType.NEW
            
        type_lower = type_str.lower()
        
        if 'variation' in type_lower:
            return LicenceType.VARIATION
        elif 'transfer' in type_lower:
            return LicenceType.TRANSFER
        elif 'review' in type_lower:
            return LicenceType.REVIEW
        elif 'provisional' in type_lower:
            return LicenceType.PROVISIONAL
        else:
            return LicenceType.NEW
    
    def _map_licence_status(self, status_str: Optional[str]) -> LicenceStatus:
        """Map string to LicenceStatus enum"""
        if not status_str:
            return LicenceStatus.PENDING
            
        status_lower = status_str.lower()
        
        if 'granted' in status_lower or 'approved' in status_lower:
            return LicenceStatus.GRANTED
        elif 'refused' in status_lower or 'rejected' in status_lower:
            return LicenceStatus.REFUSED
        elif 'withdrawn' in status_lower:
            return LicenceStatus.WITHDRAWN
        elif 'review' in status_lower:
            return LicenceStatus.UNDER_REVIEW
        else:
            return LicenceStatus.PENDING
    
    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse date string into datetime object"""
        if not date_str:
            return None
            
        # Try common date formats
        date_formats = [
            '%Y-%m-%d',
            '%d/%m/%Y',
            '%d-%m-%Y', 
            '%d %B %Y',
            '%d %b %Y',
            '%B %d, %Y',
            '%b %d, %Y'
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                continue
                
        return None
    
    async def _extract_from_url(self, session: aiohttp.ClientSession, url: str, council: Council) -> List[PremisesLicence]:
        """Extract licences from a specific URL"""
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    html_content = await response.text()
                    soup = BeautifulSoup(html_content, 'html.parser')
                    
                    # Use same extraction methods
                    licences = []
                    table_licences = await self._extract_from_tables(soup, council, session)
                    licences.extend(table_licences)
                    
                    if not licences:
                        list_licences = await self._extract_from_lists(soup, council, session)
                        licences.extend(list_licences)
                        
                    return licences
                    
        except Exception as e:
            logger.error(f"Failed to extract from {url}: {e}")
            
        return []
    
    async def _handle_search_forms(self, driver: webdriver.Chrome, council: Council):
        """Handle search forms on licence pages"""
        try:
            # Look for search forms
            search_forms = driver.find_elements(By.TAG_NAME, "form")
            
            for form in search_forms:
                # Check if it's a licence search form
                form_text = form.text.lower()
                if any(term in form_text for term in ['licence', 'search', 'premises']):
                    
                    # Try to find and click search/submit button without entering terms
                    submit_buttons = form.find_elements(By.XPATH, ".//input[@type='submit'] | .//button[@type='submit'] | .//button[contains(@class, 'search')]")
                    
                    if submit_buttons:
                        submit_buttons[0].click()
                        
                        # Wait for results to load
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.TAG_NAME, "body"))
                        )
                        break
                        
        except Exception as e:
            logger.debug(f"Could not handle search forms for {council.name}: {e}")
    
    async def _extract_paginated_data(self, driver: webdriver.Chrome, council: Council) -> List[PremisesLicence]:
        """Extract data from paginated results"""
        all_licences = []
        page_count = 0
        max_pages = 10  # Limit to prevent infinite loops
        
        while page_count < max_pages:
            page_count += 1
            
            # Extract licences from current page
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            page_licences = []
            table_licences = await self._extract_from_tables(soup, council, None)
            page_licences.extend(table_licences)
            
            if not page_licences:
                list_licences = await self._extract_from_lists(soup, council, None)
                page_licences.extend(list_licences)
                
            all_licences.extend(page_licences)
            
            # Try to find and click next page button
            try:
                next_buttons = driver.find_elements(By.XPATH, 
                    "//a[contains(text(), 'Next')] | //a[contains(@class, 'next')] | //input[@value='Next']"
                )
                
                if next_buttons:
                    next_buttons[0].click()
                    
                    # Wait for new page to load
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.TAG_NAME, "body"))
                    )
                    
                    await asyncio.sleep(2)  # Additional wait
                else:
                    break  # No more pages
                    
            except Exception:
                break  # Error navigating to next page
                
        return all_licences
    
    def _deduplicate_licences(self, licences: List[PremisesLicence]) -> List[PremisesLicence]:
        """Remove duplicate licences based on premises name and address"""
        seen = set()
        unique_licences = []
        
        for licence in licences:
            # Create a key for deduplication
            key = f"{licence.premises_name.lower().strip()}|{licence.premises_address.lower().strip()}"
            
            if key not in seen:
                seen.add(key)
                unique_licences.append(licence)
                
        logger.info(f"Deduplicated {len(licences)} -> {len(unique_licences)} licences")
        return unique_licences
    
    def _save_extraction_results(self, results: List[ScrapingResult], filename: Optional[str] = None):
        """Save extraction results to JSON file"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.settings.data_dir}/licences/extraction_results_{timestamp}.json"
            
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        
        results_data = [result.dict() for result in results]
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(results_data, f, indent=2, default=str, ensure_ascii=False)
            
        logger.info(f"Saved {len(results)} extraction results to {filename}")
    
    def load_extraction_results(self, filename: str) -> List[ScrapingResult]:
        """Load previously saved extraction results"""
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                results_data = json.load(f)
                
            results = [ScrapingResult(**result_data) for result_data in results_data]
            logger.info(f"Loaded {len(results)} extraction results from {filename}")
            return results
            
        except FileNotFoundError:
            logger.warning(f"No extraction results found at {filename}")
            return []


# CLI function for standalone usage  
async def main():
    """Main function for running data extraction standalone"""
    import logging
    logging.basicConfig(level=logging.INFO)
    
    from .council_discovery import CouncilDiscovery
    from .website_analyzer import WebsiteAnalyzer
    
    # Load councils and analyses
    discovery = CouncilDiscovery()
    councils = discovery.load_councils_data()
    
    analyzer = WebsiteAnalyzer()
    analyses = analyzer.load_analyses()
    
    if not councils:
        print("No councils found. Run council discovery first.")
        return
        
    # Filter councils with licence register URLs
    councils_with_registers = [c for c in councils if c.licence_register_url and c.scrape_successful]
    print(f"Extracting from {len(councils_with_registers)} councils")
    
    # Extract licence data
    extractor = DataExtractor()
    results = await extractor.extract_all_licences(councils_with_registers[:5], analyses)  # Test with first 5
    
    # Show summary
    successful = len([r for r in results if r.success])
    total_licences = sum(r.licences_found for r in results)
    
    print(f"Extraction completed:")
    print(f"Successful councils: {successful}/{len(results)}")
    print(f"Total licences extracted: {total_licences}")


if __name__ == "__main__":
    asyncio.run(main())
