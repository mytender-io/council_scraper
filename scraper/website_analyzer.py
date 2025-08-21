"""
Website Analyzer Module

Uses AI to analyze council websites and understand their structure for effective premises licence scraping.
This module identifies navigation patterns, data formats, and extraction strategies.
"""

import json
import asyncio
import aiohttp
from typing import List, Dict, Optional, Tuple, Any
from datetime import datetime
import logging
from pathlib import Path
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from langchain_openai import ChatOpenAI
from langchain.schema import HumanMessage

from .models import Council, WebsiteAnalysis
from .config import get_settings

logger = logging.getLogger(__name__)


class WebsiteAnalyzer:
    """Analyzes council websites to understand their structure and navigation"""
    
    def __init__(self):
        self.settings = get_settings()
        self.llm = ChatOpenAI(
            model_name=self.settings.openai_model,
            api_key=self.settings.openai_api_key,
            temperature=0
        )
        
    async def analyze_council_websites(self, councils: List[Council]) -> List[WebsiteAnalysis]:
        """Analyze multiple council websites"""
        logger.info(f"Analyzing {len(councils)} council websites")
        
        analyses = []
        batch_size = 5  # Process in smaller batches to manage resources
        
        for i in range(0, len(councils), batch_size):
            batch = councils[i:i+batch_size]
            batch_analyses = await self._analyze_batch(batch)
            analyses.extend(batch_analyses)
            
            # Save progress after each batch
            self._save_analyses(analyses)
            await asyncio.sleep(2)  # Rate limiting
            
        logger.info(f"Completed analysis of {len(analyses)} websites")
        return analyses
    
    async def _analyze_batch(self, councils: List[Council]) -> List[WebsiteAnalysis]:
        """Analyze a batch of council websites"""
        analyses = []
        
        # Process councils one by one to avoid session issues
        for council in councils:
            if council.licence_register_url:
                try:
                    async with aiohttp.ClientSession(
                        timeout=aiohttp.ClientTimeout(total=self.settings.request_timeout)
                    ) as session:
                        analysis = await self._analyze_website(session, council)
                        analyses.append(analysis)
                except Exception as e:
                    logger.error(f"Error analyzing {council.name}: {e}")
                    # Create a failed analysis
                    analysis = WebsiteAnalysis(
                        council_name=council.name,
                        url=council.licence_register_url,
                        licence_register_found=False,
                        analysis_notes=f"Analysis failed: {str(e)}"
                    )
                    analyses.append(analysis)
                    
                # Small delay between requests
                await asyncio.sleep(0.5)
                    
        return analyses
        
        # Filter out exceptions and return valid analyses
        analyses = []
        for result in results:
            if isinstance(result, WebsiteAnalysis):
                analyses.append(result)
            elif isinstance(result, Exception):
                logger.error(f"Analysis failed: {result}")
                
        return analyses
    
    async def _analyze_website(self, session: aiohttp.ClientSession, council: Council) -> WebsiteAnalysis:
        """Analyze a single council website"""
        logger.info(f"Analyzing website for {council.name}")
        
        analysis = WebsiteAnalysis(
            council_name=council.name,
            url=council.licence_register_url or council.website_url
        )
        
        try:
            # Step 1: Basic HTTP analysis
            await self._basic_http_analysis(session, analysis)
            
            # Step 2: HTML structure analysis
            if analysis.licence_register_found:
                await self._html_structure_analysis(session, analysis)
                
            # Step 3: AI-powered content analysis
            await self._ai_content_analysis(analysis)
            
            # Step 4: JavaScript detection (if needed)
            if analysis.javascript_required:
                await self._selenium_analysis(analysis)
                
        except Exception as e:
            logger.error(f"Error analyzing {council.name}: {e}")
            analysis.analysis_notes = f"Analysis failed: {str(e)}"
            
        return analysis
    
    async def _basic_http_analysis(self, session: aiohttp.ClientSession, analysis: WebsiteAnalysis):
        """Basic HTTP response analysis"""
        try:
            async with session.get(str(analysis.url)) as response:
                analysis.licence_register_found = response.status == 200
                
                if response.status == 200:
                    html_content = await response.text()
                    analysis.raw_html = html_content  # Store for later analysis
                    
                    # Check for licence-related content
                    content_lower = html_content.lower()
                    licence_indicators = [
                        'premises licence', 'alcohol licence', 'entertainment licence',
                        'licensing register', 'licence application', 'licensable activities'
                    ]
                    
                    if any(indicator in content_lower for indicator in licence_indicators):
                        analysis.licence_register_found = True
                        
        except Exception as e:
            logger.error(f"HTTP analysis failed for {analysis.council_name}: {e}")
            analysis.licence_register_found = False
            
    async def _html_structure_analysis(self, session: aiohttp.ClientSession, analysis: WebsiteAnalysis):
        """Analyze HTML structure for navigation and data patterns"""
        try:
            html_content = getattr(analysis, 'raw_html', '')
            if not html_content:
                async with session.get(str(analysis.url)) as response:
                    html_content = await response.text()
                    
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Detect website type
            analysis.website_type = self._detect_website_type(soup)
            
            # Find potential licence URLs
            analysis.potential_licence_urls = self._find_licence_urls(soup, str(analysis.url))
            
            # Analyze navigation structure
            analysis.navigation_structure = self._analyze_navigation(soup)
            
            # Check for search functionality
            analysis.search_functionality = self._detect_search_functionality(soup)
            
            # Check for pagination
            analysis.pagination_detected = self._detect_pagination(soup)
            
            # Check for JavaScript requirements
            analysis.javascript_required = self._detect_javascript_requirement(soup)
            
        except Exception as e:
            logger.error(f"HTML analysis failed for {analysis.council_name}: {e}")
            
    def _detect_website_type(self, soup: BeautifulSoup) -> str:
        """Detect the type of website (gov.uk, custom, third-party)"""
        # Check for common CMS indicators
        if soup.find(class_=lambda x: x and 'gov-' in x):
            return "gov.uk"
        elif soup.find(class_=lambda x: x and 'wordpress' in str(x).lower()):
            return "wordpress"  
        elif soup.find(class_=lambda x: x and 'drupal' in str(x).lower()):
            return "drupal"
        elif soup.find(class_=lambda x: x and any(cms in str(x).lower() for cms in ['joomla', 'typo3', 'concrete5'])):
            return "other_cms"
        else:
            return "custom"
            
    def _find_licence_urls(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Find URLs that likely contain licence information"""
        urls = []
        
        # Look for links with licence-related text or URLs
        links = soup.find_all('a', href=True)
        
        for link in links:
            href = link.get('href', '')
            text = link.get_text(strip=True).lower()
            
            licence_indicators = [
                'licence', 'licensing', 'alcohol', 'entertainment',
                'premises', 'register', 'application'
            ]
            
            if any(indicator in text for indicator in licence_indicators) or \
               any(indicator in href.lower() for indicator in licence_indicators):
                full_url = urljoin(base_url, href)
                if full_url not in urls:
                    urls.append(full_url)
                    
        return urls[:10]  # Limit to top 10 URLs
    
    def _analyze_navigation(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Analyze navigation structure"""
        nav_structure = {
            'main_menu_items': [],
            'breadcrumbs': [],
            'sidebar_links': [],
            'footer_links': []
        }
        
        # Main navigation
        nav_elements = soup.find_all(['nav', 'ul', 'div'], class_=lambda x: x and any(
            nav_class in str(x).lower() for nav_class in ['nav', 'menu', 'header']
        ))
        
        for nav in nav_elements[:3]:  # Limit to first 3 nav elements
            links = nav.find_all('a', href=True)
            nav_structure['main_menu_items'].extend([
                {'text': link.get_text(strip=True), 'url': link.get('href')}
                for link in links[:10]  # Limit links per nav
            ])
            
        # Breadcrumbs
        breadcrumb_elements = soup.find_all(class_=lambda x: x and 'breadcrumb' in str(x).lower())
        for breadcrumb in breadcrumb_elements:
            links = breadcrumb.find_all('a', href=True)
            nav_structure['breadcrumbs'].extend([
                {'text': link.get_text(strip=True), 'url': link.get('href')}
                for link in links
            ])
            
        return nav_structure
    
    def _detect_search_functionality(self, soup: BeautifulSoup) -> bool:
        """Check if the website has search functionality"""
        # Look for search forms, inputs, or buttons
        search_elements = soup.find_all(['form', 'input', 'button'], 
                                       class_=lambda x: x and 'search' in str(x).lower())
        
        if search_elements:
            return True
            
        # Look for input fields with search-related attributes
        search_inputs = soup.find_all('input', {'type': 'search'}) or \
                       soup.find_all('input', {'placeholder': lambda x: x and 'search' in x.lower()})
                       
        return len(search_inputs) > 0
    
    def _detect_pagination(self, soup: BeautifulSoup) -> bool:
        """Check if the page has pagination"""
        pagination_indicators = [
            'pagination', 'page-numbers', 'next', 'previous', 'pager'
        ]
        
        for indicator in pagination_indicators:
            elements = soup.find_all(class_=lambda x: x and indicator in str(x).lower())
            if elements:
                return True
                
        # Look for common pagination text
        page_text = soup.get_text().lower()
        if any(text in page_text for text in ['page 1 of', 'next page', 'previous page']):
            return True
            
        return False
    
    def _detect_javascript_requirement(self, soup: BeautifulSoup) -> bool:
        """Check if JavaScript is required for content loading"""
        # Look for common SPA frameworks
        scripts = soup.find_all('script')
        
        for script in scripts:
            script_content = script.get_text().lower()
            if any(framework in script_content for framework in [
                'angular', 'react', 'vue', 'ember', 'backbone'
            ]):
                return True
                
        # Look for dynamic loading indicators
        if soup.find_all(class_=lambda x: x and any(
            indicator in str(x).lower() for indicator in ['loading', 'spinner', 'dynamic']
        )):
            return True
            
        return False
    
    async def _ai_content_analysis(self, analysis: WebsiteAnalysis):
        """Use AI to analyze website content and structure"""
        try:
            html_content = getattr(analysis, 'raw_html', '')
            if not html_content:
                return
                
            # Truncate content for AI analysis
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract key sections
            main_content = self._extract_main_content(soup)
            
            prompt = f"""
You are analyzing a UK council website for premises licensing information.

Council: {analysis.council_name}
URL: {analysis.url}
Website Type: {analysis.website_type}

Content (truncated):
{main_content[:4000]}

Please analyze this content and provide:

1. EXTRACTION_STRATEGY: How should we extract licence data from this site?
   - Direct HTML parsing
   - Form submission required  
   - Search functionality needed
   - JavaScript rendering required
   - API endpoints available

2. DATA_STRUCTURE: What format is the licence data in?
   - HTML tables
   - List items
   - Card/tile layouts
   - PDF documents
   - Database search results

3. KEY_SELECTORS: What CSS selectors or XPath expressions would help find:
   - Licence holder names
   - Premises addresses  
   - Licence types
   - Application dates
   - Status information

4. NAVIGATION_REQUIRED: What steps are needed to access licence data?
   - Direct page access
   - Menu navigation
   - Search form submission
   - Pagination handling

5. CHALLENGES: What difficulties might arise?
   - JavaScript requirements
   - CAPTCHA protection
   - Rate limiting
   - Session management

Provide a JSON response with these keys: extraction_strategy, data_structure, key_selectors, navigation_required, challenges.
"""

            response = await asyncio.to_thread(
                self.llm.invoke,
                [HumanMessage(content=prompt)]
            )
            
            # Parse AI response
            ai_analysis = self._parse_ai_response(response.content)
            if ai_analysis:
                analysis.analysis_notes = json.dumps(ai_analysis, indent=2)
                
        except Exception as e:
            logger.error(f"AI content analysis failed for {analysis.council_name}: {e}")
            analysis.analysis_notes = f"AI analysis failed: {str(e)}"
    
    def _extract_main_content(self, soup: BeautifulSoup) -> str:
        """Extract main content from HTML for AI analysis"""
        # Remove script, style, and other non-content elements
        for element in soup(['script', 'style', 'nav', 'header', 'footer']):
            element.decompose()
            
        # Try to find main content area
        main_selectors = [
            'main', '.main', '#main', '.content', '#content',
            '.main-content', '#main-content', 'article', '.article'
        ]
        
        for selector in main_selectors:
            main_element = soup.select_one(selector)
            if main_element:
                return main_element.get_text(separator=' ', strip=True)
                
        # Fallback to body content
        body = soup.find('body')
        if body:
            return body.get_text(separator=' ', strip=True)
            
        return soup.get_text(separator=' ', strip=True)
    
    def _parse_ai_response(self, response_content: str) -> Optional[Dict]:
        """Parse AI response into structured data"""
        try:
            # Try to extract JSON from response
            start = response_content.find('{')
            end = response_content.rfind('}') + 1
            
            if start >= 0 and end > start:
                json_str = response_content[start:end]
                return json.loads(json_str)
                
        except Exception as e:
            logger.error(f"Failed to parse AI response: {e}")
            
        return None
    
    async def _selenium_analysis(self, analysis: WebsiteAnalysis):
        """Use Selenium for JavaScript-heavy sites"""
        driver = None
        try:
            # Setup Chrome options
            chrome_options = Options()
            if self.settings.headless_browser:
                chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            
            driver = webdriver.Chrome(options=chrome_options)
            driver.set_page_load_timeout(self.settings.browser_timeout)
            
            # Load the page
            driver.get(str(analysis.url))
            
            # Wait for content to load
            wait = WebDriverWait(driver, 10)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            
            # Take screenshot for analysis (optional)
            # driver.save_screenshot(f"{self.settings.data_dir}/screenshots/{analysis.council_name}.png")
            
            # Get rendered HTML
            rendered_html = driver.page_source
            
            # Re-analyze with rendered content
            soup = BeautifulSoup(rendered_html, 'html.parser')
            analysis.potential_licence_urls = self._find_licence_urls(soup, str(analysis.url))
            
            logger.info(f"Selenium analysis completed for {analysis.council_name}")
            
        except Exception as e:
            logger.error(f"Selenium analysis failed for {analysis.council_name}: {e}")
            analysis.analysis_notes = f"Selenium analysis failed: {str(e)}"
            
        finally:
            if driver:
                driver.quit()
    
    def _save_analyses(self, analyses: List[WebsiteAnalysis], filename: Optional[str] = None):
        """Save website analyses to JSON file"""
        if filename is None:
            filename = f"{self.settings.data_dir}/councils/website_analyses.json"
            
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        
        analyses_data = [analysis.dict() for analysis in analyses]
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(analyses_data, f, indent=2, default=str, ensure_ascii=False)
            
        logger.info(f"Saved {len(analyses)} website analyses to {filename}")
    
    def load_analyses(self, filename: Optional[str] = None) -> List[WebsiteAnalysis]:
        """Load previously saved website analyses"""
        if filename is None:
            filename = f"{self.settings.data_dir}/councils/website_analyses.json"
            
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                analyses_data = json.load(f)
                
            analyses = [WebsiteAnalysis(**analysis_data) for analysis_data in analyses_data]
            logger.info(f"Loaded {len(analyses)} website analyses from {filename}")
            return analyses
            
        except FileNotFoundError:
            logger.warning(f"No saved analyses found at {filename}")
            return []


# CLI function for standalone usage
async def main():
    """Main function for running website analysis standalone"""
    import logging
    logging.basicConfig(level=logging.INFO)
    
    from .council_discovery import CouncilDiscovery
    
    # Load councils
    discovery = CouncilDiscovery()
    councils = discovery.load_councils_data()
    
    if not councils:
        print("No councils found. Run council discovery first.")
        return
        
    # Filter councils with licence register URLs
    councils_with_registers = [c for c in councils if c.licence_register_url]
    print(f"Analyzing {len(councils_with_registers)} councils with licence registers")
    
    # Analyze websites
    analyzer = WebsiteAnalyzer()
    analyses = await analyzer.analyze_council_websites(councils_with_registers)
    
    print(f"Completed analysis of {len(analyses)} websites")
    
    # Show summary
    successful = len([a for a in analyses if a.licence_register_found])
    js_required = len([a for a in analyses if a.javascript_required])
    
    print(f"Licence registers found: {successful}/{len(analyses)}")
    print(f"JavaScript required: {js_required}/{len(analyses)}")


if __name__ == "__main__":
    asyncio.run(main())
