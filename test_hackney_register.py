"""
Test extraction from Hackney's actual licence register
"""

import asyncio
import aiohttp
from bs4 import BeautifulSoup
from langchain_openai import ChatOpenAI
from langchain.schema import HumanMessage
from scraper.config import get_settings

async def test_hackney_register():
    """Test extraction from Hackney's public licence register"""
    print("🎯 Testing Hackney Public Licence Register")
    
    register_url = "https://map2.hackney.gov.uk/lbh-licensing-register/"
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(register_url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                print(f"Status: {response.status}")
                
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Remove scripts and styles
                    for script in soup(["script", "style"]):
                        script.decompose()
                    
                    text = soup.get_text()
                    lines = [line.strip() for line in text.splitlines() if line.strip()]
                    clean_text = ' '.join(lines)
                    
                    print(f"Content length: {len(clean_text)} chars")
                    print(f"Content preview: {clean_text[:1000]}...")
                    
                    # Check for actual licence data
                    licence_indicators = [
                        'premises name', 'licence holder', 'address', 'granted', 
                        'application number', 'licence number', 'status', 'pub', 'restaurant', 'bar'
                    ]
                    
                    found_indicators = []
                    for indicator in licence_indicators:
                        if indicator in clean_text.lower():
                            found_indicators.append(indicator)
                    
                    print(f"Licence indicators found: {found_indicators}")
                    
                    # Look for structured data
                    tables = soup.find_all('table')
                    print(f"Tables found: {len(tables)}")
                    
                    divs_with_class = soup.find_all('div', class_=True)
                    print(f"Divs with classes: {len(divs_with_class)}")
                    
                    # If there's substantial content, try AI extraction
                    if len(clean_text) > 500 and len(found_indicators) >= 2:
                        print("\n🤖 Trying AI extraction...")
                        
                        settings = get_settings()
                        llm = ChatOpenAI(
                            model_name=settings.openai_model,
                            api_key=settings.openai_api_key,
                            temperature=0
                        )
                        
                        # Truncate content for AI
                        sample_content = clean_text[:4000]
                        
                        prompt = f"""
You are extracting UK premises licence data from Hackney Council's public register.

Content from https://map2.hackney.gov.uk/lbh-licensing-register/:
{sample_content}

This appears to be a real licence database. Extract any premises licence information you can find.

Look for:
- Business/premises names
- Addresses  
- Licence holders
- Licence types
- Status information

Return a JSON array of any licences found, even if partial data:
[
  {{
    "premises_name": "business name",
    "premises_address": "full address", 
    "licence_holder": "person/company name",
    "licence_type": "premises licence",
    "licence_status": "granted",
    "activities": ["Sale of Alcohol"]
  }}
]

If you find actual licence data, return it. If this is just a search interface with no actual data visible, return: []

JSON:"""
                        
                        response = llm.invoke([HumanMessage(content=prompt)])
                        
                        print(f"AI Response: {response.content}")
                        
                        # Try to parse the response
                        try:
                            import json
                            import re
                            
                            # Extract JSON from response
                            json_match = re.search(r'\[(.*?)\]', response.content, re.DOTALL)
                            if json_match:
                                json_str = '[' + json_match.group(1) + ']'
                                licences = json.loads(json_str)
                                
                                print(f"\n🎉 Successfully parsed {len(licences)} licences!")
                                for i, licence in enumerate(licences):
                                    print(f"Licence {i+1}:")
                                    for key, value in licence.items():
                                        print(f"  {key}: {value}")
                                    print()
                            else:
                                print("No JSON array found in response")
                                
                        except Exception as parse_error:
                            print(f"JSON parsing error: {parse_error}")
                    
                    else:
                        print("Not enough content or indicators for AI extraction")
                        
                else:
                    print(f"❌ HTTP Error: {response.status}")
                    
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_hackney_register())
