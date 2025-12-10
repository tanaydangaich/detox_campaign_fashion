#!/usr/bin/env python3
"""
Greenpeace USA Campaign Scraper
Crawls Greenpeace USA website to find campaigns and extract companies targeted
for toxic pollution (air, water, land, nuclear).

NOTE: Greenpeace USA organizes campaigns by ISSUE AREAS, not a general campaigns page.
Main issue areas: toxics, oceans, climate, plastic pollution, industrial pollution.
The scraper targets these issue pages and their sub-pages.

Uses Firecrawl's Extract feature for one-step scraping + extraction.
"""

import os
import json
import time
from typing import List, Dict, Optional
from datetime import datetime


try:
    from firecrawl import FirecrawlApp
except ImportError:
    print("Please install firecrawl: pip install firecrawl-py")
    exit(1)


class GreenpeaceCampaignScraper:
    def __init__(self, firecrawl_api_key: str):
        """Initialize scraper with Firecrawl API key."""
        self.firecrawl = FirecrawlApp(api_key=firecrawl_api_key)
        self.results = []
        
        # Define the schema for extraction - research-focused design
        self.extraction_schema = {
            "type": "object",
            "properties": {
                "has_campaign_targets": {
                    "type": "boolean",
                    "description": "Whether this page describes a campaign targeting specific companies for pollution"
                },
                "campaign_name": {
                    "type": "string",
                    "description": "Name of the campaign or issue, if clearly stated"
                },
                "campaign_priority": {
                    "type": "string",
                    "enum": ["high", "medium", "low"],
                    "description": "Priority level based on prominence on page, detail level, and call-to-action presence. High = featured campaign with detailed info, Medium = mentioned campaign with some details, Low = brief mention"
                },
                "target_companies": {
                    "type": "array",
                    "description": "List of companies being targeted for pollution violations",
                    "items": {
                        "type": "object",
                        "properties": {
                            "company_name": {
                                "type": "string",
                                "description": "Exact name of the company as mentioned"
                            },
                            "industry_sector": {
                                "type": "string",
                                "description": "Industry sector (e.g., oil & gas, coal, petrochemical, manufacturing, fashion, electronics, insurance, finance)"
                            },
                            "pollution_categories": {
                                "type": "array",
                                "description": "Broad categories of pollution",
                                "items": {
                                    "type": "string",
                                    "enum": ["air", "water", "land", "nuclear", "toxic_waste", "climate"]
                                }
                            },
                            "specific_issues": {
                                "type": "array",
                                "description": "Specific environmental issues (e.g., methane leaks, water contamination, plastic pollution, deforestation)",
                                "items": {"type": "string"}
                            },
                            "pollutants": {
                                "type": "array",
                                "description": "Specific chemicals or pollutants mentioned (e.g., methane, benzene, mercury, microplastics)",
                                "items": {"type": "string"}
                            },
                            "project_or_asset": {
                                "type": "string",
                                "description": "Specific project, facility, or asset mentioned (e.g., Permian Basin operations, Deepwater Horizon rig, manufacturing plant in City X)"
                            },
                            "location": {
                                "type": "object",
                                "description": "Geographic location details",
                                "properties": {
                                    "site": {
                                        "type": "string",
                                        "description": "Specific site or facility name"
                                    },
                                    "region": {
                                        "type": "string",
                                        "description": "State, province, or region"
                                    },
                                    "country": {
                                        "type": "string",
                                        "description": "Country"
                                    }
                                }
                            },
                            "accusation_summary": {
                                "type": "string",
                                "description": "Clear summary of what the company is accused of (2-3 sentences max)"
                            },
                            "evidence_excerpt": {
                                "type": "string",
                                "description": "Key quote or evidence excerpt from the page that supports the accusation (verbatim text from page)"
                            },
                            "claim_date": {
                                "type": "string",
                                "description": "Date when the claim/campaign was made, in YYYY-MM-DD format if available, or null"
                            },
                            "company_response_detected": {
                                "type": "boolean",
                                "description": "Whether the page mentions any company response (lawsuit, statement, policy change, denial)"
                            },
                            "response_type": {
                                "type": "string",
                                "enum": ["lawsuit", "slapp_lawsuit", "public_statement", "policy_change", "denial", "no_response", None],
                                "description": "Type of company response if mentioned. SLAPP = Strategic Lawsuit Against Public Participation"
                            },
                            "response_summary": {
                                "type": "string",
                                "description": "Brief summary of company response if mentioned"
                            }
                        },
                        "required": ["company_name", "pollution_categories", "accusation_summary"]
                    }
                }
            },
            "required": ["has_campaign_targets", "target_companies"]
        }
        
    def map_greenpeace_site(self) -> List[str]:
        """
        Get list of Greenpeace USA campaign URLs.
        
        Greenpeace USA organizes campaigns by issue areas, not a general campaigns page.
        We use seed URLs for known issue pages, then optionally map to discover sub-pages.
        """
        print("🗺️  Getting Greenpeace USA campaign URLs...")
        
        # Greenpeace USA organizes by issue areas - these are known starting points
        seed_urls = [
            "https://www.greenpeace.org/usa/toxics/",
            "https://www.greenpeace.org/usa/oceans/",
            "https://www.greenpeace.org/usa/climate/",
            "https://www.greenpeace.org/usa/fighting-plastic-pollution/",
            "https://www.greenpeace.org/usa/issues/",
            "https://www.greenpeace.org/usa/preventing-chemical-disasters/",
            "https://www.greenpeace.org/usa/pvc-free/",
            "https://www.greenpeace.org/usa/green-electronics/",
            "https://www.greenpeace.org/usa/industrial-pollution/",
        ]
        
        print(f"  📋 Using {len(seed_urls)} seed URLs (issue area pages)")
        
        # Optional: Try to discover additional sub-pages using Firecrawl map
        try:
            base_url = "https://www.greenpeace.org/usa"
            
            # Use Firecrawl v2 map method (not map_url)
            map_result = self.firecrawl.map(
                url=base_url,
                search='toxic pollution chemical campaign'
            )
            
            # Map returns a dict with 'links' array
            if map_result and 'links' in map_result:
                mapped_urls = [link['url'] if isinstance(link, dict) else link 
                              for link in map_result['links']]
                print(f"  ✅ Discovered {len(mapped_urls)} additional URLs via mapping")
                
                # Filter for relevant campaign/issue URLs
                campaign_urls = [
                    url for url in mapped_urls 
                    if any(keyword in url.lower() for keyword in [
                        '/toxics/', '/pollution/', '/chemical', '/oil', '/gas',
                        '/coal', '/plastic', '/ocean', '/climate', '/industrial',
                        '/electronics', '/fashion', '/detox', '/pvc',
                        '/preventing-', '/fighting-', 'disaster'
                    ])
                    and not any(exclude in url.lower() for exclude in [
                        'donate', 'give', 'volunteer', 'shop', 'jobs',
                        'about', 'contact', 'login', 'privacy', 'terms',
                        '/tag/', '/author/', '/category/'
                    ])
                ]
                
                # Combine seed URLs with discovered URLs
                all_urls = list(set(seed_urls + campaign_urls))
                print(f"  ✅ Total relevant URLs: {len(all_urls)}")
                return all_urls[:50]  # Limit for testing
                
        except Exception as e:
            print(f"  ⚠️  Mapping failed: {e}")
            print(f"  📋 Using seed URLs only")
        
        return seed_urls
    
    def extract_from_url(self, url: str) -> List[Dict]:
        """
        Use Firecrawl Extract to scrape and extract company data in one call.
        Returns list of company records with full metadata.
        """
        try:
            # Firecrawl extract method signature: extract(urls, schema=None, prompt=None)
            result = self.firecrawl.extract(
                urls=[url],
                schema=self.extraction_schema,
                prompt="""Extract information about companies being targeted by Greenpeace for pollution violations.
                
CRITICAL RULES:
- Only include companies that are explicitly named as targets of criticism or campaigns
- Do NOT include Greenpeace itself, partner organizations, or companies mentioned positively
- Only include companies clearly associated with pollution/environmental harm
- Pollution categories must be from: air, water, land, nuclear, toxic_waste, climate
- For location, extract as much detail as available (site, region/state, country)
- For dates, use YYYY-MM-DD format if you can determine a specific date, otherwise null
- For evidence_excerpt, copy verbatim text from the page (direct quote)
- For accusation_summary, write a clear 2-3 sentence summary in your own words
- Identify company responses like lawsuits (especially SLAPP suits), denials, or policy changes
- Campaign priority: HIGH if prominently featured with detailed info, MEDIUM if mentioned with some context, LOW if brief mention
- Be conservative - if unsure whether a company is a target, do not include it"""
            )
            
            # Extract returns results in data field
            if result and 'data' in result and len(result['data']) > 0:
                extracted_data = result['data'][0]
                
                # Check if page actually has campaign targets
                if not extracted_data.get('has_campaign_targets', False):
                    return []
                
                # Get the target companies
                companies = extracted_data.get('target_companies', [])
                
                # Add metadata to each company record
                campaign_name = extracted_data.get('campaign_name', 'Unknown Campaign')
                campaign_priority = extracted_data.get('campaign_priority', 'medium')
                scrape_timestamp = datetime.now().isoformat()
                
                enriched_companies = []
                for company in companies:
                    # Create full record with research-friendly structure
                    record = {
                        # Source metadata
                        "record_id": self._generate_record_id(company.get('company_name'), url),
                        "source_organization": "Greenpeace",
                        "source_url": url,
                        "scrape_date": scrape_timestamp,
                        
                        # Target company
                        "company_name": company.get('company_name'),
                        "industry_sector": company.get('industry_sector'),
                        
                        # Campaign context
                        "campaign_name": campaign_name,
                        "activist_priority_level": campaign_priority,
                        
                        # Pollution details
                        "pollution_categories": company.get('pollution_categories', []),
                        "specific_issues": company.get('specific_issues', []),
                        "pollutants": company.get('pollutants', []),
                        "project_or_asset": company.get('project_or_asset'),
                        "location": company.get('location', {}),
                        
                        # Claim details
                        "accusation_summary": company.get('accusation_summary'),
                        "evidence_excerpt": company.get('evidence_excerpt'),
                        "claim_date": company.get('claim_date'),
                        
                        # Company response
                        "company_response": {
                            "detected": company.get('company_response_detected', False),
                            "response_type": company.get('response_type'),
                            "response_summary": company.get('response_summary')
                        },
                        
                        # Data quality
                        "extraction_confidence": "high",  # Firecrawl Extract is generally high quality
                        "needs_manual_review": False
                    }
                    
                    enriched_companies.append(record)
                
                return enriched_companies
            
            return []
            
        except Exception as e:
            print(f"❌ Error extracting from {url}: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def _generate_record_id(self, company_name: str, url: str) -> str:
        """
        Generate a unique record ID for deduplication.
        Format: GP_YEAR_COMPANYSHORT_HASH
        """
        import hashlib
        
        # Sanitize company name for ID
        company_short = company_name.replace(' ', '').replace(',', '')[:10].upper() if company_name else 'UNKNOWN'
        
        # Get year from current date
        year = datetime.now().year
        
        # Create short hash of URL to ensure uniqueness
        url_hash = hashlib.md5(url.encode()).hexdigest()[:6]
        
        return f"GP_{year}_{company_short}_{url_hash}"
    
    def run_full_pipeline(self, test_mode: bool = True) -> List[Dict]:
        """
        Run the complete pipeline: map -> extract with Firecrawl.
        
        Args:
            test_mode: If True, only process first 5 URLs for testing
        """
        print("🚀 Starting Greenpeace USA scraper pipeline\n")
        
        # Step 1: Map the site
        campaign_urls = self.map_greenpeace_site()
        
        if test_mode:
            campaign_urls = campaign_urls[:5]
            print(f"\n🧪 TEST MODE: Processing only {len(campaign_urls)} URLs\n")
        
        # Step 2: Extract from each URL using Firecrawl Extract
        all_records = []
        
        for i, url in enumerate(campaign_urls, 1):
            print(f"\n[{i}/{len(campaign_urls)}] Processing: {url}")
            
            # Extract directly - Firecrawl handles scraping + LLM extraction
            records = self.extract_from_url(url)
            
            if records:
                print(f"  ✅ Found {len(records)} target companies")
                for record in records:
                    pollution = ', '.join(record.get('pollution_categories', []))
                    sector = record.get('industry_sector', 'unknown sector')
                    priority = record.get('activist_priority_level', 'medium')
                    print(f"     - {record['company_name']} ({sector}) - {pollution} [{priority} priority]")
                all_records.extend(records)
            else:
                print("  ℹ️  No target companies found on this page")
            
            # Rate limiting - be nice to Firecrawl API
            time.sleep(2)
        
        print(f"\n\n{'='*60}")
        print(f"✅ COMPLETE: Found {len(all_records)} company records total")
        print(f"{'='*60}\n")
        
        # Print summary statistics
        if all_records:
            self._print_summary_stats(all_records)
        
        return all_records
    
    def _print_summary_stats(self, records: List[Dict]):
        """Print summary statistics about extracted records."""
        from collections import Counter
        
        print("\n📊 SUMMARY STATISTICS:")
        print(f"{'='*60}")
        
        # Unique companies
        unique_companies = len(set(r['company_name'] for r in records))
        print(f"Unique companies: {unique_companies}")
        
        # Industry breakdown
        industries = [r.get('industry_sector') for r in records if r.get('industry_sector')]
        if industries:
            print(f"\nTop industries targeted:")
            for industry, count in Counter(industries).most_common(5):
                print(f"  - {industry}: {count}")
        
        # Pollution categories
        all_pollution = []
        for r in records:
            all_pollution.extend(r.get('pollution_categories', []))
        if all_pollution:
            print(f"\nPollution categories:")
            for category, count in Counter(all_pollution).most_common():
                print(f"  - {category}: {count}")
        
        # Priority levels
        priorities = [r.get('activist_priority_level') for r in records if r.get('activist_priority_level')]
        if priorities:
            print(f"\nPriority distribution:")
            for priority, count in Counter(priorities).most_common():
                print(f"  - {priority}: {count}")
        
        # Company responses
        responses = sum(1 for r in records if r.get('company_response', {}).get('detected'))
        print(f"\nCompany responses detected: {responses} ({responses/len(records)*100:.1f}%)")
        
        print(f"{'='*60}\n")
    
    def save_results(self, records: List[Dict], filename: str = "greenpeace_targets.json"):
        """Save results to JSON file with summary statistics."""
        from collections import Counter
        
        output_path = f"/home/claude/{filename}"
        
        # Calculate summary statistics
        unique_companies = len(set(r['company_name'] for r in records))
        
        industries = [r.get('industry_sector') for r in records if r.get('industry_sector')]
        top_industries = dict(Counter(industries).most_common(5))
        
        all_pollution = []
        for r in records:
            all_pollution.extend(r.get('pollution_categories', []))
        pollution_breakdown = dict(Counter(all_pollution))
        
        priorities = [r.get('activist_priority_level') for r in records if r.get('activist_priority_level')]
        priority_distribution = dict(Counter(priorities))
        
        responses_detected = sum(1 for r in records if r.get('company_response', {}).get('detected'))
        
        # Build output structure
        output = {
            'metadata': {
                'scrape_date': datetime.now().isoformat(),
                'source_organization': 'Greenpeace',
                'total_records': len(records),
                'unique_companies': unique_companies,
                'test_mode': len(records) < 10  # Heuristic
            },
            'summary_statistics': {
                'industry_breakdown': top_industries,
                'pollution_categories': pollution_breakdown,
                'priority_distribution': priority_distribution,
                'company_responses_detected': responses_detected,
                'response_rate_percent': round(responses_detected / len(records) * 100, 1) if records else 0
            },
            'records': records
        }
        
        with open(output_path, 'w') as f:
            json.dump(output, f, indent=2)
        
        print(f"💾 Saved {len(records)} records to: {output_path}")
        return output_path


def main():
    """
    Main execution function.
    
    BEFORE RUNNING:
    1. Get Firecrawl API key from https://firecrawl.dev
    2. Set as environment variable:
       export FIRECRAWL_API_KEY='your_key_here'
    
    Note: Firecrawl Extract uses built-in LLM, no separate Anthropic key needed!
    """
    
    # Get API key from environment
    firecrawl_key = os.getenv('FIRECRAWL_API_KEY')
    
    if not firecrawl_key:
        print("❌ Error: FIRECRAWL_API_KEY not set")
        print("Get your key from https://firecrawl.dev and set it:")
        print("export FIRECRAWL_API_KEY='your_key_here'")
        return
    
    # Initialize scraper (only needs Firecrawl key now!)
    scraper = GreenpeaceCampaignScraper(firecrawl_key)
    
    # Run pipeline in test mode (processes only 5 URLs)
    records = scraper.run_full_pipeline(test_mode=True)
    
    # Save results
    if records:
        output_file = scraper.save_results(records)
        print(f"\n📊 Results saved to: {output_file}")
        print("\n✅ SUCCESS! Review the output file to see the structured data.")
        print("\n💡 TIP: To process all URLs, edit the script and change test_mode=False")
    else:
        print("\n⚠️  No companies found. This could mean:")
        print("  - The URLs scraped didn't contain campaign information")
        print("  - The content structure is different than expected")
        print("  - Try adjusting the URL filters or search terms")
        print("\n💡 TIP: Check the URLs being scraped - they may not have campaign content")


if __name__ == "__main__":
    main()