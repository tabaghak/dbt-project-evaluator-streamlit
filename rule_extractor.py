#!/usr/bin/env python3
"""
dbt Project Evaluator Rules Generator

This script fetches dbt project evaluator rule documentation from GitHub
and generates a comprehensive JSON structure for each rule.

Author: AI Assistant
Date: 2025
"""

import requests
import re
import json
from typing import Dict, List, Optional
from dataclasses import dataclass


@dataclass
class Rule:
    """Data class to represent a dbt project evaluator rule"""
    name: str
    description: str
    example: str
    exception: str
    reason_to_flag: str
    remediation: str
    table_name: str


class DBTRuleParser:
    """Parses markdown content from dbt project evaluator documentation https://github.com/dbt-labs/dbt-project-evaluator"""
    
    def __init__(self):
        self.urls = {
            'Modeling': 'https://raw.githubusercontent.com/dbt-labs/dbt-project-evaluator/refs/heads/main/docs/rules/modeling.md',
            'Testing': 'https://raw.githubusercontent.com/dbt-labs/dbt-project-evaluator/refs/heads/main/docs/rules/testing.md',
            'Structure': 'https://raw.githubusercontent.com/dbt-labs/dbt-project-evaluator/refs/heads/main/docs/rules/structure.md',
            'Documentation': 'https://raw.githubusercontent.com/dbt-labs/dbt-project-evaluator/refs/heads/main/docs/rules/documentation.md',
            'Governance': 'https://raw.githubusercontent.com/dbt-labs/dbt-project-evaluator/refs/heads/main/docs/rules/governance.md',
            'Performance': 'https://raw.githubusercontent.com/dbt-labs/dbt-project-evaluator/refs/heads/main/docs/rules/performance.md'
        }

    def fetch_content(self, url: str) -> str:
        """Fetch content from a URL"""
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response.text
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            return ""

    def parse_rule_sections(self, content: str) -> List[Rule]:
        """Parse individual rule sections from markdown content"""
        rules = []
        
        # Split content by ## headers (rule sections)
        sections = re.split(r'\n## ', content)
        
        for section in sections[1:]:  # Skip first empty section
            try:
                rule = self._parse_single_rule(section)
                if rule:
                    rules.append(rule)
            except Exception as e:
                print(f"Error parsing rule section: {e}")
                continue
        
        return rules

    def _parse_single_rule(self, section: str) -> Optional[Rule]:
        """Parse a single rule section"""
        lines = section.strip().split('\n')
        if not lines:
            return None
        
        # Extract rule name from first line
        name = lines[0].strip()
        
        # Extract table name from first paragraph with `fct_` pattern
        table_match = re.search(r'`(fct_[^`]+)`', section)
        table_name = table_match.group(1) if table_match else f"fct_{name.lower().replace(' ', '_')}"
        
        # Extract description (first paragraph)
        description_match = re.search(r'`[^`]+`[^.]*\. ([^.]+\.)', section)
        description = description_match.group(1).strip() if description_match else f"Shows issues related to {name.lower()}"
        
        # Extract example section
        example_match = re.search(r'\*\*Example\*\*\s*\n\n([^*]+?)(?=\n\*\*|\n---|$)', section, re.DOTALL)
        example = example_match.group(1).strip() if example_match else "Example not specified in documentation"
        
        # Extract exception section
        exception_match = re.search(r'\*\*Exception[s]?\*\*\s*\n\n([^*]+?)(?=\n\*\*|\n---|$)', section, re.DOTALL)
        exception = exception_match.group(1).strip() if exception_match else "Not specified in documentation"
        
        # Extract reason to flag
        reason_match = re.search(r'\*\*Reason to Flag\*\*\s*\n\n([^*]+?)(?=\n\*\*|\n---|$)', section, re.DOTALL)
        reason_to_flag = reason_match.group(1).strip() if reason_match else f"This pattern violates dbt best practices for {name.lower()}"
        
        # Extract remediation
        remediation_match = re.search(r'\*\*How to Remediate\*\*\s*\n\n([^*]+?)(?=\n\*\*|\n---|$)', section, re.DOTALL)
        remediation = remediation_match.group(1).strip() if remediation_match else f"Follow dbt best practices to resolve {name.lower()} issues"
        
        return Rule(
            name=name,
            description=description,
            example=example,
            exception=exception,
            reason_to_flag=reason_to_flag,
            remediation=remediation,
            table_name=table_name
        )

    def generate_complete_json(self) -> Dict:
        """Generate the complete JSON structure with all rules and diagrams"""
        result = {}
        
        for category, url in self.urls.items():
            print(f"Processing {category}...")
            
            # Fetch content
            content = self.fetch_content(url)
            if not content:
                print(f"Skipping {category} due to fetch error")
                continue
            
            # Parse rules
            rules = self.parse_rule_sections(content)
            if not rules:
                print(f"No rules found for {category}")
                continue
            
            # Generate JSON structure for this category
            category_data = {"rules": {}}
            
            for rule in rules:
                try:
                    # Create rule entry
                    rule_data = {
                        "name": rule.name,
                        "description": rule.description,
                        "example": rule.example,
                        "exception": rule.exception,
                        "reason_to_flag": rule.reason_to_flag,
                        "remediation": rule.remediation
                    }
                    
                    category_data["rules"][rule.table_name] = rule_data
                    print(f"  ✓ Added rule: {rule.name}")
                    
                except Exception as e:
                    print(f"  ✗ Error processing rule {rule.name}: {e}")
                    continue
            
            result[category] = category_data
            print(f"Completed {category} with {len(category_data['rules'])} rules")
        
        return result

    def save_to_file(self, data: Dict, filename: str = "dbt_project_evaluator_rules.json"):
        """Save the generated data to a JSON file"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            print(f"✓ Successfully saved to {filename}")
        except Exception as e:
            print(f"✗ Error saving to file: {e}")


def main():
    """Main function to run the dbt rules generator"""
    print("🚀 dbt Project Evaluator Rules Generator")
    print("=" * 50)
    print("DEBUG: Main function started")
    
    # Initialize parser
    parser = DBTRuleParser()
    
    # Generate complete JSON structure
    print("\n📥 Fetching and parsing documentation...")
    rules_data = parser.generate_complete_json()
    
    if not rules_data:
        print("❌ No data generated. Exiting.")
        return
    
    # Save to file
    print(f"\n💾 Saving results...")
    parser.save_to_file(rules_data)
    
    # Print summary
    total_rules = sum(len(category["rules"]) for category in rules_data.values())
    print(f"\n📊 Summary:")
    print(f"   Total categories: {len(rules_data)}")
    print(f"   Total rules: {total_rules}")
    
    for category, data in rules_data.items():
        rule_count = len(data["rules"])
        print(f"   {category}: {rule_count} rules")
    
    print("\n✅ Generation complete!")
    
    # Optionally print a sample rule
    if rules_data:
        sample_category = list(rules_data.keys())[0]
        sample_rule_key = list(rules_data[sample_category]["rules"].keys())[0]
        sample_rule = rules_data[sample_category]["rules"][sample_rule_key]
        
        print(f"\n🔍 Sample rule ({sample_category}):")
        print(f"   Name: {sample_rule['name']}")
        print(f"   Description: {sample_rule['description'][:100]}...")


if __name__ == "__main__":
    # Install required packages
    required_packages = ["requests"]
    
    try:
        import requests
    except ImportError:
        print("❌ Missing required package: requests")
        print("Please install with: pip install requests")
        exit(1)
    
    main()