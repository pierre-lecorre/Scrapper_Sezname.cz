import requests
from bs4 import BeautifulSoup
import csv
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import time
import random

def fetch_website(url, retries=3, delay=5):
    """Fetch the website with retry mechanism and latency."""
    attempt = 0
    while attempt < retries:
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise an error for bad responses
            return BeautifulSoup(response.text, 'html.parser')
        except requests.RequestException as e:
            print(f"Exception occurred while fetching website {url}: {e}")
            attempt += 1
            wait_time = delay * (2 ** attempt) + random.uniform(0, 1)  # Exponential backoff with jitter
            print(f"Retrying in {wait_time:.2f} seconds...")
            time.sleep(wait_time)
    print(f"Failed to fetch website {url} after {retries} attempts.")
    return None

def extract_company_info(soup, page_number):
    """Extract company information from the BeautifulSoup object."""
    if not soup:
        return []
    company_links = soup.find_all('a', class_="companyTitle statCompanyDetail")
    companies = []
    for link in company_links:
        href = link['href']
        name = link.get_text(strip=True)
        companies.append({"name": name, "href": href, "page": page_number})
    return companies

def save_to_csv(companies, filename):
    """Save company information to a CSV file."""
    try:
        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=["name", "href", "page"])
            writer.writeheader()
            writer.writerows(companies)
    except Exception as e:
        print(f"Exception occurred while saving to CSV: {e}")

def update_url_for_next_page(url, page_number):
    """Update the URL to point to the next page."""
    url_parts = list(urlparse(url))
    query = parse_qs(url_parts[4])
    if page_number > 1:
        query['page'] = str(page_number)
    else:
        query.pop('page', None)
    url_parts[4] = urlencode(query, doseq=True)
    return urlunparse(url_parts)

def main(url):
    """Main function to orchestrate the scraping process."""
    all_companies = []
    page_number = 1

    while True:
        current_page_url = update_url_for_next_page(url, page_number)
        print(f"Fetching: {current_page_url}")
        soup = fetch_website(current_page_url)

        if not soup:
            print(f"Failed to fetch page {page_number}. Stopping.")
            break

        companies = extract_company_info(soup, page_number)
        if not companies:
            print(f"No more companies found on page {page_number}. Stopping.")
            break

        all_companies.extend(companies)
        save_to_csv(all_companies, "company_info.csv")
        page_number += 1

    print("Company information saved to company_info.csv")

# Start the process with the initial URL
initial_url = "https://www.firmy.cz/?q=fotovoltaick%C3%A9"
main(initial_url)
