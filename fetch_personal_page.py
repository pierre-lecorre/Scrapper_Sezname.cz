import requests
import csv
from bs4 import BeautifulSoup
import random
import time

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

def extract_detailed_info(soup):
    # Initialize data dictionary
    data = {
        'Address': None,
        'Web': [],
        'Social Sites': [],
        'Mobile': [],
        'Phone': [],
        'Email': [],
        'ICO': None
    }

    # Extract Address
    address_div = soup.find('div', class_='detailAddress')
    if address_div:
        address_text = address_div.get_text(strip=True)
        data['Address'] = address_text

    # Extract Web URLs
    web_links = soup.find_all('a', class_='detailWebUrl')
    for link in web_links:
        if 'href' in link.attrs:
            data['Web'].append(link['href'])

    # Extract Social Sites
    social_sites_div = soup.find('div', class_='detailSocialNetworks')
    if social_sites_div:
        social_links = social_sites_div.find_all('a', href=True)
        for link in social_links:
            data['Social Sites'].append(link['href'])

    # Extract Phones
    phone_sections = soup.find_all('h2', class_='label')
    for section in phone_sections:
        label = section.get_text(strip=True)
        phone_div = section.find_next_sibling('div', class_='value')
        if phone_div:
            phone_span = phone_div.find('span', {'data-dot': 'origin-phone-number'})
            if phone_span:
                phone_number = phone_span.get_text(strip=True)
                if label == 'Mobil':
                    data['Mobile'].append(phone_number)
                elif label == 'Telefon':
                    data['Phone'].append(phone_number)

    # Extract Emails
    email_links = soup.find_all('a', {'data-dot': 'e-mail'})
    for email_link in email_links:
        if 'href' in email_link.attrs:
            data['Email'].append(email_link['href'].replace('mailto:', ''))

    # Extract ICO
    ico_div = soup.find('div', class_='detailBusinessInfo')
    if ico_div:
        ico_text = ico_div.get_text(strip=True)
        data['ICO'] = ico_text.split()[0] if ico_text else None

    return data


def process_csv(input_csv, output_csv):
    with open(input_csv, 'r', encoding='utf-8') as infile, open(output_csv, 'w', newline='',
                                                                encoding='utf-8') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        # Read and write header row
        headers = next(reader, None)  # Read the header row
        if headers:
            writer.writerow(['URL', 'Address', 'Web', 'Social Sites', 'Mobile', 'Phone', 'Email', 'ICO'])

        for row in reader:
            if not row:  # Skip empty rows
                continue

            # Adjust the column index here if needed
            url = row[1].strip()  # Assuming the 'href' column is index 1
            if url and url.lower() != 'href':  # Skip rows where URL is 'href'
                # Make sure URL has a proper scheme
                if not url.startswith(('http://', 'https://')):
                    url = 'https://' + url  # Prepend 'https://' if scheme is missing

                print(f"Processing URL: {url}")
                soup = fetch_website(url)
                if soup:  # Proceed only if fetch was successful
                    detailed_info = extract_detailed_info(soup)
                    # Write row to output CSV
                    writer.writerow([
                        url,
                        detailed_info['Address'],
                        '; '.join(detailed_info['Web']),
                        '; '.join(detailed_info['Social Sites']),
                        '; '.join(detailed_info['Mobile']),
                        '; '.join(detailed_info['Phone']),
                        '; '.join(detailed_info['Email']),
                        detailed_info['ICO']
                    ])
                else:
                    # Log failed fetch attempt
                    writer.writerow([url, 'Failed to fetch', '', '', '', '', '', ''])

# Example usage
if __name__ == "__main__":
    input_csv = 'company_info.csv'  # Input CSV with URLs
    output_csv = 'company_info_detailed.csv'  # Output CSV with detailed info
    process_csv(input_csv, output_csv)
