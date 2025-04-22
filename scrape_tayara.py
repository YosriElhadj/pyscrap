import argparse
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from datetime import datetime
import time
import re
import json
import os
import sys

def scrape_tayara(location=None):
    """
    Scrape real estate listings from tayara.tn
    
    Args:
        location: Optional location filter
    """
    print("Starting tayara.tn scraper...")
    
    # Set up headless Chrome with Windows-compatible options
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--window-size=1920x1080')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')  # Important for Windows
    
    try:
        # Try with default ChromeDriver location
        driver = webdriver.Chrome(options=options)
    except Exception as e:
        print(f"Error initializing Chrome: {e}")
        print("Trying with explicit driver path...")
        try:
            # Try with current directory
            script_dir = os.path.dirname(os.path.abspath(__file__))
            chromedriver_path = os.path.join(script_dir, "chromedriver.exe")
            service = Service(chromedriver_path)
            driver = webdriver.Chrome(service=service, options=options)
        except Exception as e2:
            print(f"Failed to initialize Chrome with explicit path: {e2}")
            print("Please ensure ChromeDriver is installed and in PATH")
            return []

    # Prepare URL with location filter if provided
    base_url = "https://www.tayara.tn/fr/ads/c/Immobilier"
    if location:
        # Clean and format location for URL
        location_param = location.lower().strip().replace(' ', '-')
        base_url = f"{base_url}/{location_param}"

    results = []
    page = 1
    max_pages = 20

    while True:
        url = f"{base_url}/?page={page}"
        print(f"🔄 Scraping page {page}...")
        driver.get(url)

        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "article.mx-0"))
            )
        except Exception as e:
            print(f"❌ Page load failed: {e}")
            break

        cards = driver.find_elements(By.CSS_SELECTOR, "article.mx-0")
        if not cards:
            print("⛔ No listings found on this page.")
            break

        for card in cards:
            try:
                a_tag = card.find_element(By.TAG_NAME, "a")
                detail_url = "https://www.tayara.tn" + a_tag.get_attribute("href")
                title = card.find_element(By.CSS_SELECTOR, "h2.card-title").text.strip()
                
                try:
                    image = card.find_element(By.CSS_SELECTOR, "img").get_attribute("src")
                except:
                    image = ""
                
                try:
                    price_text = card.find_element(By.TAG_NAME, "data").text.strip()
                    price = int(''.join(filter(str.isdigit, price_text)))
                except:
                    # If we can't find or parse the price, set a default
                    price = 0
                    
                try:
                    location_span = card.find_elements(By.CSS_SELECTOR, "span.text-neutral-500")[-1]
                    location = location_span.text.strip()
                except:
                    location = "Unknown"

                # Scrape the detail page to get more information
                if len(results) < 100:  # Limit detailed scrapes to 100 to avoid too much time
                    try:
                        driver.execute_script("window.open('');")
                        driver.switch_to.window(driver.window_handles[1])
                        driver.get(detail_url)
                        
                        # Wait for detail page to load
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.TAG_NAME, "article"))
                        )
                        
                        # Try to extract more images
                        imgs = driver.find_elements(By.CSS_SELECTOR, "img.rounded-md")
                        image_urls = [img.get_attribute("src") for img in imgs if img.get_attribute("src")]
                        
                        # Get full description
                        description_elem = driver.find_elements(By.CSS_SELECTOR, "div.description")
                        full_description = description_elem[0].text.strip() if description_elem else title
                        
                        # Close tab and switch back
                        driver.close()
                        driver.switch_to.window(driver.window_handles[0])
                    except Exception as e:
                        print(f"⚠️ Error scraping detail page: {e}")
                        # Reset back to main window if something went wrong
                        if len(driver.window_handles) > 1:
                            driver.close()
                            driver.switch_to.window(driver.window_handles[0])
                        full_description = title
                        image_urls = [image] if image else []
                else:
                    full_description = title
                    image_urls = [image] if image else []

                results.append({
                    "description": full_description,
                    "price": price,
                    "address": location,
                    "images": image_urls if isinstance(image_urls, list) else [image_urls],
                    "sourceUrl": detail_url,
                    "source": "tayara.tn",
                    "listedDate": datetime.now().strftime("%Y-%m-%d"),
                })
                
                if len(results) % 5 == 0:
                    print(f"Found {len(results)} listings so far...")
                    
            except Exception as e:
                print(f"⚠️ Skipped card due to error: {str(e)[:100]}...")
                continue

        page += 1
        time.sleep(2)
        if page > max_pages or len(results) >= 100:  # Limit to 100 listings total for faster processing
            print("✅ Max pages or listings limit reached.")
            break

    driver.quit()
    print(f"Finished scraping. Found {len(results)} listings.")
    return results

def clean_and_enhance_data(results):
    """
    Clean and enhance the scraped data
    """
    # Convert to DataFrame
    df = pd.DataFrame(results)
    
    if df.empty:
        print("No data to clean and enhance")
        return df
        
    # 🧼 Basic Cleaning
    try:
        df.drop_duplicates(subset=["description", "price", "sourceUrl"], inplace=True)
        df = df[df["price"] > 10]
        df["description"] = df["description"].str.strip()
        df["address"] = df["address"].str.replace(r"\s+", " ", regex=True).str.strip()
        df.dropna(subset=["description", "price", "address"], inplace=True)
        
        # Fix images list if they're already strings
        if df.shape[0] > 0 and isinstance(df.iloc[0]["images"], str):
            df["images"] = df["images"].apply(lambda img_str: img_str.split(","))
        
        # Ensure all image URLs are valid    
        df["images"] = df["images"].apply(lambda imgs: [img for img in imgs if isinstance(img, str) and img.startswith("http")])
    except Exception as e:
        print(f"Error in basic cleaning: {e}")

    # 🏛 Governorate detection
    governorates = [
        "Tunis", "Ariana", "Ben Arous", "Manouba", "Nabeul", "Bizerte", "Zaghouan",
        "Beja", "Jendouba", "Kef", "Siliana", "Sousse", "Monastir", "Mahdia", "Kairouan",
        "Kasserine", "Sidi Bouzid", "Sfax", "Gabes", "Medenine", "Tataouine", "Tozeur", "Kebili", "Gafsa"
    ]
    df["governorate"] = df["address"].apply(
        lambda addr: next((gov for gov in governorates if gov.lower() in addr.lower()), "Unknown")
    )

    # 📏 Area extraction & price per sq ft
    def extract_area_m2(text):
        if not isinstance(text, str):
            return None
        text = text.lower()
        match = re.search(r"(\d{1,5})\s*(m²|m2| m)", text)
        if match:
            return int(match.group(1))
        return None

    try:
        df["originalArea"] = df["description"].apply(extract_area_m2)
        df["area"] = df["originalArea"].apply(lambda m2: round(m2 * 10.7639) if pd.notnull(m2) else None)
        df["pricePerSqFt"] = df.apply(
            lambda row: round(row["price"] / row["area"], 2) if pd.notnull(row["price"]) and pd.notnull(row["area"]) and row["area"] > 0 else None,
            axis=1
        )
        
        # Filter out unreasonable price per sqft values, but keep nulls
        df = df[df["pricePerSqFt"].between(1, 5000, inclusive="both") | df["pricePerSqFt"].isnull()]
    except Exception as e:
        print(f"Error in area extraction: {e}")

    # 🏠 Property type inference
    def infer_property_type(description):
        if not isinstance(description, str):
            return "autre"
        desc = description.lower()
        if "terrain" in desc:
            if "agricole" in desc:
                return "terrain_agricole"
            elif "industriel" in desc:
                return "terrain_industriel"
            elif "commercial" in desc:
                return "terrain_commercial"
            else:
                return "terrain_construction"
        return "autre"

    df["propertyType"] = df["description"].apply(infer_property_type)

    # 🏗️ Zoning inference
    def infer_zoning(description):
        if not isinstance(description, str):
            return "unknown"
        desc = description.lower()
        if any(word in desc for word in ["villa", "appartement", "studio", "immeuble", "résidence"]):
            return "residential"
        elif any(word in desc for word in ["dépôt", "industriel", "usine"]):
            return "industrial"
        elif "commercial" in desc or "magasin" in desc:
            return "commercial"
        elif "agricole" in desc or "ferme" in desc:
            return "agricultural"
        return "unknown"

    df["zoning"] = df["description"].apply(infer_zoning)

    # 💸 Add original price format
    df["originalPrice"] = df["price"].apply(lambda p: f"{p:,} DT".replace(",", " "))

    # Add geocoordinates based on governorate (approximate)
    geocoords = {
        "Tunis": [10.1815, 36.8065],
        "Ariana": [10.1939, 36.8625],
        "Ben Arous": [10.2233, 36.7535],
        "Sousse": [10.6412, 35.8245],
        "Sfax": [10.7600, 34.7400],
        "Monastir": [10.8090, 35.7780]
    }
    
    def get_approx_coords(governorate):
        if governorate in geocoords:
            # Add a small random offset to avoid all properties in same location
            import random
            lat_offset = random.uniform(-0.01, 0.01)
            lng_offset = random.uniform(-0.01, 0.01)
            return [geocoords[governorate][0] + lng_offset, geocoords[governorate][1] + lat_offset]
        return [10.1815, 36.8065]  # Default to Tunis
    
    df["coordinates"] = df["governorate"].apply(get_approx_coords)

    # 📸 Convert image list to string for CSV
    df["image_list"] = df["images"].copy()
    df["images"] = df["images"].apply(lambda imgs: ", ".join(imgs) if isinstance(imgs, list) else imgs)

    # Add nearWater, roadAccess, utilities features
    df["nearWater"] = df["description"].apply(lambda desc: "mer" in desc.lower() or "lac" in desc.lower() if isinstance(desc, str) else False)
    df["roadAccess"] = True  # Assume all properties have road access
    df["utilities"] = True   # Assume all properties have utilities

    return df

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Scrape real estate listings from tayara.tn')
    parser.add_argument('--location', help='Location to search for properties', default=None)
    args = parser.parse_args()

    print(f"🚀 Starting scraper for location: {args.location or 'all locations'}")
    
    # Run the scraper
    results = scrape_tayara(args.location)
    
    if not results:
        print("⚠️ No results found!")
        return
    
    print(f"✅ Scraped {len(results)} properties")
    
    # Clean and enhance the data
    df = clean_and_enhance_data(results)
    
    if df.empty:
        print("❌ No properties after cleaning!")
        return
    
    print(f"✅ Cleaned data - {len(df)} properties remaining")
    
    # Save to CSV
    csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "properties_enhanced.csv")
    df.to_csv(csv_path, index=False)
    print(f"💾 Saved to {csv_path}")
    
    # Also save to JSON format that's easier for Node.js to parse
    # Convert all NaN/None values to null for JSON compatibility
    df_json = df.where(pd.notnull(df), None)
    
    # Save properties to JSON
    json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "properties.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(df_json.to_dict(orient="records"), f, ensure_ascii=False, indent=2)
    
    print(f"💾 Saved to {json_path}")
    print("🔚 Scraping complete")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)