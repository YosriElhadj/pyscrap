from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from datetime import datetime
import time
import re

# Set up headless Chrome
options = Options()
options.add_argument('--headless=new')
options.add_argument('--window-size=1920x1080')
driver = webdriver.Chrome(options=options)

results = []
page = 1
max_pages = 20

while True:
    url = f"https://www.tayara.tn/fr/ads/c/Immobilier/?page={page}"
    print(f"🔄 Scraping page {page}...")
    driver.get(url)

    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "article.mx-0"))
        )
    except:
        print("❌ No more listings found or page load failed.")
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
            image = card.find_element(By.CSS_SELECTOR, "img").get_attribute("src")
            price_text = card.find_element(By.TAG_NAME, "data").text.strip()
            price = int(''.join(filter(str.isdigit, price_text)))
            location_span = card.find_elements(By.CSS_SELECTOR, "span.text-neutral-500")[-1]
            location = location_span.text.strip()

            results.append({
                "description": title,
                "price": price,
                "address": location,
                "images": [image],
                "sourceUrl": detail_url,
                "source": "tayara.tn",
                "listedDate": datetime.now().strftime("%Y-%m-%d"),
            })
        except Exception as e:
            print("⚠️ Skipped card due to error:", e)
            continue

    page += 1
    time.sleep(2)
    if page > max_pages:
        print("✅ Max pages reached.")
        break

driver.quit()

# Convert to DataFrame
df = pd.DataFrame(results)

# 🧼 Basic Cleaning
df.drop_duplicates(subset=["description", "price", "sourceUrl"], inplace=True)
df = df[df["price"] > 10]
df["description"] = df["description"].str.strip()
df["address"] = df["address"].str.replace(r"\s+", " ", regex=True).str.strip()
df.dropna(subset=["description", "price", "address"], inplace=True)
df["images"] = df["images"].apply(lambda imgs: [img for img in imgs if img.startswith("http")])

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
    text = text.lower()
    match = re.search(r"(\d{2,5})\s*(m²|m2| m)", text)
    if match:
        return int(match.group(1))
    return None

df["originalArea"] = df["description"].apply(extract_area_m2)
df["area"] = df["originalArea"].apply(lambda m2: round(m2 * 10.7639) if pd.notnull(m2) else None)
df["pricePerSqFt"] = df.apply(
    lambda row: round(row["price"] / row["area"], 2) if pd.notnull(row["price"]) and pd.notnull(row["area"]) else None,
    axis=1
)
df = df[df["pricePerSqFt"].between(2, 5000, inclusive="both") | df["pricePerSqFt"].isnull()]

# 🏠 Property type inference
def infer_property_type(description):
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

# 💸 Add original price format & price in USD
df["originalPrice"] = df["price"].apply(lambda p: f"{p:,} DT".replace(",", " "))
conversion_rate = 0.32
df["priceUSD"] = df["price"].apply(lambda p: round(p * conversion_rate, 2))

# 📐 Virtual area fields
df["areaInSqMeters"] = df["area"].apply(lambda ft: round(ft * 0.092903, 2) if pd.notnull(ft) else None)
df["areaInHectares"] = df["area"].apply(lambda ft: round(ft / 107639, 5) if pd.notnull(ft) else None)

# 📸 Flatten image array for CSV
df["images"] = df["images"].apply(lambda imgs: ", ".join(imgs))

# 💾 Save final cleaned & enriched CSV
df.to_csv("properties_enhanced.csv", index=False)
print(f"✅ Enhanced data saved to properties_enhanced.csv with {len(df)} entries.")
print("🔚 Scraping and enrichment complete.")
