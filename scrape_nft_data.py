from __future__ import absolute_import, unicode_literals
from celery import shared_task
from .scraper import scrape_nft_data
from .models import Collection, Nft, Wallet
from .purchase import purchase_nft
import logging

# Get an instance of a logger
logger = logging.getLogger(__name__)

@shared_task
def scrape_nft_data_task(collection_name, max_price, wallet_id):
    logger.debug(f"Received request to start scraping for collection: {collection_name} with max price: {max_price} and wallet_id: {wallet_id}")

    if not collection_name or not max_price or not wallet_id:
        logger.error("Missing collection_name, max_price or wallet_id in request data")
        return {"message": "collection_name, max_price and wallet_id are required", "status": "400"}

    try:
        max_price = float(max_price)
    except ValueError:
        logger.error("Invalid max_price value provided")
        return {"message": "max_price should be a valid number", "status": "400"}

    try:
        wallet = Wallet.objects.get(id=wallet_id)
    except Wallet.DoesNotExist:
        logger.error("Wallet does not exist")
        return {"message": "Wallet does not exist", "status": "404"}

    data = scrape_nft_data(collection_name)
    logger.debug(f"Scraped data: {data}")

    if not data:
        logger.warning(f"No data found for the given collection: {collection_name}")
        return {"message": "No data found for the given collection", "status": "404"}

    for item in data:
        try:
            # Remove any non-numeric characters (e.g., currency symbols) and convert to float
            item_price = float(''.join(c for c in item['price'] if c.isdigit() or c == '.'))
            logger.debug(f"Item price: {item_price}")

            if item_price <= max_price:
                collection, created = Collection.objects.get_or_create(
                    name=collection_name,
                    defaults={'address': collection_name}
                )
                Nft.objects.update_or_create(
                    collection=collection,
                    name=item['name'],
                    defaults={'price': item_price, 'url': item['url'], 'is_purchased': False}
                )
        except ValueError:
            logger.error(f"Invalid price value for item: {item}")

    logger.info(f"Scraping completed for collection: {collection_name}. Data updated.")

    # Fetch NFTs from the database
    nfts_to_purchase = Nft.objects.filter(collection__name=collection_name, price__lte=max_price, is_purchased=False)

    # Loop through the filtered NFTs and purchase each one
    for nft in nfts_to_purchase:
        purchase_nft(nft.url, wallet)
        nft.is_purchased = True  # Update the NFT status to purchased
        nft.save()

    return {"message": "Scraping completed, data updated, and purchase process initiated", "status": "200"}
