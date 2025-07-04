import pathway as pw
import threading
from fastapi import FastAPI, HTTPException
import uvicorn
import time

# Define your product schema
class ProductSchema(pw.Schema):
    _id: str
    name: str
    price: float
    description: str
    category: str
    stock: int
    imageUrl: str
    isNew: bool
    isActive: bool
    createdBy: str
    ratings: dict
    createdAt: str
    updatedAt: str
    promoCopy: str

# Global cache and lock for thread safety
cache = {}
cache_lock = threading.Lock()

# FastAPI instance
app = FastAPI()

def run_pipeline():
    """Function to run the Pathway pipeline in a background thread."""
    # Read data in streaming mode from JSONL
    products = pw.io.jsonlines.read(
        "mongo_exports/products.jsonl",
        schema=ProductSchema,
        mode="streaming"
    )

    # Transform the product table
    transformed = products.select(
        _id=pw.this._id,
        name=pw.this.name,
        price=pw.this.price,
        category=pw.this.category,
        promo=pw.this.promoCopy
    ).with_id_from(pw.this._id)

    # Update cache on change
    def update_cache(key, row, time, is_addition):
        product_id = str(key)
        with cache_lock:
            if is_addition:
                cache[product_id] = row
            else:
                cache.pop(product_id, None)

    # Subscribe to changes in transformed data
    pw.io.subscribe(transformed, on_change=update_cache)

    # Run the Pathway pipeline
    pw.run()

# API endpoint: get all products
@app.get("/products")
def get_all_products():
    with cache_lock:
        return list(cache.values())

# API endpoint: get product by ID
@app.get("/products/{product_id}")
def get_product(product_id: str):
    with cache_lock:
        product = cache.get(product_id)
        if not product:
            raise HTTPException(status_code=404, detail="Product not found")
        return product

if __name__ == "__main__":
    # Start the Pathway pipeline in a background thread
    pipeline_thread = threading.Thread(target=run_pipeline, daemon=True)
    pipeline_thread.start()

    # Start the FastAPI server
    uvicorn.run(app, host="0.0.0.0", port=8000)
