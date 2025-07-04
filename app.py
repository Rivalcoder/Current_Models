import pathway as pw
import threading
from fastapi import FastAPI, HTTPException
import uvicorn
import os
from exporter import export_loop

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

cache = {}
cache_lock = threading.Lock()
app = FastAPI()

def run_pipeline():
    products = pw.io.jsonlines.read(
        "mongo_exports/products.jsonl",
        schema=ProductSchema,
        mode="streaming"
    )
    transformed = products.select(
        _id=pw.this._id,
        name=pw.this.name,
        price=pw.this.price,
        category=pw.this.category,
        promo=pw.this.promoCopy
    ).with_id_from(pw.this._id)

    def update_cache(key, row, time, is_addition):
        with cache_lock:
            if is_addition:
                cache[str(key)] = row
            else:
                cache.pop(str(key), None)

    pw.io.subscribe(transformed, on_change=update_cache)
    pw.run()

@app.get("/products")
def get_all_products():
    with cache_lock:
        return list(cache.values())

@app.get("/products/{product_id}")
def get_product(product_id: str):
    with cache_lock:
        product = cache.get(product_id)
        if not product:
            raise HTTPException(status_code=404, detail="Product not found")
        return product

if __name__ == "__main__":
    threading.Thread(target=export_loop, daemon=True).start()
    threading.Thread(target=run_pipeline, daemon=True).start()
    uvicorn.run(app, host="0.0.0.0", port=7860)
