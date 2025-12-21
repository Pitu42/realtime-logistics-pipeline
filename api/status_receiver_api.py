from fastapi import FastAPI, Query
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel
from pymongo import MongoClient
import datetime
import uvicorn


mongo_client = MongoClient('mongodb://admin:password@mongodb:27017/')
db = mongo_client['parcel_db']
orders_collection = db['purchase_orders']

class OrderRequest(BaseModel):
    order_id: int
    tracking_number: str
    status: str
    status_time: str 

app = FastAPI()

def insert_order(order, tracking):
    parcel_collection.insert_one({'order_id':order,'tracking_number':tracking})

@app.post("/send-status")
def create_tracking_number(request: OrderRequest):
    try:
        order_id = request.order_id
        tracking_number = request.tracking_number
        status = request.status
        status_time = request.status_time
        
        result = orders_collection.update_one(
            {"order_id":order_id},
            {
                "$set": {
                    "status":status,
                    "status_date":status_time
                    }
                }
            )
        
        return Response(status_code=200)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")

# For debugging
@app.post("/debug-send-status")
def debug_endpoint(request: dict):
    print("Raw request data:", request)
    print("Data types:", {k: type(v).__name__ for k, v in request.items()})
    return {"received": request}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
