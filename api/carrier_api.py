from fastapi import FastAPI, Query
from pydantic import BaseModel
from pymongo import MongoClient


mongo_client = MongoClient('mongodb://admin:password@mongodb:27017/')
db = mongo_client['parcel_db']
parcel_collection = db['parcel_orders']


class OrderRequest(BaseModel):
    order_id: int

class OrderResponse(BaseModel):
    order_id: int
    tracking_number: str

app = FastAPI()

def get_next_tracking_number():
    # gets last tracking number
    result = parcel_collection.find_one({}, sort=[("tracking_number", -1)])
    print(f'result: {result}')
    if result != None:
        tracking = result["tracking_number"]
        last_number = int(tracking.replace('JVLG', ''))
        next_number = last_number + 1
        return next_number
    else:
        return 1

i = get_next_tracking_number()

def insert_order(order, tracking):
    parcel_collection.insert_one({'order_id':order,'tracking_number':tracking})

@app.post("/create-tracking")
def create_tracking_number(request: OrderRequest):
    order_id = request.order_id
    global i
    i += 1
    random_tracking = 'JVLG' + str(i).zfill(10)
    insert_order(order_id, random_tracking)
    return OrderResponse(order_id=order_id, tracking_number=random_tracking)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
