import json
import os

import boto3
import uvicorn
from fastapi import FastAPI, Response

from response import ResponseBuilder, ResponseStatusCode
from data_reader import DataReader

app = FastAPI()

reader_host = os.environ.get("READER_HOST", "localhost")
reader_port = os.environ.get("READER_PORT", "8000")

dynamodb = boto3.client(
    'dynamodb',
    region_name='local',
    endpoint_url=f"http://{reader_host}:{reader_port}",
    aws_access_key_id="X",
    aws_secret_access_key="X"
)

data_reader = DataReader(dynamodb)


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/tariffs")
def get_tariffs():
    data = data_reader.get_all("Tariffs")
    response = ResponseBuilder(data=data, status_code=ResponseStatusCode.generic_success).format()  # success
    return Response(content=json.dumps(response), media_type="application/json")


@app.get("/locations")
def get_locations():
    data = data_reader.get_all("Locations")
    response = ResponseBuilder(data=data, status_code=ResponseStatusCode.generic_success).format()  # success
    return Response(content=json.dumps(response), media_type="application/json")


@app.get("/locations/{location_id}")
def get_locations_location_id(location_id: str):
    # data = data_reader.get_all("Locations")
    # response = ResponseBuilder(data=data, status_code=ResponseStatusCode.generic_success).format()  # success
    response = {
        "location_id": location_id,
    }
    return Response(content=json.dumps(response), media_type="application/json")


@app.get("/locations/{location_id}/{evse_uid}")
def get_locations_evse_uid(location_id: str, evse_uid: str):
    # data = data_reader.get_all("Locations")
    # response = ResponseBuilder(data=data, status_code=ResponseStatusCode.generic_success).format()  # success
    response = {
        "location_id": location_id,
        "evse_uid": evse_uid,
    }
    return Response(content=json.dumps(response), media_type="application/json")


@app.get("/locations/{location_id}/{evse_uid}/{connector_id}")
def get_locations_connector_id(location_id: str, evse_uid: str, connector_id: int):
    # data = data_reader.get_all("Locations")
    # response = ResponseBuilder(data=data, status_code=ResponseStatusCode.generic_success).format()  # success
    response = {
        "location_id": location_id,
        "evse_uid": evse_uid,
        "connector_id": connector_id
    }
    return Response(content=json.dumps(response), media_type="application/json")


@app.get("/cdrs")
def get_cdrs():
    data = data_reader.get_all("CDRs")
    response = ResponseBuilder(data=data, status_code=ResponseStatusCode.generic_success).format()  # success
    return Response(content=json.dumps(response), media_type="application/json")


#
# @app.get("/versions")
# def get_versions():
#     data = data_reader.get_all("Locations")
#     response = ResponseBuilder(data=data, status_code=ResponseStatusCode.generic_success).format()  # success
#     return Response(content=json.dumps(response), media_type="application/json")

# @app.get("/credentials")
# @app.get("/tokens")
# # country_code, party_id, token_uid, type
#
# @app.get("/commands")
# @app.get("/charging_profiles")
# @app.get("/hub_client_info")




if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)