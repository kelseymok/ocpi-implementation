# OCPI Implementation POC
This is a quick and dirty (emphasis) POC to explore some of the components of an OCPI implementation (CDR)

## Components
* **OCPP Event Producer** of generated data for ChargePoints over 6 months
* **Kafka Stack** to receive OCPP Events
* **Consumer Reshaper** - pulls out fields important to downstream queries and stores the payload and priority fields to DynamoDB
* **Consumer Stop Transaction Dynamo** - triggers the creation of a CDR based on a StopTransaction Request 
* **OCPP Storage (DynamoDB)** - stores OCPI objects
* **OCPP Storage Seeder** - seeds some basic objects like Tariffs/Locations and tables
* **API** - gateway to OCPI objects

**Note:** these are not at all in production state and is just for demonstrative purposes.

## Quickstart

```bash
# If using Colima
colima start --cpu 4 --memory 8

docker-compose up
```
* Kafka Control Center: [http://localhost:9021/](http://localhost:9021/)

