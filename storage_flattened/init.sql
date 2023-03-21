--create database flattenedocpp;
--CREATE SCHEMA flattenedocpp;

--GRANT usage on schema public to consumerflattener;


create table startTransactionRequest(
 messageId VARCHAR NOT NULL,
 messageType INT NOT NULL,
 chargePointId VARCHAR NOT NULL,
 actionName VARCHAR NOT NULL,
 writeTimestamp VARCHAR NOT NULL,
 writeTimestampEpoch bigint NOT NULL,
 connectorId INT,
 idTag VARCHAR,
 meterStart FLOAT,
 eventTimestamp VARCHAR
);

create table startTransactionResponse(
 messageId VARCHAR NOT NULL,
 messageType INT NOT NULL,
 chargePointId VARCHAR NOT NULL,
 actionName VARCHAR NOT NULL,
 writeTimestamp VARCHAR NOT NULL,
 writeTimestampEpoch bigint NOT NULL,
 bodyTransactionId INT,
 bodyIdTagInfoStatus VARCHAR,
 bodyIdTagInfoParentIdTag VARCHAR,
 bodyIdTagInfoExpiryDate VARCHAR
);

create table stopTransactionRequest(
 messageId VARCHAR NOT NULL,
 messageType INT NOT NULL,
 chargePointId VARCHAR NOT NULL,
 actionName VARCHAR NOT NULL,
 writeTimestamp VARCHAR NOT NULL,
 writeTimestampEpoch bigint NOT NULL,
 transactionId INT,
 reason VARCHAR,
 idTag VARCHAR,
 bodyTransactionData VARCHAR
);

create table stopTransactionResponse(
 messageId VARCHAR NOT NULL,
 messageType INT NOT NULL,
 chargePointId VARCHAR NOT NULL,
 actionName VARCHAR NOT NULL,
 writeTimestamp VARCHAR NOT NULL,
 writeTimestampEpoch bigint NOT NULL,
 bodyIdTagInfoStatus VARCHAR,
 bodyIdTagInfoParentIdTag VARCHAR,
 bodyIdTagInfoExpiryDate VARCHAR
);


create table heartbeatRequest(
 messageId VARCHAR NOT NULL,
 messageType INT NOT NULL,
 chargePointId VARCHAR NOT NULL,
 actionName VARCHAR NOT NULL,
 writeTimestamp VARCHAR NOT NULL,
 writeTimestampEpoch bigint NOT NULL
);

create table heartbeatResponse(
 messageId VARCHAR NOT NULL,
 messageType INT NOT NULL,
 chargePointId VARCHAR NOT NULL,
 actionName VARCHAR NOT NULL,
 writeTimestamp VARCHAR NOT NULL,
 writeTimestampEpoch bigint NOT NULL,
 bodyCurrentTime VARCHAR
);

create table bootNotificationRequest(
 messageId VARCHAR NOT NULL,
 messageType INT NOT NULL,
 chargePointId VARCHAR NOT NULL,
 actionName VARCHAR NOT NULL,
 writeTimestamp VARCHAR NOT NULL,
 writeTimestampEpoch bigint NOT NULL,
 bodyChargePointModel VARCHAR,
 bodyChargePointVendor VARCHAR,
 bodyChargeBoxSerialNumber VARCHAR,
 bodyChargePointSerialNumber VARCHAR,
 bodyFirmwareVersion VARCHAR,
 bodyIccid VARCHAR,
 bodyImsi VARCHAR,
 bodyMeterSerialNumber VARCHAR,
 bodyMeterType VARCHAR
);

create table bootNotificationResponse(
 messageId VARCHAR NOT NULL,
 messageType INT NOT NULL,
 chargePointId VARCHAR NOT NULL,
 actionName VARCHAR NOT NULL,
 writeTimestamp VARCHAR NOT NULL,
 writeTimestampEpoch bigint NOT NULL,
 bodyCurrentTime VARCHAR,
 bodyInterval INT,
 bodyStatus VARCHAR
);

create table meterValuesRequest(
 messageId VARCHAR NOT NULL,
 messageType INT NOT NULL,
 chargePointId VARCHAR NOT NULL,
 actionName VARCHAR NOT NULL,
 writeTimestamp VARCHAR NOT NULL,
 writeTimestampEpoch bigint NOT NULL,
 eventValue FLOAT,
 context VARCHAR,
 format VARCHAR,
 measurand VARCHAR,
 phase VARCHAR,
 location VARCHAR,
 unit VARCHAR,
 connectorId INT,
 transactionId INT,
 eventTimestamp VARCHAR
);

create table meterValuesResponse(
 messageId VARCHAR NOT NULL,
 messageType INT NOT NULL,
 chargePointId VARCHAR NOT NULL,
 actionName VARCHAR NOT NULL,
 writeTimestamp VARCHAR NOT NULL,
 writeTimestampEpoch bigint NOT NULL
);

-- Ideally that shouldn't be a superuser
create user consumerflattener with encrypted password 'example' LOGIN SUPERUSER;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO consumerflattener;
GRANT ALL PRIVILEGES ON DATABASE flattenedocpp TO consumerflattener;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO consumerflattener;

create user consumerstoptransaction with encrypted password 'example' LOGIN SUPERUSER;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO consumerstoptransaction;
GRANT ALL PRIVILEGES ON DATABASE flattenedocpp TO consumerstoptransaction;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO consumerstoptransaction;