# Ark Daemon Client
This repository contains the code of the gRPC client that can be used in combination with Bigfoot to conduct measurements on CAIDA Ark platform.

## Client
The client consists of two modules:
* `ArkMeasurementConductor` - schedules measurements on Ark platform and retrieves the results.
* `GRPCManager` - implements gRPC stubs to communicate with Bigfoot gRPC server. It implements the following methods:
  1. `RegisterDaemon` - is used to register the client with Bigfoot gRPC server.
  2. `GetAllVantagePoints` - returns a boolean flag to inform the client whether the list of all available vantage points should be sent to the server.
  3. `SendVantagePoints` - is used to send the list of the available vantage points to the server.
  4. `GetMeasurementRequestsBulk` - is used to get the list of the measurement requests, defined by measurement ID, target and set of vantage points, that should be scheduled on Ark.
  5. `SendMeasurementResultsBulk` - is used to send each result obtained from Ark back to the server. The aggregation of the results is implemented on the Bigfoot server.

In order to prevent the client from stalling in case one RPC gets stuck, there are two channels running in parallel:
* __request_channel__ - is responsible for the initialization of the daemon (`RegisterDaemon`, `GetAllVantagePoints`, `SendVantagePoints`) and processing of the incoming measurement requests (`GetMeasurementRequestsBulk`)
* __result_channel__ - is responsible for sending the results of the measurements back to the server (`SendMeasurementResultsBulk`).

A separate daemon thread (__ArkMeasurementConductor.measurement_worker__) is responsible for scheduling the measurements on Ark and for retrieval of the measurement results.

## Installation & Startup
### Installation
This Ark Daemon Client should be used in combination with Bigfoot, in order to run measurements on Ark. The client and its dependencies should be installed on the machine that has access to Ark platform. In the latest release the client was tested on a machine with Python 3.10.12.

> [!IMPORTANT]
> Users should contact [CAIDA](https://www.caida.org/) to get access to Ark platform.

To install the daemon follow the steps below:
* Clone the repository to the machine with access to Ark.

  ```bash
  git clone https://github.com/IP-Geolocation-Project/ark_daemon_client
  ```
* Install the requirements (described in section below).
* Run the client (described in the section below).

### Requirements
Before using it for the first time, check the following items:
* Make sure that __scamper__ library is available on your machine - https://www.caida.org/projects/ark/.
* Make sure that a Unix domain socket for Ark is available on the machine (more at https://www.caida.org/catalog/software/scamper/python/).
* Install the dependencies defined in `requirements.txt` (located in the directory where __ark_daemon_client__ is cloned).

  ```pip3 install -r requirements.txt```

### Runtime parameters
On startup user is able to configure the following CLI parameters:
* `--hostname` - __required__ parameter - defines the hostname of the gRPC server.
* `--port` - __required__ parameter - defines the port of the gRPC server.
* `--mux` - __required__ parameter - defines the Unix domain socket used to instantiate ScamperCtrl.
* `--secure` - __False__ by default - defines whether client should create a secure channel to communicate with the gRPC server.
* `--debug` - __False__ by default - sets the log level to DEBUG if true and INFO otherwise.
* `--use-short-names` - __False__ by default - defines whether client should use short vantage point names (without .ark.caida.org) when communicating with the gRPC server.


## Usage
The following command can be used to run the client:
```bash
python3 daemon.py --use-short-names --debug --secure --hostname <hostname> --port <port> --mux <mux>
```

The daemon maintains the connection to the server using the following logic:
- On startup the daemon retries registration every 5 seconds, until the server accepts.
- If an established connection drops, it reconnects with backoff of 5/10/20/40/60s, staying at 60s after that. The daemon retries indefinitely until the connection is re-established or daemon is stopped by the user.

## Logging
The log files are stored in the `logs` directory in the `ark_daemon.log` file. The log file is deleted on each new startup of the daemon.
