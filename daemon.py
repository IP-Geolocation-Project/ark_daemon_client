import uuid
import sys
import json
import grpc
import time
import queue
import logging
import argparse
import threading

from pathlib import Path
from typing import Optional
from datetime import timedelta
from dataclasses import dataclass, field

from scamper import ScamperCtrl, ScamperInst, ScamperFile

from colorama import Fore, Style

from ark_grpc import ark_daemon_pb2
from ark_grpc import ark_daemon_pb2_grpc
 
CLIENT_OPTIONS = [
    ("grpc.keepalive_time_ms", 120_000),
    ("grpc.keepalive_timeout_ms", 30_000),
    ("grpc.keepalive_permit_without_calls", 1),
    ("grpc.http2.max_pings_without_data", 0),
    ("grpc.max_send_message_length", -1),
    ("grpc.max_receive_message_length", -1),
]

REGISTER_TIMEOUT_S = 60      
BULK_POLL_TIMEOUT_S = 60         
RECONNECT_BACKOFF = [5, 10, 20, 40, 60]
 
def get_logger(name: str):
    class CustomFormatter(logging.Formatter):
        green = Fore.GREEN
        grey = Style.DIM
        yellow = Fore.YELLOW
        red = Fore.RED
        bold_red = f"{Style.BRIGHT}{Fore.RED}"
        reset = Style.RESET_ALL

        _format_str = "%(asctime)s - %(filename)s - %(levelname)s - [Thread: %(thread)d] - %(message)s"

        FORMATS = {
            logging.DEBUG: grey + _format_str + reset,
            logging.INFO: green + _format_str + reset,
            logging.WARNING: yellow + _format_str + reset,
            logging.ERROR: red + _format_str + reset,
            logging.CRITICAL: bold_red + _format_str + reset,
        }

        def format(self, record):
            log_fmt = self.FORMATS.get(record.levelno)
            formatter = logging.Formatter(log_fmt)
            return formatter.format(record)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(CustomFormatter())
    logger.addHandler(stream_handler)

    log_path = Path("./logs/ark_daemon.log")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    if log_path.exists():
        log_path.unlink(missing_ok=True)
    log_path.touch(exist_ok=True)

    f_handler = logging.FileHandler(log_path)
    f_handler.setLevel(logging.DEBUG)
    f_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(filename)s - %(levelname)s - [Thread: %(thread)d] - %(message)s")
    )
    logger.addHandler(f_handler)

    return logger

logger = get_logger("daemon")

class ArkMeasurementConductor:
    """
    This class manages the measurement requests and results, interacting with ScamperCtrl to perform measurements.
    """
    def __init__(
        self,
        measurement_requests: queue.Queue,
        measurement_results: queue.Queue,
        mux: str,
        # output_file: str = "measurement_data",
    ):
        self.measurement_requests = measurement_requests
        self.measurement_results = measurement_results

        self.ctrl = ScamperCtrl(mux=mux)
        # self.scamper_file = ScamperFile(output_file, mode="w", kind="warts.gz")
        self.available_instances: dict = self.initialize_available_vps()
        self.vp_data = self.get_all_available_vps()

        self.initialized = False
        
        self._loads_lock = threading.Lock()
        self.loads_of_nodes: dict[int, dict] = {}

    def start(self):
        self.initialized = True

    def initialize_available_vps(self) -> dict[str, ScamperInst]:
        ctrl_vps = self.ctrl.vps()
        vps = [vp.name for vp in ctrl_vps]

        added_names = []
        for vp in ctrl_vps:
            if vp.name in vps:
                added_names.append(vp.name)
                self.ctrl.add_vps(vp)

        available_instances = {}
        for inst in self.ctrl.instances():
            if inst.name in added_names:
                available_instances[inst.name] = inst

        logger.info(f"{len(available_instances)} instances out of {len(ctrl_vps)} vps are available for usage")
        return available_instances

    def get_all_available_vps(self) -> list[dict]:
        vp_data = []
        if self.ctrl:
            for vp in self.ctrl.vps():
                vp_data.append({
                    "name": vp.name,
                    "shortname": vp.shortname,
                    "asn": vp.asn4,
                    "ipv4": vp.ipv4,
                    "country_code": vp.cc,
                    "state": vp.st,
                    "place": vp.place,
                    "location": list(vp.loc) if vp.loc else [],
                    "tags": list(vp.tags),
                    "is_anchor": True,

                })
        return vp_data

    def get_load_of_nodes(self, instances) -> dict[str, int]:
        """
        A method to get the current load of nodes for a given list of instances. 
        The load is a dictionary where the keys are the instance shortnames and the values are the corresponding task counts.
      
        :param instances: A list of ScamperInst instances for which to retrieve the load.
        :return: A dictionary mapping instance shortnames to their current task counts.
        """
        return {inst.shortname: inst.taskc for inst in instances}

    def get_load_of_nodes_for_response(self, measurement_id: int, instance_shortname: str) -> dict[str, int]:
        """
        A method to get the load of the instance with the given shortname for a specific measurement ID.

        :param measurement_id: The ID of the measurement for which to retrieve the load.
        :param instance_shortname: The shortname of the instance for which to retrieve the load.
        :return: A dictionary mapping the instance shortname to its current task count for the given measurement ID. 
        """
        with self._loads_lock:
            node_loads = self.loads_of_nodes.get(measurement_id, {})
            return {instance_shortname: node_loads.get(instance_shortname, 0)}

    def cleanup_load_data(self, measurement_id: int):
        with self._loads_lock:
            self.loads_of_nodes.pop(measurement_id, None)

    def _extract_measurement_information(self, measurement):
        target = measurement.target
        vps = list(measurement.vps)
        measurement_id = measurement.measurement_id
        vps = [vp if ".ark.caida.org" in vp else f"{vp}.ark.caida.org" for vp in vps]

        target_instances = [
            inst for inst in self.ctrl.instances() if inst.name in vps or inst.shortname in vps
        ]
        logger.debug(f"Target: {target} | MeasurementID: {measurement_id} | VPs: {','.join(vps)}")
        return target, target_instances, int(measurement_id)

    def _do_measurement(self, target: str, instances, measurement_id: int) -> None:
        try:
            self.ctrl.do_ping(target, inst=instances, userid=measurement_id)
        except RuntimeError as e:
            logger.exception(f"ScamperCtrl rejected measurement {measurement_id}")
        except Exception:
            logger.exception(f"Exception scheduling ping for measurement {measurement_id}")

        with self._loads_lock:
            self.loads_of_nodes[measurement_id] = self.get_load_of_nodes(instances).copy()

        logger.debug(
            f"Scheduling measurement {measurement_id} for {target} using instances={[i.shortname for i in instances]} - current load={self.loads_of_nodes[measurement_id]}"
        )

    def measurement_worker(self, stop_event: threading.Event):
        while not self.initialized:
            time.sleep(0.5)

        logger.info("Measurement worker started")
        idle_streak = 0

        while not stop_event.is_set():
            did_work = False

            # Schedule any pending measurements
            drained = 0
            while drained < 50:
                try:
                    measurement = self.measurement_requests.get_nowait()
                    target, target_instances, measurement_id = self._extract_measurement_information(measurement)
                    self._do_measurement(target, target_instances, measurement_id)
                    did_work = True
                    drained += 1
                except queue.Empty:
                    break
                except Exception:
                    logger.exception("Error scheduling measurement")

            # Poll for completed results
            try:
                output = self.ctrl.poll(timeout=timedelta(seconds=0.05))
                if output:
                    
                    try:
                        measurement_id = output.userid
                        dst = output.dst
                        vp_short = output.inst.shortname
                        logger.debug(
                            f"Result: measurement_id={measurement_id} target={dst} vp={vp_short}"
                        )
                    except Exception:
                        pass

                    result_entry = (time.monotonic(), output)
                    self.measurement_results.put(result_entry, timeout=5)
                    did_work = True
            except queue.Full:
                logger.warning("measurement_results queue full — backpressure active")
            except Exception:
                logger.exception("Exception polling ScamperCtrl")

            if did_work:
                idle_streak = 0
            else:
                idle_streak += 1
                time.sleep(min(0.5, 0.01 * idle_streak))

        logger.info("Measurement worker stopped")

    def close(self):
        # self.scamper_file.close()
        pass


@dataclass
class DaemonConfig:
    HOSTNAME: str 
    PORT: str 
    DAEMON_ID: str = field(default_factory=lambda: str(uuid.uuid4()))
    STATUS: bool = field(default=False)
    GRPC_CHANNEL_CREDENTIALS: Optional[object] = field(default=None)

    def get_grpc_server_address(self):
        return f"{self.HOSTNAME}:{self.PORT}"

    def get_daemon_info_message(self):
        return ark_daemon_pb2.DaemonInfo(daemon_id=str(self.DAEMON_ID))

    def init_grpc_channel_credentials(self):
        self.GRPC_CHANNEL_CREDENTIALS = grpc.ssl_channel_credentials()
        return self.GRPC_CHANNEL_CREDENTIALS

    def check_daemon_id(self, daemon_id: str) -> bool:
        return str(self.DAEMON_ID) == str(daemon_id)


class ManagedChannel:
    """
    A class that manages a gRPC channel and its associated stub, providing automatic reconnection logic.
    """

    def __init__(
            self, 
            daemon_config: DaemonConfig, 
            name: str,
            secure: bool, 
            on_reconnect=None
        ):
        self.daemon_config = daemon_config
        self.secure = secure
        self.name = name
        self._on_reconnect = on_reconnect

        self._lock = threading.Lock()
        self.successful_reconnect_attempt = 0
        self.channel = None
        self.stub = None
        self._open_channel()

    def _open_channel(self):
        if self.channel is not None:
            try:
                self.channel.close()
            except Exception:
                pass

        addr = self.daemon_config.get_grpc_server_address()
        if self.secure:
            self.channel = grpc.secure_channel(
                addr,
                self.daemon_config.init_grpc_channel_credentials(),
                options=CLIENT_OPTIONS,
            )
        else:
            self.channel = grpc.insecure_channel(addr, options=CLIENT_OPTIONS)

        self.stub = ark_daemon_pb2_grpc.MeasurementServiceStub(self.channel)
        logger.info(f"[{self.name}] channel opened to {addr}")

    def reconnect(self, failed_attempt: Optional[int] = None) -> bool:
        with self._lock:
            if failed_attempt is not None and self.successful_reconnect_attempt != failed_attempt:
                logger.debug(f"[{self.name}] already reconnected — skipping")
                return True

            logger.warning(f"[{self.name}] reconnecting ...")

            attempt = 0
            while True:
                self._open_channel()

                ok = True
                if self._on_reconnect is not None:
                    try:
                        ok = self._on_reconnect()
                    except Exception:
                        logger.exception(f"[{self.name}] on_reconnect callback failed")
                        ok = False

                if ok:
                    self.successful_reconnect_attempt += 1
                    logger.info(
                        f"[{self.name}] reconnected ({self.successful_reconnect_attempt}st successful attempt)"
                    )
                    return True

                backoff = RECONNECT_BACKOFF[min(attempt, len(RECONNECT_BACKOFF) - 1)]
                attempt += 1
                logger.error(
                    f"[{self.name}] reconnect attempt {attempt} failed — retrying in {backoff}s"
                )
                time.sleep(backoff)


class GRPCManager:
    def __init__(
        self,
        ark_measurement_conductor: ArkMeasurementConductor,
        daemon_config: DaemonConfig,
        measurement_requests: queue.Queue,
        measurement_results: queue.Queue,
        secure: bool = False,
        use_short_names: bool = False,
    ):
        self.daemon_config = daemon_config
        self.measurement_requests = measurement_requests
        self.measurement_results = measurement_results
        self.secure = secure
        self.use_short_names = use_short_names
        self.ark_measurement_conductor = ark_measurement_conductor

        # start two independent channels to process incoming requests and send the measurement results
        # the requests channel is also used to register the daemon with the gRPC server. 
        self.request_channel = ManagedChannel(
            daemon_config=daemon_config, 
            secure=secure, 
            name="work",
            on_reconnect=self.register_daemon,
        )
        self.result_channel = ManagedChannel(
            daemon_config=daemon_config, 
            secure=secure, 
            name="result",
            on_reconnect=None, 
        )

    def register_daemon(self) -> bool:
        try:
            response = self.request_channel.stub.RegisterDaemon(
                self.daemon_config.get_daemon_info_message(),
                wait_for_ready=True,
                timeout=REGISTER_TIMEOUT_S,
            )

            daemon_id = response.daemon_info.daemon_id
            if not self.daemon_config.check_daemon_id(daemon_id):
                logger.error(
                    f"Daemon ID mismatch: expected {self.daemon_config.DAEMON_ID}, got {daemon_id}"
                )
                return False

            self.ark_measurement_conductor.start()

            self.get_vantage_points()  
            
            logger.info(f"Daemon {self.daemon_config.DAEMON_ID} registered")
            return True
        except Exception as e:
            logger.error(f"Registration failed: {e}")
            return False


    def get_vantage_points(self):
        try:
            stub = self.request_channel.stub
            vp_ack = stub.GetAllVantagePoints(
                self.daemon_config.get_daemon_info_message(),
                wait_for_ready=True, timeout=5,
            )
            if not vp_ack.status:
                return None

            vps = self.ark_measurement_conductor.vp_data
            vp_resp = ark_daemon_pb2.VP_RESP(
                daemon_info=self.daemon_config.get_daemon_info_message(),
                vantage_points_data=json.dumps(vps),
            )
            stub.SendVantagePoints(vp_resp, wait_for_ready=True)
            logger.info(f"Sent {len(vps)} vantage points to server")
        except Exception:
            logger.exception("Failed to handle vantage points")

    def request_worker(self, stop_event: threading.Event):
        """
        A thread that periodically polls the gRPC server for the measurement requests. 
        The received requests are put into the measurement_requests queue for processing by the measurement worker thread of the ArkMeasurementConductor class.
        """
        logger.info(f"Request worker started")

        idle_streak = 0

        while not stop_event.is_set():
            successful_reconnect_attempt = self.request_channel.successful_reconnect_attempt
            try:
                stub = self.request_channel.stub
                bulk = stub.GetMeasurementRequestsBulk(
                    self.daemon_config.get_daemon_info_message(),
                    wait_for_ready=True,
                    timeout=BULK_POLL_TIMEOUT_S,
                )

                count = 0
                for req in bulk.measurement_requests:
                    logger.info(f"[polling] Got measurement {req.measurement_id}")
                    try:
                        self.measurement_requests.put(req, timeout=10)
                        count += 1
                    except queue.Full:
                        logger.warning("measurement_requests queue full — dropping request")

                idle_streak = 0 if count > 0 else idle_streak + 1

            except grpc.RpcError as e:
                code = e.code()

                if code == grpc.StatusCode.DEADLINE_EXCEEDED:
                    # server did not send anything within the timeout
                    idle_streak += 1
               
                elif code == grpc.StatusCode.UNAVAILABLE:
                    logger.error(f"Bulk request RPC unavailable — reconnecting: {code}")
                    self.request_channel.reconnect(successful_reconnect_attempt)

                elif code == grpc.StatusCode.OUT_OF_RANGE:
                    # server informed that no requests are available 
                    idle_streak += 1

                elif code == grpc.StatusCode.RESOURCE_EXHAUSTED:
                    # is no longer implemented on the server side
                    logger.error("Daemon capacity exhausted on server side")
                    idle_streak += 1

                elif code in (grpc.StatusCode.UNAUTHENTICATED, grpc.StatusCode.FAILED_PRECONDITION):
                    # Daemon was not registred on server. Re-register.
                    logger.warning(f"Daemon registration was not successful -  error code ({code}) — reconnecting")
                    self.request_channel.reconnect(successful_reconnect_attempt)
                else:
                    logger.exception(f"Bulk request RPC error: {code}")
                    idle_streak += 1
            except Exception:
                logger.exception("Exception in polling request loop")
                idle_streak += 1

            # Only sleep when idle. 
            if idle_streak > 0:
                time.sleep(min(1.0, 0.05 * idle_streak))

        logger.info("Request worker stopped")

    def result_worker(self, stop_event: threading.Event):
        """
        A method that periodically fetches the new measurement results from the measurement_results queue and sends them to the gRPC server in bulk.
        The server returns IDs of the measurements that successfully received all measurement results.
        The method then cleans up the load data for the acknowledged measurements.
        """
        logger.info(f"Result worker started")

        while not stop_event.is_set():
            successful_reconnect_attempt = self.result_channel.successful_reconnect_attempt
            try:
                results, msm_ids = self._drain_result_queue(max_count=100)
                if not results:
                    time.sleep(0.1)
                    continue

                logger.info(f"[polling] Sending {len(results)} results to server")
                bulk_msg = ark_daemon_pb2.MSM_RES_SHORT_BULK(measurement_results=results)

                try:
                    stub = self.result_channel.stub
                    ack_bulk = stub.SendMeasurementResultsBulk(
                        bulk_msg, wait_for_ready=True, timeout=30
                    )
                    acked_ids = {int(a.measurement_id) for a in ack_bulk.measurement_acknowledgements}
                    unacked = [mid for mid in msm_ids if mid not in acked_ids]
                    if unacked:
                        logger.error(f"{len(unacked)} measurements not acknowledged: {unacked}")
                    for mid in acked_ids:
                        self.ark_measurement_conductor.cleanup_load_data(mid)

                except grpc.RpcError as e:
                    code = e.code()
                    if code in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.CANCELLED, grpc.StatusCode.DEADLINE_EXCEEDED):
                        logger.error(f"Connection in the SendMeasurementResultsBulk with code({code}) — reconnecting result channel")
                        self.result_channel.reconnect(successful_reconnect_attempt)
                    elif code in (grpc.StatusCode.UNAUTHENTICATED, grpc.StatusCode.FAILED_PRECONDITION):
                        # Registration is done via the request_worker channel, not the result_worker channel.
                        # Asking request_worker channel to reconnect.
                        logger.warning(f"Server rejected results ({code}) — asking request_worker channel to reconnect")
                        self.request_channel.reconnect()
                    else:
                        logger.exception(f"Bulk send RPC error: {code}")
                        time.sleep(1)
            except Exception:
                logger.exception("Exception in polling send loop")
                time.sleep(0.5)

        logger.info("Result worker stopped")

    def _build_short_result(self, result_entry, include_load: bool = False):
        _, msm_result = result_entry

        target = msm_result.dst
        vp = msm_result.inst.name
        vp_short = msm_result.inst.shortname
        measurement_id = msm_result.userid
        instance_name = vp if not self.use_short_names else vp_short

        try:
            min_rtt = msm_result.min_rtt.total_seconds() * 1000
        except Exception:
            min_rtt = -1.0

        logger.debug(f"Sending result: mid={measurement_id} target={target} vp={vp_short} rtt={min_rtt:.2f}ms")

        kwargs = dict(
            measurement_id=str(measurement_id),
            target=str(target),
            instance=str(instance_name),
            min_rtt=min_rtt,
        )
        if include_load:
            load = self.ark_measurement_conductor.get_load_of_nodes_for_response(measurement_id, vp_short)
            if load:
                kwargs["load_of_nodes"] = load

        return ark_daemon_pb2.MSM_RES_SHORT(**kwargs), measurement_id

    def _drain_result_queue(self, max_count: int = 100):
        results = []
        msm_ids = []

        try:
            result_entry = self.measurement_results.get(timeout=0.1)
            msg, mid = self._build_short_result(result_entry, include_load=True)
            results.append(msg)
            msm_ids.append(mid)
        except queue.Empty:
            return results, msm_ids

        while len(results) < max_count:
            try:
                result_entry = self.measurement_results.get_nowait()
                msg, mid = self._build_short_result(result_entry, include_load=True)
                results.append(msg)
                msm_ids.append(mid)
            except queue.Empty:
                break

        return results, msm_ids

def main():
    parser = argparse.ArgumentParser(description="Optimized Ark Daemon gRPC Client")
    parser.add_argument("--hostname", type=str, required=True)
    parser.add_argument("--port", type=str, required=True)
    parser.add_argument("--mux", type=str, required=True)
    parser.add_argument("--secure", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--use-short-names", action="store_true")
    args = parser.parse_args()

    logger.setLevel(logging.DEBUG if args.debug else logging.INFO)
    logger.info(f"Starting daemon | secure={args.secure} | host={args.hostname}:{args.port}")

    measurement_worker_stop = threading.Event()
    request_worker_stop = threading.Event()
    result_worker_stop = threading.Event()

    threads: list[threading.Thread] = []
    ark_measurement_conductor = None

    try:
        measurement_requests = queue.Queue()
        measurement_results = queue.Queue()

        ark_measurement_conductor = ArkMeasurementConductor(
            measurement_requests=measurement_requests,
            measurement_results=measurement_results,
            mux=args.mux
        )
        # with open('active_vps.txt', 'w') as f:
        #     for vp_name in ark_measurement_conductor.vp_data:
        #         f.write(f"{vp_name},\n")

        daemon_config = DaemonConfig(HOSTNAME=args.hostname, PORT=args.port)
        grpc_manager = GRPCManager(
            ark_measurement_conductor=ark_measurement_conductor,
            daemon_config=daemon_config,
            measurement_requests=measurement_requests,
            measurement_results=measurement_results,
            secure=args.secure,
            use_short_names=args.use_short_names,
        )

        logger.info("Attempting to register daemon with server ...")
        while not grpc_manager.register_daemon():
            logger.info("Retrying registration in 5s ...")
            time.sleep(5)
        logger.info("Daemon registered successfully")
 
        measurement_thread = threading.Thread(
            target=ark_measurement_conductor.measurement_worker,
            args=(measurement_worker_stop,),
            daemon=False, name="measurement-worker",
        )
        request_thread = threading.Thread(
            target=grpc_manager.request_worker,
            args=(request_worker_stop,),
            daemon=False, name="request-worker",
        )
        result_thread = threading.Thread(
            target=grpc_manager.result_worker,
            args=(result_worker_stop,),
            daemon=False, name="result-worker",
        )
        threads.extend([measurement_thread, request_thread, result_thread])

        measurement_thread.start()
        request_thread.start()
        time.sleep(0.5)
        result_thread.start()
        logger.info("All worker threads running")

        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received")
    except Exception:
        logger.exception("Fatal exception in main — shutting down")
    finally:
        logger.info("Initiating shutdown ...")
        request_worker_stop.set()
        result_worker_stop.set()
        measurement_worker_stop.set()

        for t in threads:
            t.join(timeout=10)

        if ark_measurement_conductor is not None:
            ark_measurement_conductor.close()
        logger.info("Client stopped.")


if __name__ == "__main__":
    main()