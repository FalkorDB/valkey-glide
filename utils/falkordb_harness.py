#!/usr/bin/env python3
# Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
"""Isolated, digest-pinned FalkorDB 6.0.3 standalone/replica/cluster lifecycle."""

from __future__ import annotations

import argparse
import json
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Callable

from falkordb_protocol import Connection

if not __debug__:
    raise RuntimeError(
        "FalkorDB harness safety checks require assertions; optimized Python "
        "(-O, -OO or PYTHONOPTIMIZE) is unsupported"
    )

IMAGE = (
    "falkordb/falkordb@sha256:"
    "0fb85036662ce3d1206530a38fdf7df5a92b1fb31ad23087f8d4e89038ce0812"
)
LABEL = "io.valkey-glide.falkordb-harness"
OWNER = "graph-compatibility-v1"


def docker(*args: str, timeout: int = 120) -> str:
    """Run a bounded Docker operation and expose actionable failures."""
    result = subprocess.run(
        ["docker", *args], capture_output=True, text=True, timeout=timeout, check=False
    )
    if result.returncode:
        raise RuntimeError(f"docker {' '.join(args)}: {result.stderr.strip()}")
    return result.stdout.strip()


def wait_for(check: Callable[[], bool], description: str, seconds: int = 90) -> None:
    """Retry a readiness predicate, with a fixed deadline."""
    deadline = time.monotonic() + seconds
    last_error = ""
    while time.monotonic() < deadline:
        try:
            if check():
                return
        except (OSError, RuntimeError, ConnectionError) as error:
            last_error = str(error)
        time.sleep(0.2)
    raise RuntimeError(f"Timed out waiting for {description}: {last_error}")


class Harness:
    """Own only resources whose exact IDs and per-run labels are recorded."""

    def __init__(self, state_path: Path) -> None:
        """Create an inert harness; start/stop perform Docker operations."""
        root = Path(__file__).resolve().parents[1]
        if not state_path.resolve().is_relative_to(root):
            raise ValueError("Harness state must be inside this repository")
        self.path = state_path
        self.state: dict[str, object] = {}

    def save(self) -> None:
        """Persist resources immediately, allowing recovery after interruption."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        pending = self.path.with_suffix(self.path.suffix + ".next")
        pending.write_text(json.dumps(self.state, indent=2) + "\n", encoding="utf-8")
        pending.replace(self.path)

    def nodes(self) -> list[dict[str, object]]:
        """Return the manifest's endpoints."""
        nodes = self.state.get("nodes", [])
        assert isinstance(nodes, list)
        return nodes

    @staticmethod
    def connect(node: dict[str, object], protocol: int = 2) -> Connection:
        """Connect to a published loopback-only endpoint."""
        return Connection("127.0.0.1", int(str(node["port"])), protocol)

    def start(self, topology: str = "all") -> dict[str, object]:
        """Start fresh resources, or clean up this run if initialization fails."""
        if self.path.exists():
            raise FileExistsError(f"Refusing to overwrite state: {self.path}")
        if topology in ("all", "cluster") and sys.platform != "linux":
            raise RuntimeError(
                "FalkorDB cluster tests require a native Linux host with directly "
                "reachable Docker bridge addresses; Docker Desktop and remote "
                "Docker daemons are unsupported"
            )
        docker("info", "--format", "{{.ServerVersion}}")
        try:
            docker("image", "inspect", IMAGE)
        except RuntimeError:
            docker("pull", IMAGE, timeout=600)
        token = uuid.uuid4().hex
        self.state = {
            "format": 1,
            "owner": OWNER,
            "run": token,
            "image": IMAGE,
            "network": None,
            "nodes": [],
        }
        self.save()
        try:
            name = f"glide-graph-{token}"
            self.state["network"] = name
            self.save()
            network = docker(
                "network",
                "create",
                "--label",
                f"{LABEL}={OWNER}",
                "--label",
                f"{LABEL}.run={token}",
                name,
            )
            self.state["network"] = network
            self.save()
            if topology in ("all", "standalone", "replica"):
                primary = self.add_node("standalone", network, name)
                if topology in ("all", "replica"):
                    replica = self.add_node(
                        "replica",
                        network,
                        name,
                        ["--replicaof", str(primary["ip"]), "6379"],
                    )

                    def replicated() -> bool:
                        with self.connect(replica) as conn:
                            info = str(conn.command("INFO", "replication").plain())
                            return (
                                "master_link_status:up" in info
                                and "master_sync_in_progress:0" in info
                            )

                    wait_for(replicated, "standalone replica synchronization")
            if topology in ("all", "cluster"):
                cluster = [
                    self.add_node(
                        f"cluster-{index}",
                        network,
                        name,
                        [
                            "--cluster-enabled",
                            "yes",
                            "--cluster-config-file",
                            "nodes.conf",
                            "--cluster-node-timeout",
                            "5000",
                        ],
                    )
                    for index in range(6)
                ]
                self.check_cluster_reachability(cluster)
                docker(
                    "exec",
                    str(cluster[0]["id"]),
                    "redis-cli",
                    "--cluster",
                    "create",
                    *(f"{node['ip']}:6379" for node in cluster),
                    "--cluster-replicas",
                    "1",
                    "--cluster-yes",
                )

                wait_for(lambda: self.cluster_ready(cluster), "six-node cluster state")
            self.state["ready"] = True
            self.state["environment"] = self.environment()
            self.save()
            return self.state
        except BaseException:
            self.stop()
            raise

    @staticmethod
    def check_cluster_reachability(cluster: list[dict[str, object]]) -> None:
        """Verify every advertised redirect address from the actual client host."""
        for node in cluster:
            endpoint = f"{node['ip']}:6379"
            try:
                with Connection(str(node["ip"]), 6379, timeout=2) as conn:
                    if conn.command("PING").plain() != "PONG":
                        raise RuntimeError("Unexpected PING response")
            except (OSError, RuntimeError, ConnectionError) as error:
                raise RuntimeError(
                    f"Advertised cluster endpoint {endpoint} is unreachable from "
                    "this client host. FalkorDB cluster tests require a native "
                    "Linux host with directly reachable Docker bridge addresses; "
                    "a published loopback seed alone cannot route MOVED/ASK. "
                    "Docker Desktop, remote Docker daemons and isolated client "
                    "containers are unsupported."
                ) from error

    def cluster_ready(self, cluster: list[dict[str, object]]) -> bool:
        """Check cluster ownership and synchronization of each replica."""
        for node in cluster:
            with self.connect(node) as conn:
                info = str(conn.command("CLUSTER", "INFO").plain())
                if "cluster_state:ok" not in info:
                    return False
                replication = str(conn.command("INFO", "replication").plain())
                if "role:slave" in replication and (
                    "master_link_status:up" not in replication
                    or "master_sync_in_progress:0" not in replication
                ):
                    return False
        return True

    def add_node(
        self,
        role: str,
        network: str,
        name: str,
        extra: list[str] | None = None,
    ) -> dict[str, object]:
        """Create, record and probe one unprivileged, ephemeral server."""
        token = str(self.state["run"])
        container_name = f"{name}-{role}"
        node: dict[str, object] = {"id": container_name, "role": role}
        self.nodes().append(node)
        self.save()
        container = docker(
            "run",
            "--detach",
            "--name",
            container_name,
            "--label",
            f"{LABEL}={OWNER}",
            "--label",
            f"{LABEL}.run={token}",
            "--network",
            network,
            "--publish",
            "127.0.0.1::6379",
            "--user",
            "999:999",
            "--read-only",
            "--tmpfs",
            "/data:rw,mode=1777,size=64m",
            "--workdir",
            "/data",
            "--entrypoint",
            "redis-server",
            IMAGE,
            "--bind",
            "0.0.0.0",
            "--protected-mode",
            "no",
            "--port",
            "6379",
            "--dir",
            "/data",
            "--save",
            "",
            "--appendonly",
            "no",
            "--loadmodule",
            "/var/lib/falkordb/bin/falkordb.so",
            "TEMP_FOLDER",
            "/data",
            *(extra or []),
        )
        node["id"] = container
        self.save()
        details = json.loads(docker("inspect", container))[0]
        if not details["NetworkSettings"]["Ports"].get("6379/tcp"):
            raise RuntimeError(
                f"No published endpoint for {role}: {docker('logs', container)}"
            )
        node["port"] = int(
            details["NetworkSettings"]["Ports"]["6379/tcp"][0]["HostPort"]
        )
        node["ip"] = next(iter(details["NetworkSettings"]["Networks"].values()))[
            "IPAddress"
        ]
        self.save()

        def ready() -> bool:
            with self.connect(node) as conn:
                pong = conn.command("PING").plain() == "PONG"
                modules = conn.command("MODULE", "LIST").plain()
                if not pong or not isinstance(modules, list):
                    return False
                for module in modules:
                    if isinstance(module, list):
                        fields = dict(zip(module[::2], module[1::2]))
                        if fields.get("name") == "graph" and fields.get("ver") == 60003:
                            return True
                return False

        try:
            wait_for(ready, f"{role} PING and graph module version 60003")
        except RuntimeError as error:
            raise RuntimeError(f"{error}\n{docker('logs', container)}") from error
        return node

    def environment(self) -> dict[str, str]:
        """Expose the dedicated graph-test endpoint convention."""
        result = {}
        for role, variable in (
            ("standalone", "FALKORDB_STANDALONE_ENDPOINT"),
            ("replica", "FALKORDB_REPLICA_ENDPOINT"),
            ("cluster-0", "FALKORDB_CLUSTER_ENDPOINT"),
        ):
            for node in self.nodes():
                if node["role"] == role:
                    result[variable] = f"127.0.0.1:{node['port']}"
        return result

    def stop(self) -> None:
        """Remove exact recorded IDs after verifying both ownership labels."""
        if not self.state:
            if not self.path.exists():
                return
            self.state = json.loads(self.path.read_text(encoding="utf-8"))
        if self.state.get("owner") != OWNER or not self.state.get("run"):
            raise ValueError("Unrecognized harness state; refusing cleanup")
        failures = []
        resources = [("container", str(node["id"])) for node in self.nodes()]
        if self.state.get("network"):
            resources.append(("network", str(self.state["network"])))
        for kind, resource in resources:
            try:
                self.remove_owned(kind, resource)
            except (
                OSError,
                RuntimeError,
                ValueError,
                subprocess.TimeoutExpired,
            ) as error:
                failures.append(str(error))
        if failures:
            raise RuntimeError("; ".join(failures))
        self.path.unlink(missing_ok=True)
        self.path.with_suffix(self.path.suffix + ".next").unlink(missing_ok=True)

    def remove_owned(self, kind: str, resource: str) -> None:
        """Inspect one recorded resource and delete it only with matching labels."""
        result = subprocess.run(
            ["docker", kind, "inspect", resource],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        if result.returncode:
            if "No such" in result.stderr or "not found" in result.stderr:
                return
            raise RuntimeError(result.stderr.strip())
        detail = json.loads(result.stdout)[0]
        labels = (
            detail.get("Config", {}).get("Labels", {})
            if kind == "container"
            else detail.get("Labels", {})
        )
        if (
            labels.get(LABEL) != OWNER
            or labels.get(f"{LABEL}.run") != self.state["run"]
        ):
            raise ValueError(f"Ownership mismatch for {resource}")
        if kind == "container":
            docker("rm", "--force", "--volumes", resource)
        else:
            docker("network", "rm", resource)


def main() -> None:
    """Run the standalone lifecycle CLI."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("start", "stop"))
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--format", choices=("json", "env"), default="json")
    parser.add_argument(
        "--topology", choices=("all", "standalone", "replica", "cluster"), default="all"
    )
    args = parser.parse_args()
    harness = Harness(args.state)
    if args.action == "start":

        def interrupted(signum: int, _: object) -> None:
            raise InterruptedError(f"Received signal {signum}")

        signal.signal(signal.SIGTERM, interrupted)
        signal.signal(signal.SIGINT, interrupted)
        state = harness.start(args.topology)
        if args.format == "env":
            for variable, value in harness.environment().items():
                print(f"export {variable}={value}")
        else:
            print(json.dumps(state, indent=2))
    else:
        harness.stop()


if __name__ == "__main__":
    main()
