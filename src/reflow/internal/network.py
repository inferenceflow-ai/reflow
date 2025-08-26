import logging
from dataclasses import dataclass, field
from typing import Optional, List

import netifaces


@dataclass(frozen=True)
class Address:
    ip: str
    port: int

    def ipc_bind_address(self):
        return ipc_address_for_port(self.port)

    def tcp_bind_address(self):
        return f'tcp://{self.ip}:{self.port}'


@dataclass
class WorkerDescriptor:
    cluster_size: int
    cluster_number: int
    worker_number: int
    engine_address: str = field(init=False)
    address: Optional[Address] = None


def ipc_address_for_port(port: int)->str:
    return f'ipc:///tmp/service_{port:04d}.sock'


def get_local_ipv4_addresses()->List[str]:
    result = []
    for iface in netifaces.interfaces():
        families = netifaces.ifaddresses(iface)
        if netifaces.AF_INET in families:
            for addr in netifaces.ifaddresses(iface)[netifaces.AF_INET]:
                result.append(addr['addr'])

    return result


def get_preferred_interface_ip(preferred_network: str)->str:
    logging.debug(f'looking for a bind address on this host that starts with {preferred_network}')
    bind_address = None
    ipv4_addresses = get_local_ipv4_addresses()
    for addr in ipv4_addresses:
        if addr.startswith(preferred_network):
            logging.debug(f'{addr} matches preferred network')
            bind_address = addr
            break
        else:
            logging.debug(f'{addr} does not match preferred network')

    if bind_address is None:
        raise RuntimeError(f'Could not find a bind address starting with {preferred_network}')

    return bind_address



