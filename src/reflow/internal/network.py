import logging
import socket
from dataclasses import dataclass


@dataclass(frozen=True)
class Address:
    ip: str
    port: int

    def ipc_bind_address(self):
        return ipc_address_for_port(self.port)

    def tcp_bind_address(self):
        return f'tcp://{self.ip}:{self.port}'


@dataclass
class QueueDescriptor:
    address: Address
    cluster_size: int
    cluster_number: int

def ipc_address_for_port(port: int)->str:
    return f'ipc:///tmp/service_{port:04d}.sock'

def get_preferred_interface_ip(preferred_network: str)->str:
    logging.debug(f'looking for a bind address on this host that starts with {preferred_network}')
    addresses = socket.getaddrinfo(None, 80, family=socket.AF_INET, type=socket.SOCK_STREAM)
    ips = [address[4][0] for address in addresses]
    bind_address = None
    for ip in ips:
        if ip.startswith(preferred_network):
            logging.debug(f'{ip} matches preferred network')
            bind_address = ip
            break
        else:
            logging.debug(f'{ip} does not match preferred network')

    if bind_address is None:
        raise RuntimeError(f'Could not find a bind address starting with {preferred_network}')

    return bind_address

