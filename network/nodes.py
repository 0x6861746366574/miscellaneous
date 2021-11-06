import argparse
import json
import time
from threading import Lock, Thread

from requests.exceptions import RequestException
from zenlog import log

from client.ResourceLoader import load_resources, locate_blockchain_client_class


class NodeDownloader:
    def __init__(self, resources, thread_count):
        self.resources = resources
        self.thread_count = thread_count

        self.api_client_class = locate_blockchain_client_class(resources)
        self.visited_hosts = set()
        self.remaining_api_clients = []
        self.host_to_node_info_map = {}
        self.busy_thread_count = 0
        self.lock = Lock()

    def discover(self):
        log.info('seeding crawler with known hosts')
        for node_descriptor in self.resources.nodes.find_all_by_role(None):
            api_client = self.api_client_class(node_descriptor.host)
            self.remaining_api_clients.append(api_client)

        log.info('starting {} crawler threads'.format(self.thread_count))
        threads = [Thread(target=self._discover_thread) for i in range(0, self.thread_count)]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        log.info('crawling completed and discovered {} nodes'.format(len(self.host_to_node_info_map)))

    def _discover_thread(self):
        while self.remaining_api_clients or self.busy_thread_count:
            with self.lock:
                api_client = self._pop_next_api_client()
                if not api_client:
                    time.sleep(1)
                    continue

            log.debug('processing {} [{} discovered, {} remaining, {} busy]'.format(
                api_client.node_host,
                len(self.host_to_node_info_map),
                len(self.remaining_api_clients),
                self.busy_thread_count))

            is_failure = False
            try:
                json_peers = api_client.get_peers()
                json_node = api_client.get_node_info()
            except RequestException:
                log.warning('failed to load peers from {}:{}'.format(api_client.node_host, api_client.node_port))
                is_failure = True

            with self.lock:
                if not is_failure:
                    self._update(api_client.node_host, json_peers, json_node)

                self.busy_thread_count -= 1

    # this function must be called in context of self.lock
    def _pop_next_api_client(self):
        api_client = None
        while self.remaining_api_clients:
            api_client = self.remaining_api_clients.pop(0)
            if api_client and api_client.node_host not in self.visited_hosts:
                break

            api_client = None

        if not api_client:
            return None

        self.visited_hosts.add(api_client.node_host)
        self.busy_thread_count += 1
        return api_client

    # this function must be called in context of self.lock
    def _update(self, host, json_peers, json_node):
        self.host_to_node_info_map[host] = json_node
        self.remaining_api_clients += [
            self.api_client_class.from_node_info_dict(json_peer, retry_count=1, timeout=3) for json_peer in json_peers
        ]

    def save(self, output_filepath):
        log.info('saving nodes json to {}'.format(output_filepath))
        with open(output_filepath, 'w') as outfile:
            json.dump(list(self.host_to_node_info_map.values()), outfile)


def main():
    parser = argparse.ArgumentParser(description='downloads node information from a network')
    parser.add_argument('--resources', help='directory containing resources', required=True)
    parser.add_argument('--output', help='output file', required=True)
    parser.add_argument('--thread-count', help='number of threads', default=16)
    args = parser.parse_args()

    resources = load_resources(args.resources)
    downloader = NodeDownloader(resources, args.thread_count)
    downloader.discover()
    downloader.save(args.output)


if '__main__' == __name__:
    main()
