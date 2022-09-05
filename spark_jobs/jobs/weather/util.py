import urllib3


http = urllib3.PoolManager()


def download_file(url: str, path: str, chunk_size: int = 2**16) -> None:
    r = http.request('GET', url, preload_content=False)
    with open(path, 'wb') as out:
        while True:
            data = r.read(chunk_size)
            if not data:
                break
            out.write(data)
    r.release_conn()
