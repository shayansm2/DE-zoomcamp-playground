import os


def download(url: str) -> str:
    file_name = get_file_name(url)
    os.system(f"wget {url} -O {file_name}")
    return file_name


def download_if_not_exists(url: str) -> str:
    file_name = get_file_name(url)
    if os.system(f"test -f {file_name}") == 0:
        return file_name
    return download(url)


def get_file_name(url: str) -> str:
    return url.split('/')[-1]


def unzip(file_name: str) -> str:
    assert file_name.endswith('.gz')
    os.system(f"gunzip {file_name}")
    return file_name[:-3]  # remove .gz
