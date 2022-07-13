import sys
import os
import subprocess
import pathlib
import urllib3
import shutil


def download_file(url, dest):
    http = urllib3.PoolManager()
    with http.request('GET', url, preload_content=False) as req, \
            open(dest, 'wb') as dest_file:
        shutil.copyfileobj(req, dest_file)


def cargo_path():
    return pathlib.Path.home() / '.cargo' / 'bin'


def export_cargo_to_path():
    """Add ``cargo`` to the PATH, assuming it's installed."""
    os.environ['PATH'] = str(cargo_path()) + os.pathsep + os.environ['PATH']


if sys.platform in ('linux', 'darwin'):
    def install_rust():
        # curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
        download_file('https://sh.rustup.rs', 'rustup-init.sh')
        subprocess.check_call(['sh', 'rustup-init.sh',
            '-y', '--profile', 'minimal'])


elif sys.platform == 'win32':
    def install_rust():
        download_file('https://win.rustup.rs/x86_64', 'rustup-init.exe')
        subprocess.check_call(['rustup-init.exe',
            '-y', '--profile', 'minimal'])


else:
    raise NotImplementedError(f'Unsupported platform: {sys.platform}')


if __name__ == '__main__':
    install_rust()
