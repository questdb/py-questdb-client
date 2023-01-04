import sys
sys.dont_write_bytecode = True
import os
import subprocess
import pathlib
import urllib
import urllib.request


def download_file(url, dest):
    req = urllib.request.Request(url, method='GET')
    resp = urllib.request.urlopen(req, timeout=120)
    data = resp.read()
    with open(dest, 'wb') as dest_file:
        dest_file.write(data)


def cargo_path():
    return pathlib.Path.home() / '.cargo' / 'bin'


def export_cargo_to_path():
    """Add ``cargo`` to the PATH, assuming it's installed."""
    os.environ['PATH'] = str(cargo_path()) + os.pathsep + os.environ['PATH']


if sys.platform in ('linux', 'darwin'):
    def install_rust():
        if not cargo_path().exists():
            # curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
            download_file('https://sh.rustup.rs', 'rustup-init.sh')
            subprocess.check_call(['sh', 'rustup-init.sh',
                '-y', '--profile', 'minimal'])


elif sys.platform == 'win32':
    def install_rust():
        if not cargo_path().exists():
            download_file('https://win.rustup.rs/x86_64', 'rustup-init.exe')
            subprocess.check_call(['rustup-init.exe',
                '-y', '--profile', 'minimal'])


else:
    raise NotImplementedError(f'Unsupported platform: {sys.platform}')


if __name__ == '__main__':
    install_rust()
