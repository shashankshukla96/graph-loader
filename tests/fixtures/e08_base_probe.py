"""Standalone isolated runtime-only probe; never imports pytest or conftest."""
import hashlib
import importlib
import importlib.metadata as metadata
import importlib.util
import json
from pathlib import Path
import sys
import sysconfig


def main():
    inventory = json.loads(Path(sys.argv[1]).read_text())
    site = Path(sysconfig.get_paths()['purelib']).resolve()
    assert sys.prefix != sys.base_prefix
    assert not sys.flags.no_site and sys.flags.isolated
    assert Path(sys.prefix, 'pyvenv.cfg').read_text().split('include-system-site-packages = ')[1].splitlines()[0] == 'false'
    absence = {}
    for name in ('pytest', 'testcontainers', 'pandas', 'dotenv', 'build', 'twine', 'psutil'):
        absence[name] = importlib.util.find_spec(name) is None
    assert all(absence.values()), absence
    versions = {d.metadata['Name']:d.version for d in metadata.distributions()}
    for name in ('neo4j', 'neo4j-rust-ext', 'confluent-kafka', 'pydantic', 'PyYAML', 'docker', 'numpy', 'prometheus-client'):
        assert metadata.version(name)
    driver = metadata.version('neo4j')
    major = int(driver.split('.')[0])
    rust_path = {5:'neo4j._codec.packstream._rust', 6:'neo4j._rust'}[major]
    codec_path = {5:rust_path+'.v1', 6:rust_path+'.codec.packstream.v1'}[major]
    rust = importlib.import_module(rust_path)
    codec = importlib.import_module(codec_path)
    from neo4j._codec.packstream import v1 as packstream
    assert packstream._rust_pack is codec.pack and packstream._rust_unpack is codec.unpack
    assert callable(packstream._rust_pack) and callable(packstream._rust_unpack)
    assert packstream._rust_pack(1, None) == b'\x01'
    extension = metadata.version('neo4j-rust-ext')
    assert extension.rsplit('.', 1)[0] == driver
    assert int(driver.split('.')[0]) < 7
    assert Path(rust.__file__).resolve().is_relative_to(site)
    for relative, digest in inventory['package_payload'].items():
        path = site/relative
        assert path.is_file() and not path.is_symlink()
        assert hashlib.sha256(path.read_bytes()).hexdigest() == digest, relative
    Path(sys.argv[2]).write_text(json.dumps({'application_version':metadata.version('neo4j-graph-loader'),
        'versions':versions, 'optional_absence':absence, 'active_rust':str(rust.__file__),
        'driver':driver, 'extension':extension, 'site':str(site), 'payload_members':len(inventory['package_payload'])},indent=2)+'\n')


if __name__ == '__main__':
    main()
