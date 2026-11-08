"""Local Fernet credentials. Key and SQLite never enter the source archive."""
import os
import secrets
from pathlib import Path
from cryptography.fernet import Fernet

def state_dir():
    root=Path(os.environ.get('COLLECTOR_HOME',Path(__file__).resolve().parents[1]))/'状态'
    root.mkdir(parents=True,exist_ok=True,mode=0o700)
    if os.name=='posix':root.chmod(0o700)
    return root

def private_file(path,data):
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'wb') as stream:stream.write(data);stream.flush();os.fsync(stream.fileno())

def secret(value,decode=False):
    path=state_dir()/'凭据密钥.key'
    if not path.exists():
        if decode:raise ValueError('本机凭据密钥丢失，请恢复原密钥或重新配置Token')
        try:private_file(path,Fernet.generate_key())
        except FileExistsError:pass
    if os.name=='posix':path.chmod(0o600)
    cipher=Fernet(path.read_bytes())
    return cipher.decrypt(value.encode()).decode() if decode else cipher.encrypt(value.encode()).decode()

def access_token():
    path=state_dir()/'网页访问令牌.key'
    if not path.exists():
        try:private_file(path,secrets.token_urlsafe(32).encode())
        except FileExistsError:pass
    if os.name=='posix':path.chmod(0o600)
    return path.read_text().strip()
