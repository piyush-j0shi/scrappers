#!/usr/bin/env bash
cd "$(dirname "$0")"

.venv/bin/python -c "import proxy" 2>/dev/null || .venv/bin/pip install -q proxy.py
[ -x ./bore ] || { curl -L -o bore.tgz https://github.com/ekzhang/bore/releases/download/v0.5.0/bore-v0.5.0-x86_64-unknown-linux-musl.tar.gz && tar xzf bore.tgz && rm -f bore.tgz && chmod +x bore; }

pgrep -f "proxy --hostname 127.0.0.1 --port 8888" >/dev/null || nohup .venv/bin/python -m proxy --hostname 127.0.0.1 --port 8888 --log-level ERROR >/dev/null 2>&1 &
pgrep -f "bore local 8888" >/dev/null || nohup ./bore local 8888 --to bore.pub --port 24385 >/dev/null 2>&1 &
pgrep -f "R 8890:127.0.0.1:8888" >/dev/null || nohup ssh -N -o ServerAliveInterval=20 -R 8890:127.0.0.1:8888 piyush@hetzner >/dev/null 2>&1 &

sleep 6
echo "tunnel exit IP: $(curl -s --max-time 15 -x http://bore.pub:24385 https://api.ipify.org)"
