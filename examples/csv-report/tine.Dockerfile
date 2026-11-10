# A tine image with the same Python and pytest as task.json's Docker template
# (python:3.13-slim plus pytest 8.4.2), so the task runs unchanged on both
# drivers. Build it on the tine host:
#
#   sudo tined image build python313 -f tine.Dockerfile --kernel ~/tine/artifacts/vmlinux-6.18.56
#
# A tine image boots with systemd as its init (tine-agent is a systemd service).
# Only the filesystem goes into the image, not ENV; python:3.13-slim's python
# lives in /usr/local/bin, which is on the default PATH.
FROM python:3.13-slim

RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      systemd systemd-sysv udev iproute2 ca-certificates \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

RUN systemctl mask getty@tty1.service serial-getty@ttyS0.service systemd-networkd.service systemd-resolved.service 2>/dev/null || true

RUN pip install --no-cache-dir --quiet --disable-pip-version-check pytest==8.4.2
