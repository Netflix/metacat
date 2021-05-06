#!/usr/bin/env bash
set -x
curl -fsSL "https://github.com/docker/docker-credential-helpers/releases/download/v0.6.0/docker-credential-pass-v0.6.0-amd64.tar.gz" | tar xv
chmod + $(pwd)/docker-credential-pass

gpg --batch --gen-key <<-EOF ; pass init $(gpg --no-auto-check-trustdb --list-secret-keys | grep ^sec | cut -d/ -f2 | cut -d" " -f1)
%echo Generating a standard key
Key-Type: DSA
Key-Length: 1024
Subkey-Type: ELG-E
Subkey-Length: 1024
Name-Real: NetflixOSS
Name-Email: netflixoss@netflix.com
Expire-Date: 0
# Do a commit here, so that we can later print "done" :-)
%commit
%echo done
EOF

pass init $key

echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin
docker info
