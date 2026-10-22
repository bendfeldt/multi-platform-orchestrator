#!/usr/bin/env bash
# Installs pinned Terraform and Trivy into ~/.local/bin, each checked against its publisher's
# checksums. Used by both CI systems.
set -euo pipefail
terraform_version="1.13.4"
trivy_version="0.76.1"
dir="$(mktemp -d)"
mkdir -p "$HOME/.local/bin"

tf="terraform_${terraform_version}_linux_amd64.zip"
curl -sSfL -o "$dir/$tf" "https://releases.hashicorp.com/terraform/$terraform_version/$tf"
curl -sSfL -o "$dir/tf.sums" "https://releases.hashicorp.com/terraform/$terraform_version/terraform_${terraform_version}_SHA256SUMS"
(cd "$dir" && grep " $tf\$" tf.sums | sha256sum -c -)
unzip -oq "$dir/$tf" terraform -d "$HOME/.local/bin"

trivy="trivy_${trivy_version}_Linux-64bit.tar.gz"
curl -sSfL -o "$dir/$trivy" "https://github.com/aquasecurity/trivy/releases/download/v$trivy_version/$trivy"
curl -sSfL -o "$dir/trivy.sums" "https://github.com/aquasecurity/trivy/releases/download/v$trivy_version/trivy_${trivy_version}_checksums.txt"
(cd "$dir" && grep " $trivy\$" trivy.sums | sha256sum -c -)
tar xzf "$dir/$trivy" -C "$HOME/.local/bin" trivy

"$HOME/.local/bin/terraform" version | head -1
"$HOME/.local/bin/trivy" --version | head -1
