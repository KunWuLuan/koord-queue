#!/usr/bin/env bash

# Copyright 2020 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit
set -o nounset
set -o pipefail

SCRIPT_ROOT=$(dirname "${BASH_SOURCE[0]}")/..

ENVTEST_K8S_VERSION=${ENVTEST_K8S_VERSION:-1.33}

# Install setup-envtest if not present
if ! command -v setup-envtest &> /dev/null; then
    echo "setup-envtest not found, installing..."
    go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest
fi

# Download kubebuilder assets
# Use HTTPS_PROXY/HTTP_PROXY if available (e.g. via proxy_wrapper)
KUBEBUILDER_ASSETS="$(setup-envtest use ${ENVTEST_K8S_VERSION} -p path --bin-dir ${SCRIPT_ROOT}/bin/k8s)"
export KUBEBUILDER_ASSETS

echo "Using KUBEBUILDER_ASSETS=${KUBEBUILDER_ASSETS}"

# Run integration tests
go test -mod=vendor -count=1 ./pkg/jobext/test/integration/... ./pkg/test/integration/... "${@}"
