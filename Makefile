GOOS=${TARGETOS}
ifeq ($(GOOS),)
GOOS=$(shell uname -s | tr A-Z a-z)
endif
GOARCH=${TARGETARCH}
ifeq ($(GOARCH),)
GOARCH=$(subst x86_64,amd64,$(patsubst i%86,386,$(shell uname -m)))
endif
# COMMONENVVAR=GOOS=$(shell uname -s | tr A-Z a-z) GOARCH=$(subst x86_64,amd64,$(patsubst i%86,386,$(shell uname -m)))
BUILDENVVAR=CGO_ENABLED=0

# ENVTEST_K8S_VERSION refers to the version of kubebuilder assets to be downloaded by envtest binary.
ENVTEST_K8S_VERSION = 1.33

# Setting SHELL to bash allows bash commands to be executed by recipes.
# This is a requirement for 'setup-envtest' in the test target.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

## Tool Binaries
ENVTEST ?= $(LOCALBIN)/setup-envtest

.PHONY: all
all: build

.PHONY: build
build: build-queue

.PHONY: build-queue
build-queue: fixcodec
	GOOS=$(GOOS) GOARCH=$(GOARCH) $(BUILDENVVAR) go build -ldflags '-w' -o bin/koord-queue cmd/main.go

.PHONY: fixcodec
fixcodec:
	hack/fix-codec-factory.sh

.PHONY: update-vendor
update-vendor:
	hack/update-vendor.sh

.PHONY: unit-test
unit-test: fixcodec update-vendor
	hack/unit-test.sh

.PHONY: envtest
envtest: $(ENVTEST) ## Download envtest-setup locally if necessary.
$(ENVTEST): $(LOCALBIN)
	GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest

.PHONY: setup-envtest
setup-envtest: envtest ## Download kubebuilder assets for envtest.
	$(ENVTEST) use $(ENVTEST_K8S_VERSION) -p path --bin-dir $(LOCALBIN)/k8s

.PHONY: integration-test
integration-test: fixcodec update-vendor setup-envtest ## Run integration tests with envtest.
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) -p path --bin-dir $(LOCALBIN)/k8s)" go test -mod=vendor ./pkg/jobext/test/integration/... ./pkg/test/integration/...

.PHONY: clean
clean:
	rm -rf ./bin
