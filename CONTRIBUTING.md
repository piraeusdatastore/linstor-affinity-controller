# Contributing to LINSTOR Affinity Controller

The [Piraeus contributing guide](https://github.com/piraeusdatastore/piraeus/blob/master/CONTRIBUTING.md) describes how to report issues, write commits and changelog entries, and open pull requests.
The sections that follow describe what is specific to this repository.

## Target branch

Open pull requests against the `main` branch.

## Build and test

Install [Go](https://go.dev/) in the version declared in `go.mod`.
Run the unit tests and the linter:

```
$ go test ./...
$ golangci-lint run
```

`.golangci.yaml` enables the `gofumpt` and `goimports` formatters.
To format the code, run `golangci-lint fmt`.

On every pull request, CI runs [`golangci-lint`](https://golangci-lint.run/) on new issues and a build of the container image.
CI does not run the unit tests.
Run them locally before you open a pull request.

## Deployment manifests

Two projects deploy the Affinity Controller:

* [Piraeus Operator](https://github.com/piraeusdatastore/piraeus-operator), with the manifests in `pkg/resources/cluster/affinity-controller/`.
* The `linstor-affinity-controller` chart in [helm-charts](https://github.com/piraeusdatastore/helm-charts).

If you add a command-line option that users must set, or if the Affinity Controller requires new RBAC permissions, open pull requests in both projects.

## Testing on a cluster

To try a change on a cluster, build an image with [Docker Buildx](https://docs.docker.com/build/buildx/) and push it to your registry:

```
$ docker buildx build --push --tag registry.example.com/myname/linstor-affinity-controller:dev .
```

If you deploy with Piraeus Operator, set the `linstor-affinity-controller` component in the [image configuration](https://piraeus.io/docs/stable/explanation/image-configuration/) to the new image.
