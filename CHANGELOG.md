# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] - 2023-10-02

### Added
- Implement cache for resource listings.
- Use custom User-Agent to identify calls in LINSTOR.

## [0.3.0] -- 2023-05-04

### Added
- Support for `--property-namespace`, as used by Operator v2 to configure LINSTOR CSI.

## [0.2.2] -- 2022-06-02

### Changed
- Fixed an issue with the selection labels for the PodDisruptionBudget not matching the pods.

## [0.2.1] -- 2022-06-02

### Changed
- Fixed a potential nil-pointer if no volume context was found

## [0.2.0] -- 2022-05-27

### Added
- Add event on PVC if bound to update PV.
- Determine access policy from volume attributes if available.
- Use leader election to safely support multiple instances running at the same time.

## [0.1.0]

### Added
- Initial implementation

[Unreleased]: https://github.com/piraeusdatastore/linstor-affinity-controller/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/piraeusdatastore/linstor-affinity-controller/compare/v0.3.0...v1.0.0
[0.3.0]: https://github.com/piraeusdatastore/linstor-affinity-controller/compare/v0.2.2...v0.3.0
[0.2.2]: https://github.com/piraeusdatastore/linstor-affinity-controller/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/piraeusdatastore/linstor-affinity-controller/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/piraeusdatastore/linstor-affinity-controller/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/piraeusdatastore/linstor-affinity-controller/releases/tag/v0.1.0
