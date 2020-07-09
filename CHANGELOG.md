
0.2.1 / 2020-07-08
==================

### Added

  * Add examples for GECCO 2020 tutorial
  * Update introduction link to Uber eng blog link
  * Add a name to each of the backends
  * Add `--image` option
  * Add `--version` to `fiber` command
  * Allow `fiber` command to use different docker registries
  * Add docs about how to set default service account permission on K8s
  * Add rbac.yaml (grant default account K8s API permission).
  * Add news media link
  * Add analysis code
  * Add favicon.ico
  * Add CONTRIBUTING.md

### Changed

  * Optimize `fiber` command startup time by lazy loading some modules
  * Remove fossa
  * Check if AWS repo needed to be created
  * Refactor multi-provider support code
  * Update README to include quick links
  * Add a section for citing fiber
  * Fix image link in "More about Fiber"
  * Update code example style
  * Update website image layout

### Fixed

  * Use cloudpickle when executing Fiber pool in interactive sessions
  * Disable logging when cleanup `_event_dict`, otherwise it may cause "I/O operation on closed file" error
  * Lazy start pool workers so that resource limits can be applied to them
  * Fix a bug that causes `get_python_exe` to use wrong backend names
  * Fix `prompt_choices` function call
  * Fix typos
  * Fix broken links in Fiber docs


0.2.0 / 2020-03-25
==================

### Added

  * Fiber code base
