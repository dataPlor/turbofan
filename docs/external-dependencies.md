# External Dependencies in Turbofan Steps

Turbofan steps run in Docker containers on AWS Batch. Each step has its own directory that becomes the Docker build context — meaning `docker build` can only see files inside that directory.

When your step depends on shared code that lives outside the step directory (e.g., services in your project's `services/` folder), Turbofan can automatically detect and stage those files into the Docker image at deploy time.

## How it works

1. You write `require "services/foo"` in your worker.rb (standard Ruby `require`)
2. At deploy time, Turbofan loads your worker.rb in an isolated process and detects which files were loaded from outside the step directory
3. Those files are staged into a temporary directory and injected into the Docker build via BuildKit's `--build-context` mechanism
4. Inside the container, the files are copied to the step root (`/app/`), mirroring their project-relative paths

No manual file copying. No stale duplicates.

## Setup

### 1. Configure the load path

Turbofan needs to know where your project root is so it can resolve `require "services/foo"` to an actual file. Add the project root to `$LOAD_PATH` in your turbofan config:

```ruby
# turbofans/config/turbofan.rb

$LOAD_PATH.unshift(File.expand_path("../..", __dir__))

Turbofan.configure do |c|
  c.bucket = "my-bucket"
  # ...
end
```

`File.expand_path("../..", __dir__)` resolves to the project root (two levels up from `turbofans/config/`). After this, `require "services/foo"` finds `<project_root>/services/foo.rb`.

Alternatively, set the `TURBOFAN_LOAD_PATH` environment variable.

### 2. Update your Dockerfile

Add this line to your step's Dockerfile, after the main `COPY` and before `CMD`:

```dockerfile
COPY . .
COPY --from=schemas . schemas/

# Auto-resolved external deps (added by Turbofan at build time)
COPY --from=deps . .

ENV TURBOFAN_SCHEMAS_PATH=/app/schemas
CMD ["ruby", "entrypoint.rb"]
```

`COPY --from=deps . .` copies the staged dependencies into the container's working directory (`/app/`). If the step has no external deps, this copies nothing — it's always safe to include.

New steps created with `turbofan step new` include this line automatically.

### 3. Update your entrypoint.rb

Add `$LOAD_PATH.unshift(__dir__)` as the first line:

```ruby
# entrypoint.rb
$LOAD_PATH.unshift(__dir__) unless $LOAD_PATH.include?(__dir__)
require "turbofan"
require_relative "worker"

Turbofan::Runtime::Wrapper.run(MyStep)
```

This makes `/app/` the load path root inside the container, so `require "services/foo"` resolves to `/app/services/foo.rb` — the same relative path as on your dev machine.

New steps created with `turbofan step new` include this line automatically.

### 4. Write requires in your worker.rb

Use `require` (not `require_relative`) for external dependencies:

```ruby
# worker.rb
require "services/device_catalog_service"
require "services/duckdb_helpers/load_extensions_service"

class ProcessDevicePartition
  include Turbofan::Step

  def call(inputs, context)
    DeviceCatalogService.call(...)
  end
end
```

The path in `require` must be **project-relative** — the same path that works from your project root on your dev machine.

### Rules for require statements

| Location | Use | Example |
|----------|-----|---------|
| External dep (outside step dir) | `require` | `require "services/foo"` |
| File within your step dir | `require_relative` | `require_relative "lib/helpers"` |
| Gem | `require` | `require "duckdb"` |

Transitive deps (files required by your external deps) are detected automatically. If `services/foo.rb` does `require_relative "bar"`, Turbofan stages both `services/foo.rb` and `services/bar.rb`.

## Migrating from manual file copying

If your step currently copies service files into the step directory before deploy:

### Before

```
turbofans/steps/my_step/
  worker.rb
  lib/services/
    device_catalog_service.rb        # manually copied from services/
    device_catalog_mode_detection_service.rb  # manually copied
    ...
```

```ruby
# worker.rb (old pattern)
class MyStep
  include Turbofan::Step

  def call(inputs, context)
    load_runtime_dependencies
    # ...
  end

  def load_runtime_dependencies
    return if @runtime_loaded
    require "duckdb"
    services_path = File.join(__dir__, "lib", "services")
    require File.join(services_path, "device_catalog_service")
    @runtime_loaded = true
  end
end
```

### After

1. Delete the manually-copied files from `lib/services/`
2. Move requires to the top of worker.rb using project-relative paths
3. Remove `load_runtime_dependencies`
4. Update Dockerfile and entrypoint.rb (see above)

```ruby
# worker.rb (new pattern)
require "duckdb"
require "services/device_catalog_service"

class MyStep
  include Turbofan::Step

  def call(inputs, context)
    # DeviceCatalogService is already loaded
  end
end
```

That's it. Turbofan detects `services/device_catalog_service.rb` (and all its transitive deps) at deploy time and stages them into the Docker image.

## How detection works (under the hood)

At deploy time, `turbofan deploy` does the following for each step:

1. Forks a child process (isolated from the deploy process)
2. Adds the project root to `$LOAD_PATH` in the child
3. Loads the step's `worker.rb` via `Kernel.load`
4. Diffs `$LOADED_FEATURES` (Ruby's global list of loaded files) before and after
5. Filters out gems, stdlib, and files already inside the step directory
6. Returns the list of external `.rb` files that were loaded

These files are then:
- Included in the content-based image tag (so the image rebuilds when a dep changes)
- Staged into a temporary directory preserving their project-relative paths
- Passed to `docker build` as a BuildKit named context (`--build-context deps=<tmpdir>`)
- Copied into the container via `COPY --from=deps . .` in the Dockerfile
- Cleaned up after the build completes

## Limitations

| Limitation | Details |
|-----------|---------|
| Only `require` is detected | `autoload`, `Kernel.load`, and `require_relative` to files outside the step dir are not detected. Use `require "path/to/file"` for external deps. |
| Only `.rb` files | Data files, YAML configs, and native extensions are not detected. Include those in the step directory or Dockerfile directly. |
| Requires must succeed at deploy time | If a `require` fails (e.g., missing native library), the dep won't be detected. Ensure all required gems are available in your dev environment. |
| `require_relative` won't work for external deps | `require_relative` resolves relative to the source file's location, which differs between dev and Docker. Use `require` with project-relative paths instead. |

## Troubleshooting

**`LoadError` at container startup for a service file**

The dep wasn't detected at deploy time. Check:
- Is the `require` at the top of worker.rb (not inside a method)?
- Is it `require "services/foo"` (not `require_relative`)?
- Is the project root on `$LOAD_PATH` in your turbofan config?
- Does `entrypoint.rb` have `$LOAD_PATH.unshift(__dir__)` as the first line?
- Does the Dockerfile have `COPY --from=deps . .`?

**Image doesn't rebuild when a service file changes**

The content-based tag includes external deps. If the tag isn't changing:
- Run `turbofan deploy` with `--dry-run` to see the computed image tags
- Verify the dep is being detected: check deploy output for the list of resolved deps

**Deploy warning: "Could not resolve deps for step_name"**

The worker.rb failed to load in the detection fork. This is non-fatal — deploy continues without auto-deps for that step (same as before this feature). Check the warning message for the specific `LoadError`.
