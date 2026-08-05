# DataPusher Plus Configuration

## Optional Features

DataPusher Plus includes some optional features that can be enabled through configuration. These features are disabled by default to ensure compatibility with different CKAN versions.

### IFormRedirect Support

The IFormRedirect interface provides custom redirect behavior after dataset and resource form submissions. This interface is only available in certain CKAN branches and is not yet merged into the main CKAN codebase.

**Note**: IFormRedirect methods are only defined when this feature is enabled, keeping the plugin completely clean when disabled.

**Configuration:**
```ini
# Enable IFormRedirect functionality (default: false)
ckanext.datapusher_plus.enable_form_redirect = true
```

**What it does:**
- **Dynamically adds IFormRedirect methods** only when enabled
- Provides custom redirect URLs after dataset/resource creation or editing
- Redirects to dataset page after dataset metadata submission
- Redirects to resource view after resource editing
- Allows "add another resource" workflow
- **Works best with DRUF** for complete resource-first workflow

**Requirements:**
- CKAN version with IFormRedirect interface support
- If the interface is not available, the feature will be automatically disabled with a warning
- **Recommended**: Enable together with DRUF for optimal resource-first experience

### DRUF (Dataset Resource Upload First) Support

DRUF allows users to upload resources before creating the dataset metadata, providing a resource-first workflow.

**Configuration:**
```ini
# Enable DRUF functionality (default: false)  
ckanext.datapusher_plus.enable_druf = true
```

**What it does:**
- Adds a `/resource-first/new` endpoint
- Creates a temporary dataset and redirects to resource upload
- Useful for workflows where users want to upload data files first
- **Overrides templates**: Modifies "Add Dataset" buttons and form stages to support resource-first workflow

**Template Overrides:**
When DRUF is enabled, the following templates are overridden:
- `snippets/add_dataset.html`: Changes "Add Dataset" to redirect to resource upload
- `package/snippets/package_form.html`: Modifies form stages to show "Add data" first
- `package/snippets/resource_form.html`: Modifies resource form stages for the resource-first flow
- `scheming/package/snippets/package_form.html`: Modifies scheming form stages
- `scheming/package/snippets/resource_form.html`: Modifies scheming resource form stages

**Requirements:**
- No special CKAN version requirements
- Works with standard CKAN installations
- Compatible with ckanext-scheming

### Turning Prefect off

By default (v3.0+) ingestion jobs are orchestrated by a Prefect server + worker. Setting `prefect_enabled = false` runs them in-process on CKAN's own background-job worker instead, over the same ingestion stages, with nothing in the path importing `prefect`.

**Configuration:**
```ini
# Orchestrate jobs with Prefect (default: true)
ckanext.datapusher_plus.prefect_enabled = false
```

**What it does:**
- `datapusher_submit` enqueues the job on CKAN's RQ queue instead of creating a Prefect flow run
- `ckanext/datapusher_plus/jobs/local_runner.py` executes the nine stages sequentially in the worker process
- The `Jobs`/`Logs` tables, the job-status page, and the `datapusher_hook` callbacks behave identically

**Requirements:**
- A running CKAN worker: `ckan -c /etc/ckan/default/ckan.ini jobs worker`
- No Prefect server, worker, or work pool (`datapusher_plus prefect-deploy` refuses to run in this mode)

**What you give up:** per-stage retries, result caching / re-run-from-failed-stage, the Prefect run graph, artifacts and `datapusher.*` events, and human-in-the-loop PII review (a job crossing `pii_review_threshold` aborts before any datastore write instead of waiting for approval). See [Running without Prefect](README.md#running-without-prefect) for the full comparison — including the common trigger for wanting it, a `PermissionError` on `$PREFECT_HOME/profiles.toml` when CKAN cannot write `$HOME/.prefect`.

## Example Configuration

Add these lines to your CKAN configuration file (e.g., `/etc/ckan/default/ckan.ini`):

```ini
# Enable DRUF (Dataset Resource Upload First) workflow
ckanext.datapusher_plus.enable_druf = true

# Enable IFormRedirect for better form redirects (recommended with DRUF)
ckanext.datapusher_plus.enable_form_redirect = true

# Run ingestions in-process on CKAN's job worker instead of Prefect
# (default: true — leave unset to keep Prefect orchestration)
ckanext.datapusher_plus.prefect_enabled = false
```

**Recommended combinations:**
- **Standard mode**: Both disabled (default) - maintains standard CKAN behavior
- **Resource-first workflow**: Both enabled - complete resource-first experience
- **DRUF only**: Only `enable_druf = true` - resource-first without custom redirects

## Template Organization

DataPusher Plus uses a conditional template loading system to avoid conflicts when optional features are disabled:

- **Base templates** (`templates/`): Always loaded, provides standard DataPusher Plus functionality
- **DRUF templates** (`templates/druf/`): Only loaded when `enable_druf = true`, overrides default dataset creation workflow

This ensures that when DRUF is disabled, your CKAN installation maintains completely standard behavior without any template modifications.

## Backwards Compatibility

When these features are disabled (default), DataPusher Plus maintains full backwards compatibility with standard CKAN installations. The plugin will automatically detect if required interfaces are available and disable features gracefully if they are not supported.

## Logging

The plugin will log the status of these features:
- Info messages when features are successfully enabled
- Warning messages when features are configured but not available
- Debug messages for DRUF blueprint registration

Check your CKAN logs to verify the status of these optional features.
